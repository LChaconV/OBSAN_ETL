"""
transform.py — Índice de Pobreza Multidimensional (IPM) a nivel departamental.

Estructura real del Excel DANE (hoja IPM_Departamentos):
  Filas 1‑N : títulos y notas metodológicas
  Fila año  : ["Departamento", 2018, 2018, 2018, 2019, "2020**", "2021***", ...]
  Fila sub  : [NaN/Dep,  "Total", "Cabeceras", "Centros poblados...", "Total", ...]
  Filas dato: nombre de departamento + valores numéricos

Produce un parquet golden con columnas:
  name_dept | year | total | cabeceras | rural
(el mapeo name_dept → id_dept lo hace load.py vía JOIN con dim_departament)
"""

import logging
import os
import re
import unicodedata
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
GOLDEN_DIR   = PROJECT_ROOT / "data" / "golden" / "ipm_departamental"
SHEET_NAME   = "IPM_Departamentos"
YEAR_PAT     = re.compile(r"^\d{4}$")


# ─── helpers ──────────────────────────────────────────────────────────────────

def _strip_ann(val) -> str:
    """Quita asteriscos y espacios: '2020**' → '2020', NaN → ''."""
    return str(val).strip().strip("*").strip() if pd.notna(val) else ""


def _normalize(val) -> str:
    """Upper + sin tildes para comparación de texto."""
    text = str(val).strip().upper()
    text = unicodedata.normalize("NFKD", text)
    return "".join(c for c in text if not unicodedata.combining(c))


def _get_input_path() -> Path:
    file_path = os.environ.get("OBSAN_INPUT_FILE")
    if not file_path:
        raise ValueError("No se definió OBSAN_INPUT_FILE")
    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(f"No existe el archivo: {p}")
    return p


def _get_run_name(input_path: Path) -> str:
    stem = input_path.stem
    if "_run_" in stem:
        return "run_" + stem.split("_run_")[-1]
    return f"run_{stem}"


# ─── detección de encabezados ─────────────────────────────────────────────────

def _find_year_row(raw: pd.DataFrame) -> int:
    """
    Devuelve el índice de la primera fila que tenga ≥2 celdas con formato
    de año de 4 dígitos, ignorando anotaciones (asteriscos).
    """
    for i in range(len(raw)):
        row = raw.iloc[i]
        count = sum(1 for v in row if YEAR_PAT.match(_strip_ann(v)))
        if count >= 2:
            return i
    raise ValueError(
        f"No se encontró fila de años en la hoja '{SHEET_NAME}'. "
        "Se esperan ≥2 celdas con formato YYYY o YYYY** en alguna fila."
    )


def _build_col_roles(
    year_series: pd.Series,
    subcol_series: pd.Series,
) -> tuple[int | None, dict[int, tuple[int, str]]]:
    """
    Devuelve (name_pos, data_cols).
      name_pos  : posición de la columna de nombre de departamento (puede ser None)
      data_cols : {col_pos -> (year, metric)}  metric in {'total','cabeceras','rural'}

    Hace forward-fill manual del año (para celdas combinadas/merged) y
    limpia asteriscos de anotaciones metodológicas.
    """
    # Forward-fill del año con strip de asteriscos
    ffilled_years: list[int | None] = []
    last_year: int | None = None
    for v in year_series:
        stripped = _strip_ann(v)
        if YEAR_PAT.match(stripped):
            last_year = int(stripped)
        ffilled_years.append(last_year)

    name_pos: int | None = None
    data_cols: dict[int, tuple[int, str]] = {}

    for pos in range(len(year_series)):
        current_year = ffilled_years[pos]
        sub_norm  = _normalize(str(subcol_series.iloc[pos]))
        year_norm = _normalize(str(year_series.iloc[pos]))
        combined  = year_norm + " " + sub_norm

        if current_year is not None:
            if "CABECERA" in sub_norm:
                metric = "cabeceras"
            elif "TOTAL" in sub_norm:
                metric = "total"
            else:
                metric = "rural"
            data_cols[pos] = (current_year, metric)
        else:
            if "DEPARTAMENTO" in combined:
                name_pos = pos

    return name_pos, data_cols


# ─── extracción ───────────────────────────────────────────────────────────────

def run() -> Path:
    input_path = _get_input_path()
    run_name   = _get_run_name(input_path)

    logging.info("Leyendo hoja '%s' de: %s", SHEET_NAME, input_path)
    raw = pd.read_excel(input_path, sheet_name=SHEET_NAME, header=None)

    year_row_idx   = _find_year_row(raw)
    subcol_row_idx = year_row_idx + 1

    if subcol_row_idx >= len(raw):
        raise ValueError("La fila de años es la última fila del archivo; no hay subcolumnas.")

    name_pos, data_cols = _build_col_roles(
        raw.iloc[year_row_idx],
        raw.iloc[subcol_row_idx],
    )

    if not data_cols:
        raise ValueError("No se encontraron columnas de datos (año + métrica) en el Excel IPM.")

    if name_pos is None:
        raise ValueError(
            "No se encontró columna de departamento en el Excel IPM. "
            "Se esperaba una celda con 'Departamento' en la fila de encabezado."
        )

    data_rows = raw.iloc[subcol_row_idx + 1:].reset_index(drop=True)

    records = []
    for _, row in data_rows.iterrows():
        name_val = row.iloc[name_pos]

        if pd.isna(name_val) or str(name_val).strip() in ("", "nan"):
            continue

        name_dept = str(name_val).strip()

        year_data: dict[int, dict[str, float | None]] = {}
        for pos, (year, metric) in data_cols.items():
            if year not in year_data:
                year_data[year] = {"total": None, "cabeceras": None, "rural": None}
            val = row.iloc[pos]
            if pd.notna(val):
                try:
                    year_data[year][metric] = float(str(val).replace(",", "."))
                except (ValueError, TypeError):
                    pass

        for year, metrics in year_data.items():
            if metrics.get("total") is None:
                continue
            records.append({
                "name_dept": name_dept,
                "year":      year,
                "total":     metrics["total"],
                "cabeceras": metrics.get("cabeceras"),
                "rural":     metrics.get("rural"),
            })

    if not records:
        raise ValueError("No se extrajo ningún registro del Excel IPM.")

    df = pd.DataFrame(records)
    df = df.drop_duplicates(subset=["name_dept", "year"], keep="last")

    print(f"Filas leídas: {len(df)}")

    GOLDEN_DIR.mkdir(parents=True, exist_ok=True)
    output_path = GOLDEN_DIR / f"ipm_departamental_{run_name}.parquet"
    df.to_parquet(output_path, index=False)

    print(f"Archivo golden guardado en: {output_path}")
    return output_path


if __name__ == "__main__":
    run()
