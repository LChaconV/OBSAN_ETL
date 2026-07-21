"""
transform.py — Pobreza Monetaria Municipal (DANE).

Estructura del Excel:
  Hojas con nombre "Monetaria {año}" (p.ej. "Monetaria 2022").
  Encabezado en fila 1 (row 0), datos a partir de fila 2.
  Columnas de interés:
    "Código Municipio"             → id_mun (5 dígitos, zfill)
    "Estimación Pobreza Monetaria" → estimacion (string "26,5%" → 26.5)

Procesa todas las hojas que coincidan con el patrón "Monetaria YYYY" en un
solo archivo. El año se extrae del nombre de la hoja (no lo proporciona el usuario).

Produce parquet golden: (id_mun, year, estimacion).
"""

import logging
import os
import re
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
GOLDEN_DIR   = PROJECT_ROOT / "data" / "golden" / "pobreza_monetaria_municipal"

_SHEET_PAT = re.compile(r"^Monetaria\s+(\d{4})$", re.IGNORECASE)

# Aliases para detección flexible de columnas
_ALIASES_MUN = [
    "código municipio", "codigo municipio",
    "cod municipio",    "cód. municipio",
]
_ALIASES_EST = [
    "estimación pobreza monetaria", "estimacion pobreza monetaria",
    "estimación",                    "pobreza monetaria",
]


def _get_input_path() -> Path:
    file_path = os.environ.get("OBSAN_INPUT_FILE")
    if not file_path:
        raise ValueError("No se definió OBSAN_INPUT_FILE")
    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(f"No existe el archivo: {p}")
    return p


def _find_col(df: pd.DataFrame, aliases: list[str]) -> str | None:
    """Busca la primera columna cuyo nombre (lower) contenga algún alias."""
    for alias in aliases:
        for col in df.columns:
            if alias in str(col).strip().lower():
                return col
    return None


def _parse_id_mun(val) -> str | None:
    """'05001', 5001 o 5001.0 → '05001'. Devuelve None si no es parseable."""
    try:
        return str(int(float(str(val).strip()))).zfill(5)
    except (ValueError, TypeError):
        return None


def _parse_pct(val) -> float | None:
    """'26,5%' → 26.5; celdas Excel formato % (0.696) → 69.6."""
    if pd.isna(val):
        return None
    if isinstance(val, (int, float)):
        pct = float(val)
        return round(pct * 100, 4) if 0 < pct < 1 else pct
    s = str(val).strip().replace("%", "").replace(",", ".").strip()
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _process_sheet(xf: pd.ExcelFile, sheet_name: str, year: int) -> pd.DataFrame:
    df = xf.parse(sheet_name, header=0)
    df.columns = [str(c).strip() for c in df.columns]

    col_mun = _find_col(df, _ALIASES_MUN)
    col_est = _find_col(df, _ALIASES_EST)

    if col_mun is None:
        raise ValueError(
            f"Hoja '{sheet_name}': no se encontró columna de Código Municipio. "
            f"Columnas detectadas: {list(df.columns)}"
        )
    if col_est is None:
        raise ValueError(
            f"Hoja '{sheet_name}': no se encontró columna Estimación Pobreza Monetaria. "
            f"Columnas detectadas: {list(df.columns)}"
        )

    df["id_mun"]    = df[col_mun].apply(_parse_id_mun)
    df["estimacion"] = df[col_est].apply(_parse_pct)
    df["year"]       = year

    df = df[df["id_mun"].notna() & df["estimacion"].notna()].copy()
    return df[["id_mun", "year", "estimacion"]]


def run() -> Path:
    input_path = _get_input_path()
    logging.info("Leyendo: %s", input_path)

    all_records: list[pd.DataFrame] = []

    with pd.ExcelFile(input_path) as xf:
        matched = [(s, int(_SHEET_PAT.match(s).group(1)))
                   for s in xf.sheet_names
                   if _SHEET_PAT.match(s)]

        if not matched:
            raise ValueError(
                f"No se encontró ninguna hoja con el patrón 'Monetaria YYYY' en {input_path.name}. "
                f"Hojas disponibles: {xf.sheet_names}"
            )

        for sheet_name, year in matched:
            logging.info("Procesando hoja '%s' (año %d)...", sheet_name, year)
            df_sheet = _process_sheet(xf, sheet_name, year)
            logging.info("  → %d registros extraídos", len(df_sheet))
            all_records.append(df_sheet)

    df = pd.concat(all_records, ignore_index=True)
    df = df.drop_duplicates(subset=["id_mun", "year"], keep="last")

    print(f"Filas leídas: {len(df)}")

    GOLDEN_DIR.mkdir(parents=True, exist_ok=True)
    stem = input_path.stem
    if stem.startswith("pobreza_monetaria_municipal_"):
        out_name = f"{stem}.parquet"
    else:
        out_name = f"pobreza_monetaria_municipal_{stem}.parquet"
    output_path = GOLDEN_DIR / out_name
    df.to_parquet(output_path, index=False)

    print(f"Archivo golden guardado en: {output_path}")
    return output_path


if __name__ == "__main__":
    run()
