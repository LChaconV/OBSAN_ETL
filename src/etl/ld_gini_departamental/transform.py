"""
transform.py — Índice de Gini Departamental (DANE).

Estructura esperada del Excel:
  - Una hoja cuyo nombre contenga "gini" (insensible a mayúsculas).
  - Encabezado de dos filas:
      Fila A: | Departamento | Año (celda combinada) |
      Fila B: | (vacío)      | 2002 | 2003 | 2005 | 2008 | … |
  - O encabezado de una fila (años directamente junto al nombre):
      Fila A: | Departamento | 2002 | 2003 | … |
  - La cantidad de años varía entre archivos (no necesariamente consecutivos).
  - Valores Gini en formato decimal 0-1 (p. ej. 0,526 → se normaliza a 0.526).
  - Las filas se leen hasta encontrar "Fuente", "Nota", "*" o "CV".

Produce parquet golden: (id_dept, year, gini).
"""

import logging
import os
import re
import unicodedata
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[4]
GOLDEN_DIR   = PROJECT_ROOT / "data" / "golden" / "gini_departamental"

_DEPT_NAME_TO_CODE: dict[str, str] = {
    "antioquia":                                "05",
    "atlantico":                                "08",
    "bogota d.c.":                              "11",
    "bogota d c":                               "11",
    "bogota d. c.":                             "11",
    "bogota, d.c.":                             "11",
    "bogota, d. c.":                            "11",
    "bogota":                                   "11",
    "bolivar":                                  "13",
    "boyaca":                                   "15",
    "caldas":                                   "17",
    "caqueta":                                  "18",
    "cauca":                                    "19",
    "cesar":                                    "20",
    "cordoba":                                  "23",
    "cundinamarca":                             "25",
    "choco":                                    "27",
    "huila":                                    "41",
    "la guajira":                               "44",
    "guajira":                                  "44",
    "magdalena":                                "47",
    "meta":                                     "50",
    "narino":                                   "52",
    "norte de santander":                       "54",
    "quindio":                                  "63",
    "risaralda":                                "66",
    "santander":                                "68",
    "sucre":                                    "70",
    "tolima":                                   "73",
    "valle del cauca":                          "76",
    "valle":                                    "76",
    "arauca":                                   "81",
    "casanare":                                 "85",
    "putumayo":                                 "86",
    "san andres providencia y santa catalina":  "88",
    "san andres, providencia y santa catalina": "88",
    "san andres":                               "88",
    "amazonas":                                 "91",
    "guainia":                                  "94",
    "guaviare":                                 "95",
    "vaupes":                                   "97",
    "vichada":                                  "99",
}

_NOISE_RE = re.compile(r"^(fuente|nota|\*|cv)\b", re.IGNORECASE)


def _normalize(s: str) -> str:
    s = str(s).strip().lower()
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()


def _is_year(val) -> int | None:
    """Retorna el año como int si el valor parece un año (2000-2035), de lo contrario None."""
    try:
        v = int(float(str(val).strip()))
        return v if 2000 <= v <= 2035 else None
    except (ValueError, TypeError):
        return None


def _get_input_path() -> Path:
    file_path = os.environ.get("OBSAN_INPUT_FILE")
    if not file_path:
        raise ValueError("No se definió OBSAN_INPUT_FILE")
    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(f"No existe el archivo: {p}")
    return p


def _detect_sheet(xf: pd.ExcelFile) -> str:
    for name in xf.sheet_names:
        if "gini" in _normalize(name):
            return name
    raise ValueError(
        f"No se encontró hoja con 'gini' en el nombre. "
        f"Hojas disponibles: {xf.sheet_names}"
    )


def _detect_structure(raw: pd.DataFrame) -> tuple[int, dict[int, int], int]:
    """
    Detecta la estructura del encabezado del Excel.

    Busca primero la columna "Departamento" por nombre (hasta las primeras 10 filas,
    primeras 5 columnas). Luego detecta la fila con ≥2 valores que sean años (2000-2035):
    esa es la fila de encabezado de años y los datos empiezan en la siguiente.

    Retorna: (data_start_row, {col_index: año}, col_dept_index)
    """
    # 1. Buscar la columna "Departamento" por su encabezado
    col_dept: int | None = None
    for i in range(min(10, len(raw))):
        for j in range(min(6, len(raw.columns))):
            val = raw.iat[i, j]
            if not pd.isna(val) and "departamento" in _normalize(str(val)):
                col_dept = j
                break
        if col_dept is not None:
            break

    # 2. Buscar la fila con ≥2 años (puede ser la misma o una posterior a "Departamento")
    for i in range(min(20, len(raw))):
        cols_year: dict[int, int] = {}
        for j in range(len(raw.columns)):
            yr = _is_year(raw.iat[i, j])
            if yr is not None:
                cols_year[j] = yr
        if len(cols_year) >= 2:
            # Si no encontramos "Departamento" por nombre, usamos la columna anterior al primer año
            if col_dept is None:
                col_dept = max(0, min(cols_year.keys()) - 1)
                logging.warning(
                    "No se encontró encabezado 'Departamento'; "
                    "usando columna %d como columna de departamentos.",
                    col_dept,
                )
            data_start = i + 1
            logging.info(
                "Fila de años detectada en índice %d. Años: %s. Columna departamento: %d.",
                i,
                sorted(cols_year.values()),
                col_dept,
            )
            return data_start, cols_year, col_dept

    raise ValueError(
        "No se encontró fila de encabezado con ≥2 años (2000-2035) "
        "en las primeras 20 filas del archivo."
    )


def _parse_gini(val) -> float | None:
    """Convierte un valor de celda a float Gini. Sólo acepta decimales en (0, 1)."""
    if pd.isna(val):
        return None
    try:
        v = float(str(val).replace(",", ".").strip())
        return v if 0 < v < 1 else None
    except (ValueError, TypeError):
        return None


def _map_dept(name: str) -> str | None:
    """Normaliza el nombre del departamento y lo mapea a su código DIVIPOLA."""
    key = _normalize(name)
    if key in _DEPT_NAME_TO_CODE:
        return _DEPT_NAME_TO_CODE[key]
    # Fallback: coincidencia de subcadena para variantes de nombre (sólo claves > 4 chars)
    for k, code in _DEPT_NAME_TO_CODE.items():
        if len(k) > 4 and (k in key or key in k):
            return code
    return None


def run() -> Path:
    input_path = _get_input_path()
    logging.info("Leyendo: %s", input_path)

    engine = "xlrd" if input_path.suffix.lower() == ".xls" else "openpyxl"
    records: list[dict] = []

    with pd.ExcelFile(input_path, engine=engine) as xf:
        sheet_name = _detect_sheet(xf)
        logging.info("Hoja detectada: '%s'", sheet_name)

        raw = xf.parse(sheet_name, header=None)
        data_start, cols_year, col_dept = _detect_structure(raw)

        for i in range(data_start, len(raw)):
            nombre = raw.iat[i, col_dept]
            if pd.isna(nombre):
                continue
            nombre_str = str(nombre).strip()
            if _NOISE_RE.match(_normalize(nombre_str)):
                break

            id_dept = _map_dept(nombre_str)
            if id_dept is None:
                logging.debug("Sin mapeo DIVIPOLA para: '%s'", nombre_str)
                continue

            for col_j, year in cols_year.items():
                valor = _parse_gini(raw.iat[i, col_j])
                if valor is not None:
                    records.append({"id_dept": id_dept, "year": year, "gini": valor})

    df = pd.DataFrame(records)
    if df.empty:
        raise ValueError(
            "No se extrajeron registros válidos del archivo. "
            "Verifique que la hoja contenga valores Gini en formato decimal (0-1) "
            "y que los departamentos sean reconocibles."
        )

    df = df.drop_duplicates(subset=["id_dept", "year"], keep="last")
    logging.info("Registros extraídos: %d (%d años únicos)", len(df), df["year"].nunique())

    GOLDEN_DIR.mkdir(parents=True, exist_ok=True)
    stem = input_path.stem
    out_name = (
        f"{stem}.parquet"
        if stem.startswith("gini_departamental_")
        else f"gini_departamental_{stem}.parquet"
    )
    output_path = GOLDEN_DIR / out_name
    df.to_parquet(output_path, index=False)

    print(f"Filas leídas: {len(df)}")
    print(f"Años detectados: {sorted(df['year'].unique().tolist())}")
    print(f"Archivo golden guardado en: {output_path}")
    return output_path


if __name__ == "__main__":
    run()
