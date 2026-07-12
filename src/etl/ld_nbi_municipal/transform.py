"""
transform.py — Necesidades Básicas Insatisfechas (NBI) a nivel municipal.

Estructura del Excel DANE (hoja Municipios):
  Fila 8  (0-idx 7 ): dominios — Total | Cabeceras | Centros Poblados y Rural Disperso
  Fila 10 (0-idx 9 ): nombres de los 7 indicadores (repetidos por dominio)
  Filas 11+ (0-idx 10+): datos (1 fila por municipio)

Columnas:
  0: Código departamento  1: Nombre departamento
  2: Código municipio     3: Nombre municipio
  4-10 : dominio Total    11-17: Cabeceras    18-24: Rural disperso

Solo se carga el dominio Total (columnas 4-10).
Produce parquet golden en formato largo: (id_mun, year, indicador, valor).
"""

import logging
import os
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[4]
GOLDEN_DIR   = PROJECT_ROOT / "data" / "golden" / "nbi_municipal"
SHEET_NAME   = "Municipios"

# Columnas del dominio Total y su nombre canónico de indicador
_TOTAL_COLS: dict[int, str] = {
    4:  "prop_nbi",
    5:  "prop_miseria",
    6:  "vivienda",
    7:  "servicios",
    8:  "hacinamiento",
    9:  "inasistencia",
    10: "dependencia_economica",
}

_DATA_START_ROW = 10   # fila 11 del Excel (0-indexed)
_COL_DEPT_CODE  = 0
_COL_MUN_CODE   = 2


def _get_input_path() -> Path:
    file_path = os.environ.get("OBSAN_INPUT_FILE")
    if not file_path:
        raise ValueError("No se definió OBSAN_INPUT_FILE")
    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(f"No existe el archivo: {p}")
    return p


def _get_year() -> int:
    year = os.environ.get("OBSAN_YEAR")
    if not year:
        raise ValueError("No se definió OBSAN_YEAR")
    return int(year)


def _build_id_mun(dept_code, mun_code) -> str | None:
    """Construye código DIVIPOLA de 5 dígitos: zfill(2) + zfill(3)."""
    try:
        d = str(int(float(str(dept_code)))).zfill(2)
        m = str(int(float(str(mun_code)))).zfill(3)
        return d + m
    except (ValueError, TypeError):
        return None


def run() -> Path:
    input_path = _get_input_path()
    year       = _get_year()

    logging.info("Leyendo hoja '%s' de: %s", SHEET_NAME, input_path)
    raw = pd.read_excel(input_path, sheet_name=SHEET_NAME, header=None)

    data_rows = raw.iloc[_DATA_START_ROW:].reset_index(drop=True)
    logging.info("Filas de datos encontradas: %d", len(data_rows))

    records = []
    skipped_divipola = 0

    for _, row in data_rows.iterrows():
        dept_code = row.iloc[_COL_DEPT_CODE]
        mun_code  = row.iloc[_COL_MUN_CODE]

        # Excluir fila TOTAL NACIONAL (código departamento == 0)
        try:
            if int(float(str(dept_code))) == 0:
                continue
        except (ValueError, TypeError):
            continue

        id_mun = _build_id_mun(dept_code, mun_code)
        if id_mun is None:
            skipped_divipola += 1
            continue

        for col_pos, indicador in _TOTAL_COLS.items():
            val = row.iloc[col_pos]
            if pd.notna(val):
                try:
                    valor = float(str(val).replace(",", "."))
                    records.append({
                        "id_mun":   id_mun,
                        "year":     year,
                        "indicador": indicador,
                        "valor":    valor,
                    })
                except (ValueError, TypeError):
                    pass

    if skipped_divipola:
        logging.warning("Filas omitidas por código DIVIPOLA inválido: %d", skipped_divipola)

    if not records:
        raise ValueError("No se extrajo ningún registro del Excel NBI.")

    df = pd.DataFrame(records)
    df = df.drop_duplicates(subset=["id_mun", "year", "indicador"], keep="last")

    print(f"Filas leídas: {len(df)}")

    GOLDEN_DIR.mkdir(parents=True, exist_ok=True)
    output_path = GOLDEN_DIR / f"nbi_municipal_{year}.parquet"
    df.to_parquet(output_path, index=False)

    print(f"Archivo golden guardado en: {output_path}")
    return output_path


if __name__ == "__main__":
    run()
