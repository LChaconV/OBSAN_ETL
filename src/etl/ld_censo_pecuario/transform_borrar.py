from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
import yaml
import pandas as pd

from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.config_utils import load_yaml, save_yaml


PROJECT_ROOT = Path(__file__).resolve().parents[3]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "censo_pecuario_transform.yaml"
LOG_DIR = PROJECT_ROOT / "logs"


def load_transform_config(key: str) -> dict:
    config = load_yaml(CONFIG_PATH)

    if key not in config:
        raise KeyError(f"No existe la clave '{key}' en censo_pecuario_transform.yaml")

   
        

    return config[key]


def get_runtime_params() -> tuple[Path, int, str]:
    #input_file= Path(r"C:\Users\laura\ESCUELA COLOMBIANA DE INGENIERIA JULIO GARAVITO\Proyecto OBSAN - General\Datos_OBSAN_web\OBSAN\observatorio-san\data\bronze\censo_pecuario\run_2026_03_01\- CENSO-BOVINO-2025_14-5-25.xlsx")
    #year="2025"
    #animal_type="bovino" 


    input_file = os.environ.get("OBSAN_INPUT_FILE")
    year = os.environ.get("OBSAN_YEAR")
    animal_type = os.environ.get("OBSAN_ANIMAL_TYPE")



    if not input_file:
        raise ValueError("Falta la variable de entorno OBSAN_INPUT_FILE")

    if not year:
        raise ValueError("Falta la variable de entorno OBSAN_YEAR")

    if not animal_type:
        raise ValueError("Falta la variable de entorno OBSAN_ANIMAL_TYPE")

    file_path = Path(input_file)

    if not file_path.exists():
        raise FileNotFoundError(f"No existe el archivo de entrada: {file_path}")

    return file_path, int(year), animal_type.strip().lower()


def find_header_row_excel(file_path: Path, required_columns: list[str], max_rows: int = 30) -> int:
    preview = pd.read_excel(file_path, header=None, nrows=max_rows)

    required_norm = {str(col).strip().upper() for col in required_columns}

    for idx, row in preview.iterrows():
        row_values = {str(value).strip().upper() for value in row.dropna().tolist()}
        matches = required_norm.intersection(row_values)

        if len(matches) == len(required_norm):
            return idx

    raise ValueError(
        f"No se encontró una fila de encabezado que contenga las columnas requeridas: {required_columns}"
    )


def read_input_file(file_path, required_columns: list[str]) -> pd.DataFrame:
    file_path = Path(file_path)
    suffix = file_path.suffix.lower()

    if suffix in [".xlsx", ".xls"]:
        header_row = find_header_row_excel(file_path, required_columns)
        logging.info("Fila de encabezado detectada: %s", header_row)
        return pd.read_excel(file_path, header=header_row)

    if suffix == ".csv":
        return pd.read_csv(file_path)

    if suffix == ".parquet":
        return pd.read_parquet(file_path)

    raise ValueError(f"Formato no soportado: {suffix}")


def validate_required_columns(df: pd.DataFrame, config: dict) -> None:
    required_columns = config["validation"]["required_columns"]
    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        raise ValueError(f"Faltan columnas requeridas: {missing}")


def transform_censo_pecuario(
    df: pd.DataFrame,
    config: dict,
    year: int,
    animal_type: str,
) -> pd.DataFrame:
    fact_cfg = config["fact_table"]

    rename_columns = fact_cfg["rename_columns"]
    grain = fact_cfg["grain"]
    metric_columns = fact_cfg["metric_columns"]

    df = df.rename(columns=rename_columns)

    df["year"] = year
    df["type"] = animal_type

    df["id_mun"] = df["id_mun"].astype(str).str.strip()

    for col in metric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    group_columns = grain + ["type"]

    df_gold = (
        df.groupby(group_columns, dropna=False, as_index=False)[metric_columns]
        .sum()
    )
    if "total_farms" not in df_gold.columns:
        logging.warning("Columna 'total_farms' no encontrada. Asignando valores nulos.")
        df_gold["total_farms"] = None 

    return df_gold.sort_values(group_columns).reset_index(drop=True)


def save_golden(df: pd.DataFrame, config: dict, year: int, animal_type: str) -> Path:
    golden_dir = PROJECT_ROOT / config["source"]["golden_fact_dir"]
    golden_dir.mkdir(parents=True, exist_ok=True)

    output_path = golden_dir / f"censo_{animal_type}_{year}.parquet"
    df.to_parquet(output_path, index=False)

    logging.info("Archivo golden guardado en: %s", output_path)

    return output_path

def run() -> None:
    file_path, year, animal_type = get_runtime_params()
    key = animal_type

    setup_logging(LOG_DIR, f"censo_{key}_transform.log")
    logging.info("Iniciando transformación de censo pecuario - %s", key)

    config = load_transform_config(key)

    logging.info("Archivo recibido: %s", file_path)
    logging.info("Año recibido: %s", year)
    logging.info("Tipo recibido: %s", animal_type)

    required_columns = config["validation"]["required_columns"]

    df = read_input_file(file_path, required_columns)

    logging.info("Filas leídas: %s", len(df))
    logging.info("Columnas leídas: %s", df.columns.tolist())

    validate_required_columns(df, config)

    df_gold = transform_censo_pecuario(
        df=df,
        config=config,
        year=year,
        animal_type=animal_type,
    )

    logging.info("Filas finales golden: %s", len(df_gold))

    save_golden(
        df=df_gold,
        config=config,
        year=year,
        animal_type=animal_type,
    )
    full_config = load_yaml(CONFIG_PATH)
    full_config[key]["source"]["last_file_transformed"] = f"censo_{animal_type}_{year}.parquet"
    save_yaml(CONFIG_PATH, full_config)

    logging.info("Transformación de %s finalizada correctamente", key)


if __name__ == "__main__":
    run()