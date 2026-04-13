from src.etl.utils.config_utils import load_yaml
from pathlib import Path
import pandas as pd
import logging
import unicodedata
import re

def load_transform_config(key: str, CONFIG_PATH: Path) -> dict:
    config = load_yaml(CONFIG_PATH)

    if key not in config:
        raise KeyError(f"No existe la clave '{key}' en el YAML de transformación.")

    return config[key]


def get_latest_bronze_run(bronze_dir: Path) -> Path:
    run_dirs = [
        path for path in bronze_dir.iterdir()
        if path.is_dir() and path.name.startswith("run_")
    ]

    if not run_dirs:
        raise ValueError(f"No se encontraron carpetas run_ en {bronze_dir}")

    latest_run = sorted(run_dirs)[-1]
    logging.info("Carpeta bronze más reciente: %s", latest_run.name)

    return latest_run

def load_latest_bronze_run(run_dir: Path) -> pd.DataFrame:
    files = sorted(run_dir.glob("*.parquet"))

    if not files:
        raise ValueError(f"No se encontraron archivos parquet en {run_dir}")

    logging.info("Archivos parquet encontrados en la corrida: %s", len(files))

    dfs = [pd.read_parquet(file) for file in files]
    df = pd.concat(dfs, ignore_index=True)

    logging.info("Filas cargadas desde bronze: %s", len(df))
    return df

def extract_run_name(run_dir: Path) -> str:
    return run_dir.name

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(col).strip().lower() for col in df.columns]
    return df



def clean_text_data(df: pd.DataFrame) -> pd.DataFrame:
    
    def normalize_string(text):
        if not isinstance(text, str):
            return text
        

        text = text.lower()

        text = unicodedata.normalize('NFKD', text)
        text = "".join([char for char in text if not unicodedata.combining(char)])
   
        text = re.sub(r'[^a-z0-9\s]', '', text)
        
        text = " ".join(text.split())
        
        return text


    string_cols = df.select_dtypes(include=['object']).columns
    
    for col in string_cols:
        df[col] = df[col].apply(normalize_string)
        
    return df


def validate_required_columns(df: pd.DataFrame, required_columns: list[str]) -> None:
    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        raise ValueError(f"Faltan columnas requeridas en bronze: {missing}")

def normalize_types(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    typing_cfg = config["typing"]

    for col in typing_cfg.get("numeric_columns", []):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in typing_cfg.get("datetime_columns", []):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    for col in typing_cfg.get("text_columns", []):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    return df

def deduplicate_by_id(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    dedup_cfg = config["deduplication"]

    id_column = dedup_cfg["id_column"]
    order_column = dedup_cfg["order_column"]
    keep = dedup_cfg["keep"]

    before = len(df)

    df = df.sort_values([id_column, order_column])
    df = df.drop_duplicates(subset=[id_column], keep=keep)

    after = len(df)
    logging.info("Duplicados técnicos eliminados por %s: %s", id_column, before - after)

    return df
# ============================================================
# GUARDADO
# ============================================================
def save_fact_table(
    df: pd.DataFrame,
    run_name: str,
    fact_dir: Path,
    config: dict,
    table_name: str,
) -> None:
    fact_dir.mkdir(parents=True, exist_ok=True)

    fact_prefix = config["fact_table"]["file_prefix"]
    output_path = fact_dir / f"{fact_prefix}_{run_name}.parquet"

    df.to_parquet(output_path, index=False)

    logging.info("Archivo silver %s guardado en: %s", table_name, output_path)

# ============================================================
# RESUMEN
# ============================================================

