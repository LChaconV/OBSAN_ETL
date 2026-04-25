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

def load_latest_silver_run(silver_dir: Path, extension: str = ".csv") -> pd.DataFrame:
    # 1. Buscar archivos con la extensión indicada
    files = [p for p in silver_dir.iterdir() if p.is_file() and p.suffix == extension]

    # 2. Validar que existan archivos
    if not files:
        raise ValueError(f"No hay archivos {extension} en {silver_dir}")

    # 3. Ordenar y tomar el más reciente (por nombre)
    latest_file = sorted(files)[-1]
    run_part = "run_" + latest_file.stem.split("run_")[1]
    logging.info("Archivo más reciente: %s", latest_file.name)

    # 4. Leer el archivo como DataFrame
    if extension == ".csv":
        df = pd.read_csv(latest_file)
    elif extension == ".parquet":
        df = pd.read_parquet(latest_file)
    elif extension == ".xlsx":
        df = pd.read_excel(latest_file)
    else:
        raise ValueError(f"Extensión no soportada: {extension}")

    return df, run_part

def ensure_two_digits(df: pd.DataFrame, column: str) -> pd.DataFrame:
    df[column] = df[column].apply(
        lambda x: str(int(x)).zfill(2) if pd.notnull(x) else x
    )
    return df
def ensure_five_digits(df: pd.DataFrame, column: str) -> pd.DataFrame:
    df[column] = df[column].apply(
        lambda x: str(int(x)).zfill(5) if pd.notnull(x) and str(x).strip() != "" else None
    )
    return df

def round_columns(df: pd.DataFrame, columns, n: int = 2) -> pd.DataFrame:
    if isinstance(columns, str):
        columns = [columns]

    for col in columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(n)

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

    # 🔥 aplicar a columnas tipo string
    str_cols = df.select_dtypes(include=["object", "string"]).columns

    for col in str_cols:
        df[col] = df[col].apply(normalize_string)

    return df


    string_cols = df.select_dtypes(include=['object']).columns
    
    for col in string_cols:
        df[col] = df[col].apply(normalize_string)
        
    return df


def validate_required_columns(df: pd.DataFrame, required_columns: list[str]) -> None:
    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        raise ValueError(f"Faltan columnas requeridas en bronze: {missing}")

def normalize_types(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    typing_cfg = config.get("typing", {})

    numeric_columns = typing_cfg.get("numeric_columns") or []
    datetime_columns = typing_cfg.get("datetime_columns") or []
    text_columns = typing_cfg.get("text_columns") or []

    if numeric_columns:
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

    if datetime_columns:
        for col in datetime_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    if text_columns:
        for col in text_columns:
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

def parse_fecha_corte(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    date_cfg = config["date_parsing"]

    source_col = date_cfg["source_column"]
    raw_col = date_cfg["raw_column"]
    parsed_col = date_cfg["parsed_column"]
    invalid_col = date_cfg["invalid_flag_column"]
    formats = date_cfg["formats"]

    df[raw_col] = df[source_col]

    parsed_series = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")

    for fmt in formats:
        parsed_try = pd.to_datetime(
            df[raw_col],
            format=fmt,
            errors="coerce",
           # dayfirst=True if fmt == "%d/%m/%Y" else False,
        )
        parsed_series = parsed_series.fillna(parsed_try)

    df[parsed_col] = parsed_series
    df[invalid_col] = df[parsed_col].isna()

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

