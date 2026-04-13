from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.config_utils import load_yaml


PROJECT_ROOT = Path(__file__).resolve().parents[3]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# ============================================================
# CONFIG
# ============================================================
def load_transform_config(key,CONFIG_PATH) -> dict:
    config = load_yaml(CONFIG_PATH)

    if key not in config:
        raise KeyError(f"No existe la clave '{key}' en el YAML.")

    return config[key]


# ============================================================
# BRONZE
# ============================================================
def get_latest_bronze_run(bronze_dir: Path) -> Path:
    run_dirs = [p for p in bronze_dir.iterdir() if p.is_dir() and p.name.startswith("run_")]

    if not run_dirs:
        raise ValueError(f"No hay carpetas run_ en {bronze_dir}")

    latest = sorted(run_dirs)[-1]
    logging.info("Última corrida: %s", latest.name)
    return latest

def load_latest_bronze_run(run_dir: Path) -> pd.DataFrame:
    """
    Carga y concatena archivos de datos desde un directorio específico.
    Implementa separación de estados y logging formal.
    """
    # Definición de extensiones soportadas y su motor de lectura
    supported_extensions = {
        ".xlsx": pd.read_excel,
        ".xls": pd.read_excel,
        ".csv": pd.read_csv
    }

    files = []
    for ext in supported_extensions.keys():
        files.extend(run_dir.glob(f"*{ext}"))

    if not files:
        logging.error("Fallo en la carga: No se encontraron archivos en %s", run_dir)
        raise ValueError(f"No se encontraron archivos compatibles en {run_dir}")

    dataframes = []
    for file_path in files:
        try:
            reader = supported_extensions.get(file_path.suffix)
            if reader:
                dataframes.append(reader(file_path))
        except Exception as e:
            logging.error("Error al procesar el archivo %s: %s", file_path.name, e)
            continue

    if not dataframes:
        raise ValueError("No se pudo cargar ningún DataFrame válido.")

    df = pd.concat(dataframes, ignore_index=True)

    # Registro formal de métricas de ingesta
    logging.info(
        "Ingesta completada: [Documentos leídos: %s] [Filas totales: %s] [Directorio: %s]",
        len(files),
        len(df),
        run_dir.name
    )

    return df


def extract_run_name(run_dir: Path) -> str:
    return run_dir.name


# ============================================================
# LIMPIEZA
# ============================================================
def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df


def validate_required_columns(df: pd.DataFrame, required: list[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas: {missing}")


def apply_filter(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    f = config["filter"]
    return df[df[f["column"]] == f["value"]]


def normalize_types(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    typing = config["typing"]

    for col in typing.get("string_columns", []):
        if col in df.columns:
            df[col] = df[col].astype(str)

    for col in typing.get("numeric_columns", []):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# ============================================================
# FECHAS
# ============================================================
def parse_dates(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    cfg = config["date_parsing"]

    source = cfg["source_column"]
    parsed = cfg["parsed_column"]
    invalid = cfg["invalid_flag_column"]

    df[parsed] = pd.to_datetime(df[source], errors="coerce")
    df[invalid] = df[parsed].isna()

    return df


def transform_dates(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    cfg = config["date_transformation"]

    if cfg["type"] == "month_start":
        col = cfg["column"]
        df[col] = df[col].dt.to_period("M").dt.to_timestamp()

    return df


# ============================================================
# DERIVADAS + RENOMBRE
# ============================================================
def create_id_muni(df: pd.DataFrame) -> pd.DataFrame:
    df["COD_DPTO_O"] = df["COD_DPTO_O"].astype(str).str.zfill(2)
    df["COD_MUN_O"] = df["COD_MUN_O"].astype(str).str.zfill(3)
    df["id_muni"] = df["COD_DPTO_O"] + df["COD_MUN_O"]
    return df


def rename_columns(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    return df.rename(columns=config["rename_columns"])




# ============================================================
# FACT TABLE
# ============================================================
def build_fact_table(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    fact = config["fact_table"]

    group_cols = fact["grain"]
    metric_name = list(fact["aggregation"].keys())[0]

    df_fact = (
        df.groupby(group_cols, dropna=False)
        .size()
        .reset_index(name=metric_name)
    )

    logging.info("Filas finales en silver: %s", len(df_fact))
    return df_fact


# ============================================================
# GUARDADO
# ============================================================
def save_fact(df: pd.DataFrame, run_name: str, fact_dir: Path, config: dict):
    fact_dir.mkdir(parents=True, exist_ok=True)

    prefix = config["fact_table"]["file_prefix"]
    path = fact_dir / f"{prefix}_{run_name}.csv"

    df.to_csv(path, index=False)
    logging.info("Archivo guardado en: %s", path)

# ============================================================
# GOLDEN
# ============================================================
def build_fact_table_golden(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    fact = config["fact_table_golden"]

    group_cols = fact["grain_golden"]
    metric_name = list(fact["aggregation"].keys())[0]

    df_fact = (
        df.groupby(group_cols, dropna=False)
        .size()
        .reset_index(name=metric_name)
    )

    logging.info("Filas finales en golden: %s", len(df_fact))
    return df_fact

def save_fact_golden(df: pd.DataFrame, run_name: str, fact_dir: Path, config: dict):
    fact_dir.mkdir(parents=True, exist_ok=True)

    prefix = config["fact_table_golden"]["file_prefix"]
    path = fact_dir / f"{prefix}_{run_name}.csv"

    df.to_csv(path, index=False)
    logging.info("Archivo guardado en: %s", path)

# ============================================================
# MAIN
# ============================================================
def silver_transform(key: str,LOG_DIR: Path,CONFIG_PATH: Path):   
    log_name=f"{key}_transform.log" 
    setup_logging(LOG_DIR, log_name)
    logging.info("Inicio transformación")

    config = load_transform_config(key,CONFIG_PATH)

    bronze_dir = PROJECT_ROOT / config["source"]["bronze_dir"]
    fact_dir = PROJECT_ROOT / config["source"]["silver_fact_dir"]

    run_dir = get_latest_bronze_run(bronze_dir)
    run_name = extract_run_name(run_dir)

    df = load_latest_bronze_run(run_dir)
    df = clean_columns(df)

    validate_required_columns(df, config["validation"]["required_columns"])

    df = apply_filter(df, config)
    df = normalize_types(df, config)

    df = parse_dates(df, config)
    df = transform_dates(df, config)

    df = create_id_muni(df)
    df = rename_columns(df, config)

 

    df_fact = build_fact_table(df, config)

    save_fact(df_fact, run_name, fact_dir, config)

    logging.info("Transformación finalizada correctamente")

    return df_fact, config,run_name

if __name__ == "__main__":
    
    # ============================================================
    # RUTAS
    # ============================================================



    CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "mortalidad_desnutricion_transform.yaml"
    LOG_DIR = PROJECT_ROOT / "logs"

    df, config,run_name = silver_transform(
        "mortalidad_desnutricion_transform",
        LOG_DIR,
        CONFIG_PATH
    )
    build_fact_table_golden(df, config)
    save_fact_golden(df, run_name, PROJECT_ROOT / config["source"]["golden_fact_dir"], config)
                       