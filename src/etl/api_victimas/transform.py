from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.config_utils import load_yaml


# ============================================================
# RUTAS DEL PROYECTO
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parents[3]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "victimas_transform.yaml"
LOG_DIR = PROJECT_ROOT / "logs"


# ============================================================
# CONFIG
# ============================================================
def load_transform_config() -> dict:
    config = load_yaml(CONFIG_PATH)

    if "victimas_transform" not in config:
        raise KeyError("No existe la clave 'victimas_transform' en el YAML de transformación.")

    return config["victimas_transform"]


# ============================================================
# BRONZE
# ============================================================
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


# ============================================================
# LIMPIEZA
# ============================================================
def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(col).strip().lower() for col in df.columns]
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
# DEDUPLICACIÓN Y REGLAS DE NEGOCIO
# ============================================================
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


def apply_business_rules(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    rules_cfg = config["business_rules"]
    metric_column = rules_cfg["metric_column"]

    if rules_cfg.get("drop_null_in_metric", False):
        before = len(df)
        df = df.dropna(subset=[metric_column])
        after = len(df)
        logging.info("Filas excluidas por %s nulo: %s", metric_column, before - after)

    return df


# ============================================================
# DIMENSIÓN ESTÁTICA
# ============================================================
def build_current_event_dimension(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    dim_cfg = config["dimensions"]["victim_event_type"]

    source_id_col = dim_cfg["source_id_column"]
    source_name_col = dim_cfg["source_name_column"]
    output_id_col = dim_cfg["output_id_column"]
    output_name_col = dim_cfg["output_name_column"]

    event_df = (
        df[[source_id_col, source_name_col]]
        .dropna(subset=[source_id_col])
        .drop_duplicates()
        .rename(
            columns={
                source_id_col: output_id_col,
                source_name_col: output_name_col,
            }
        )
        .sort_values(output_id_col)
        .reset_index(drop=True)
    )

    event_df[output_id_col] = pd.to_numeric(event_df[output_id_col], errors="coerce")
    event_df = event_df.dropna(subset=[output_id_col])
    event_df[output_id_col] = event_df[output_id_col].astype(int)

    return event_df


def upsert_victim_event_type(df: pd.DataFrame, dimension_dir: Path, config: dict) -> pd.DataFrame:
    """
    Crea la dimensión si no existe.
    Si ya existe, agrega solo nuevos eventos.
    """
    dim_cfg = config["dimensions"]["victim_event_type"]
    file_name = dim_cfg["file_name"]
    output_id_col = dim_cfg["output_id_column"]
    output_name_col = dim_cfg["output_name_column"]

    dimension_dir.mkdir(parents=True, exist_ok=True)
    dim_path = dimension_dir / file_name

    current_dim = build_current_event_dimension(df, config)

    if not dim_path.exists():
        current_dim.to_parquet(dim_path, index=False)
        logging.info("Dimensión victim_event_type creada en: %s", dim_path)
        logging.info("Eventos guardados: %s", len(current_dim))
        return current_dim

    existing_dim = pd.read_parquet(dim_path)

    combined_dim = (
        pd.concat([existing_dim, current_dim], ignore_index=True)
        .drop_duplicates(subset=[output_id_col], keep="last")
        .sort_values(output_id_col)
        .reset_index(drop=True)
    )

    combined_dim.to_parquet(dim_path, index=False)

    new_events = len(combined_dim) - len(existing_dim)
    logging.info("Dimensión victim_event_type actualizada en: %s", dim_path)
    logging.info("Nuevos eventos agregados: %s", new_events)
    logging.info("Total eventos en dimensión: %s", len(combined_dim))

    return combined_dim


# ============================================================
# TABLA DE HECHOS SILVER
# ============================================================
def build_victim_unit(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    fact_cfg = config["fact_table"]

    grain = fact_cfg["grain"]
    metric_column = fact_cfg["metric_column"]
    rename_columns = fact_cfg["rename_columns"]
    output_columns = fact_cfg["output_columns"]

    victim_unit = (
        df.groupby(grain, dropna=False, as_index=False)[metric_column]
        .sum()
        .rename(columns=rename_columns)
    )

    victim_unit = victim_unit.sort_values(
        ["date_victim", "id_mun", "id_victim_event", "sexo"]
    ).reset_index(drop=True)


    if "id_victim_event" in victim_unit.columns:
        victim_unit["id_victim_event"] = pd.to_numeric(
            victim_unit["id_victim_event"],
            errors="coerce"
        )

    victim_unit = victim_unit[output_columns]

    logging.info("Filas finales de victim_unit: %s", len(victim_unit))

    return victim_unit

# ============================================================
# TABLA DE HECHOS GOLDEN
# ============================================================
def build_victim_unit_golden(df: pd.DataFrame, config: dict) -> pd.DataFrame:

    fact_cfg = config["fact_table"]
    grain = fact_cfg["grain"]
    metric_column = fact_cfg["metric_column"]
    rename_columns = fact_cfg["rename_columns"]


    df_golden = df.copy()
    df_golden["year"] = df_golden["date_victim"].dt.year

    golden_grain = [col for col in grain if col != "date_victim"]
    
    if "year" not in golden_grain:
        golden_grain.append("year")
    
    logging.info("Agrupando en Capa Golden por: %s", golden_grain)

    victim_unit_golden = (
        df_golden.groupby(golden_grain, dropna=False, as_index=False)[metric_column]
        .sum()
        .rename(columns=rename_columns)
    )



    logging.info("Filas finales de victim_unit_golden: %s", len(victim_unit_golden))

    return victim_unit_golden



# ============================================================
# GUARDADO
# ============================================================
def save_victim_unit(
    victim_unit: pd.DataFrame,
    run_name: str,
    fact_dir: Path,
    config: dict,
) -> None:
    fact_dir.mkdir(parents=True, exist_ok=True)

    fact_prefix = config["fact_table"]["file_prefix"]
    victim_unit_path = fact_dir / f"{fact_prefix}_{run_name}.parquet"

    victim_unit.to_parquet(victim_unit_path, index=False)

    logging.info("Archivo silver victim_unit guardado en: %s", victim_unit_path)


# ============================================================
# RESUMEN
# ============================================================
def log_summary(df: pd.DataFrame, victim_unit: pd.DataFrame, config: dict) -> None:
    invalid_col = config["date_parsing"]["invalid_flag_column"]
    invalid_dates = int(df[invalid_col].sum()) if invalid_col in df.columns else 0

    logging.info("Resumen de transformación:")
    logging.info("Fechas no parseadas: %s", invalid_dates)

    if not victim_unit.empty:
        logging.info("Fecha mínima en victim_unit: %s", victim_unit["date_victim"].min())
        logging.info("Fecha máxima en victim_unit: %s", victim_unit["date_victim"].max())

    logging.info("Filas finales en victim_unit: %s", len(victim_unit))


# ============================================================
# MAIN
# ============================================================
def run() -> None:
    setup_logging(LOG_DIR, "victimas_transform.log")
    logging.info("Iniciando transformación de víctimas")

    config = load_transform_config()

    bronze_dir = PROJECT_ROOT / config["source"]["bronze_dir"]
    fact_dir = PROJECT_ROOT / config["source"]["silver_fact_dir"]
    fact_dir_golden = PROJECT_ROOT / config["source"]["golden_fact_dir"]
    dimension_dir = PROJECT_ROOT / config["source"]["silver_dimension_dir"]

    run_dir = get_latest_bronze_run(bronze_dir)
    run_name = extract_run_name(run_dir)

    df = load_latest_bronze_run(run_dir)
    df = clean_columns(df)

    validate_required_columns(df, config["validation"]["required_columns"])

    df = normalize_types(df, config)
    df = parse_fecha_corte(df, config)
    df = deduplicate_by_id(df, config)
    df_cleaned = apply_business_rules(df, config)

    event_df = upsert_victim_event_type(df, dimension_dir, config)
    victim_unit = build_victim_unit(df_cleaned, config)
    victim_unit_golden = build_victim_unit_golden(df_cleaned, config)

    save_victim_unit(victim_unit, run_name, fact_dir, config)
    save_victim_unit(victim_unit_golden, run_name, fact_dir_golden, config)
    log_summary(df, victim_unit, config)

    logging.info("Transformación finalizada correctamente")


if __name__ == "__main__":
    run()