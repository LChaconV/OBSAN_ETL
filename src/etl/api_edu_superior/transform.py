from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.config_utils import load_yaml
from src.etl.utils.transform_utils import ensure_five_digits, load_transform_config, get_latest_bronze_run,load_latest_bronze_run,extract_run_name,clean_columns,clean_text_data,validate_required_columns,normalize_types,deduplicate_by_id,save_fact_table

# ============================================================
# RUTAS DEL PROYECTO
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parents[3]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "edu_superior_transform.yaml"

LOG_DIR = PROJECT_ROOT / "logs"


def build_table(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    fact_cfg = config["fact_table"]
    
    
    grain = fact_cfg["grain"] 
    metric_column = fact_cfg["metric_column"] 
    rename_columns = fact_cfg["rename_columns"]

    
    df_temp = df.copy()
    
    
    table_fact = (
        df_temp.groupby(grain, dropna=False, as_index=False)[metric_column]
        .sum()
        .rename(columns=rename_columns)
    )
    
    table_fact = clean_text_data(table_fact )

    table_fact = table_fact.sort_values(
        ["year", "id_mun"]
    ).reset_index(drop=True)


    logging.info("Filas finales de table_fact (mensual): %s", len(table_fact))
    
    return table_fact
    
# ============================================================
# RESUMEN
# ============================================================
def log_summary(df: pd.DataFrame, table_fact: pd.DataFrame) -> None:
    logging.info("Resumen de transformación:")
    logging.info("Filas finales en table_fact: %s", len(table_fact))

    if not table_fact.empty:
        logging.info("Fecha mínimo en table_fact: %s", table_fact["year"].min())
        logging.info("Fecha máximo en table_fact: %s", table_fact["year"].max())


# ============================================================
# MAIN
# ============================================================
def run() -> None:
    setup_logging(LOG_DIR, "edu_superior.log")
    logging.info("Iniciando transformación")

    config = load_transform_config("edu_superior_transform", CONFIG_PATH)

    bronze_dir = PROJECT_ROOT / config["source"]["bronze_dir"]
    fact_dir = PROJECT_ROOT / config["source"]["silver_fact_dir"]
    fact_dir_golden = PROJECT_ROOT / config["source"]["golden_fact_dir"]

    run_dir = get_latest_bronze_run(bronze_dir)
    run_name = extract_run_name(run_dir)

    df = load_latest_bronze_run(run_dir)
    df = clean_columns(df)


    df = normalize_types(df, config)
    df = deduplicate_by_id(df, config)

    table_fact = build_table(df, config)
    table_fact = ensure_five_digits(table_fact, "id_mun")

    # Golden
    

    save_fact_table(table_fact, run_name, fact_dir, config, "edu_superior")
    save_fact_table(table_fact, run_name, fact_dir_golden, config, "edu_superior")
    log_summary(df, table_fact)

    logging.info("Transformación finalizada correctamente")

if __name__ == "__main__":
    run()