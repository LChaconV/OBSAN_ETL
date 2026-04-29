from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.config_utils import load_yaml
from src.etl.utils.transform_utils import load_transform_config, get_latest_bronze_run,load_latest_bronze_run,extract_run_name,clean_columns,clean_text_data,validate_required_columns,normalize_types,deduplicate_by_id,save_fact_table

# ============================================================
# RUTAS DEL PROYECTO
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parents[3]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "familias_accion_transform.yaml"
LOG_DIR = PROJECT_ROOT / "logs"


def build_familias_benefit_fact(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    fact_cfg = config["fact_table"]
    
    
    grain = fact_cfg["grain"] 
    metric_column = fact_cfg["metric_column"] 
    rename_columns = fact_cfg["rename_columns"]

    
    df_temp = df.copy()
    df_temp['fechaultimobeneficioasignado'] = pd.to_datetime(df_temp['fechaultimobeneficioasignado']).dt.to_period('M').dt.to_timestamp()

    familias_benefit_fact = (
        df_temp.groupby(grain, dropna=False, as_index=False)[metric_column]
        .sum()
        .rename(columns=rename_columns)
    )
    familias_benefit_fact = clean_text_data(familias_benefit_fact )

    familias_benefit_fact = familias_benefit_fact.sort_values(
        ["date_event", "id_dept", "id_mun"]
    ).reset_index(drop=True)

    logging.info("Filas finales de familias_benefit_fact (mensual): %s", len(familias_benefit_fact))

    return familias_benefit_fact

# ============================================================
# RESUMEN
# ============================================================
def log_summary(df: pd.DataFrame, familias_benefit_fact: pd.DataFrame) -> None:
    logging.info("Resumen de transformación:")
    logging.info("Filas finales en familias_benefit_fact: %s", len(familias_benefit_fact))

    if not familias_benefit_fact.empty:
        logging.info("Fecha mínimo en familias_benefit_fact: %s", familias_benefit_fact["date_event"].min())
        logging.info("Fecha máximo en familias_benefit_fact: %s", familias_benefit_fact["date_event"].max())


# ============================================================
# MAIN
# ============================================================
def run() -> None:
    setup_logging(LOG_DIR, "familias_accion_transform.log")
    logging.info("Iniciando transformación de beneficiarios de Familias en Acción")

    config = load_transform_config("familias_accion_transform", CONFIG_PATH)

    bronze_dir = PROJECT_ROOT / config["source"]["bronze_dir"]
    fact_dir = PROJECT_ROOT / config["source"]["silver_fact_dir"]

    run_dir = get_latest_bronze_run(bronze_dir)
    run_name = extract_run_name(run_dir)

    df = load_latest_bronze_run(run_dir)
    df = clean_columns(df)

    #validate_required_columns(df, config["validation"]["required_columns"])

    df = normalize_types(df, config)
    df = deduplicate_by_id(df, config)

    familias_benefit_fact = build_familias_benefit_fact(df, config)

    save_fact_table(familias_benefit_fact, run_name, fact_dir, config, "familias_accion")
    log_summary(df, familias_benefit_fact)

    logging.info("Transformación finalizada correctamente")


if __name__ == "__main__":
    run()