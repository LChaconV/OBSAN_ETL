from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.config_utils import load_yaml
from src.etl.utils.transform_utils import load_transform_config, get_latest_bronze_run,load_latest_bronze_run,extract_run_name,clean_columns,validate_required_columns,normalize_types,deduplicate_by_id,save_fact_table

# ============================================================
# RUTAS DEL PROYECTO
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parents[3]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "irca_transform.yaml"
LOG_DIR = PROJECT_ROOT / "logs"


# ============================================================
# TABLA DE HECHOS
# ============================================================

def build_irca_fact(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    fact_cfg = config["fact_table"]

    grain = fact_cfg["grain"]
    metric_column = fact_cfg["metric_column"]
    rename_columns = fact_cfg["rename_columns"]
    output_columns = fact_cfg["output_columns"]

    irca_fact = (
        df.groupby(grain, dropna=False, as_index=False)[metric_column]
        .mean()
        .rename(columns=rename_columns)
    )

    irca_fact = irca_fact.sort_values(
        ["year", "id_dept", "id_mun"]
    ).reset_index(drop=True)

    irca_fact["id_irca_fact"] = range(1, len(irca_fact) + 1)

    irca_fact = irca_fact[output_columns]

    logging.info("Filas finales de irca_fact: %s", len(irca_fact))

    return irca_fact


# ============================================================
# GUARDADO
# ============================================================
"""
save_irca_fact(irca_fact, run_name, fact_dir, config)
"""

# ============================================================
# RESUMEN
# ============================================================
def log_summary(df: pd.DataFrame, irca_fact: pd.DataFrame) -> None:
    logging.info("Resumen de transformación:")
    logging.info("Filas finales en irca_fact: %s", len(irca_fact))

    if not irca_fact.empty:
        logging.info("Año mínimo en irca_fact: %s", irca_fact["year"].min())
        logging.info("Año máximo en irca_fact: %s", irca_fact["year"].max())


# ============================================================
# MAIN
# ============================================================
def run() -> None:
    setup_logging(LOG_DIR, "irca_transform.log")
    logging.info("Iniciando transformación de IRCA")

    config = load_transform_config("irca_transform", CONFIG_PATH)

    bronze_dir = PROJECT_ROOT / config["source"]["bronze_dir"]
    fact_dir = PROJECT_ROOT / config["source"]["silver_fact_dir"]

    run_dir = get_latest_bronze_run(bronze_dir)
    run_name = extract_run_name(run_dir)

    df = load_latest_bronze_run(run_dir)
    df = clean_columns(df)

    validate_required_columns(df, config["validation"]["required_columns"])

    df = normalize_types(df, config)
    df = deduplicate_by_id(df, config)

    irca_fact = build_irca_fact(df, config)

    save_fact_table(irca_fact, run_name, fact_dir, config, "irca")
    log_summary(df, irca_fact)

    logging.info("Transformación finalizada correctamente")


if __name__ == "__main__":
    run()