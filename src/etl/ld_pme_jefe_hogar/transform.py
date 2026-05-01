from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.config_utils import load_yaml
from src.etl.utils.transform_utils import load_transform_config, get_latest_bronze_run,load_latest_bronze_run,extract_run_name,load_latest_silver_run,ensure_two_digits,round_columns,clean_columns,validate_required_columns,normalize_types,deduplicate_by_id,save_fact_table

# ============================================================
# RUTAS DEL PROYECTO
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parents[3]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "pme_jefe_hogar_transform.yaml"
LOG_DIR = PROJECT_ROOT / "logs"


# ============================================================
# TABLA DE HECHOS
# ============================================================

def build_pme_jefe_hogar_fact(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    fact_cfg = config["fact_table"]

    grain = fact_cfg["grain"]
    metric_column = fact_cfg["metric_column"]
    rename_columns = fact_cfg["rename_columns"]
    output_columns = fact_cfg["output_columns"]

    pme_fact = (
        df.groupby(grain, dropna=False, as_index=False)[metric_column]
        .mean()
        .rename(columns=rename_columns)
    )
    
    pme_fact["id_gender"] = pme_fact["id_gender"].map({
    "hombre": 1,
    "mujer": 2
        })


    pme_fact = pme_fact.sort_values(
        ["year", "id_dept", "id_gender"]
    ).reset_index(drop=True)


    pme_fact = pme_fact[output_columns]

    logging.info("Filas finales de pme_fact: %s", len(pme_fact))

    return pme_fact


# ============================================================
# GUARDADO
# ============================================================
"""
save_pme_fact(pme_fact, run_name, fact_dir, config)
"""

# ============================================================
# RESUMEN
# ============================================================
def log_summary(df: pd.DataFrame, pme_fact: pd.DataFrame) -> None:
    logging.info("Resumen de transformación:")
    logging.info("Filas finales en pme_fact: %s", len(pme_fact))

    if not pme_fact.empty:
        logging.info("Año mínimo en pme_fact: %s", pme_fact["year"].min())
        logging.info("Año máximo en pme_fact: %s", pme_fact["year"].max())


# ============================================================
# MAIN
# ============================================================
def run() -> None:
    setup_logging(LOG_DIR, "pme_jefe_hogar_transform.log")
    logging.info("Iniciando transformación de PME Jefe de Hogar")

    config = load_transform_config("pme_jefe_hogar_transform", CONFIG_PATH)

    silver_dir = PROJECT_ROOT / config["source"]["silver_fact_dir"]
    fact_dir = PROJECT_ROOT / config["source"]["golden_fact_dir"]

    #run_dir = get_latest_bronze_run(bronze_dir)
    #run_name = extract_run_name(run_dir)

    df,run_name = load_latest_silver_run(silver_dir)

    df = clean_columns(df)
    

    validate_required_columns(df, config["validation"]["required_columns"])

    df = normalize_types(df, config)
    df= ensure_two_digits(df, "id_dept")

    pme_fact = build_pme_jefe_hogar_fact(df, config)
    pme_fact=round_columns(pme_fact, "mp_idx_val", 2)
    save_fact_table(pme_fact, run_name, fact_dir, config, "pme_hh")
    log_summary(df, pme_fact)

    logging.info("Transformación finalizada correctamente")


if __name__ == "__main__":
    run()