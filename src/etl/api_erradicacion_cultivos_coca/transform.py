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

CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "erradicacion_cultivos_transform.yaml"
LOG_DIR = PROJECT_ROOT / "logs"

# ============================================================
# TABLA DE HECHOS
# ============================================================

import pandas as pd
import logging

def build_erradicacion_fact(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    fact_cfg = config["fact_table"]
    
    
    grain = fact_cfg["grain"] 
    metric_column = fact_cfg["metric_column"] 
    rename_columns = fact_cfg["rename_columns"]

    
    df_temp = df.copy()
    df_temp['fecha_hecho'] = pd.to_datetime(df_temp['fecha_hecho']).dt.to_period('M').dt.to_timestamp()

    erradicacion_fact = (
        df_temp.groupby(grain, dropna=False, as_index=False)[metric_column]
        .sum()
        .rename(columns=rename_columns)
    )
    erradicacion_fact = clean_text_data(erradicacion_fact )
    # Ordenamiento (ajusté los nombres de columnas a los que usas en el grain)
    # Si en rename_columns cambiaste "cod_depto" por "id_dept", usa los nombres finales aquí.
    erradicacion_fact = erradicacion_fact.sort_values(
        ["date_event", "id_dept", "id_mun"]
    ).reset_index(drop=True)

    logging.info("Filas finales de erradicacion_fact (mensual): %s", len(erradicacion_fact))

    return erradicacion_fact

# ============================================================
# RESUMEN
# ============================================================
def log_summary(df: pd.DataFrame, erradicacion_fact: pd.DataFrame) -> None:
    logging.info("Resumen de transformación:")
    logging.info("Filas finales en erradicacion_fact: %s", len(erradicacion_fact))

    if not erradicacion_fact.empty:
        logging.info("Fecha mínimo en erradicacion_fact: %s", erradicacion_fact["date_event"].min())
        logging.info("Fecha máximo en erradicacion_fact: %s", erradicacion_fact["date_event"].max())


# ============================================================
# MAIN
# ============================================================
def run() -> None:
    setup_logging(LOG_DIR, "erradicacion_cultivos_transform.log")
    logging.info("Iniciando transformación de erradicacion de cultivos")

    config = load_transform_config("erradicacion_cultivos_transform", CONFIG_PATH)

    bronze_dir = PROJECT_ROOT / config["source"]["bronze_dir"]
    fact_dir = PROJECT_ROOT / config["source"]["silver_fact_dir"]
    fact_dir_golden = PROJECT_ROOT / config["source"]["golden_fact_dir"]

    run_dir = get_latest_bronze_run(bronze_dir)
    run_name = extract_run_name(run_dir)

    df = load_latest_bronze_run(run_dir)
    df = clean_columns(df)

    #validate_required_columns(df, config["validation"]["required_columns"])

    df = normalize_types(df, config)
    df = deduplicate_by_id(df, config)

    erradicacion_fact = build_erradicacion_fact(df, config)
    # Golden
    df_golden= erradicacion_fact.copy()
    df_golden["year"]= df_golden["date_event"].dt.year
    df_golden = df_golden.drop(columns=["date_event"])

    df_golden = (
        df_golden.groupby(["id_dept", "id_mun", "id_illicit_crop", "year"], dropna=False, as_index=False)["quantity"]
        .sum()
    )

    save_fact_table(erradicacion_fact, run_name, fact_dir, config, "erradicacion_cultivos")
    save_fact_table(df_golden, run_name, fact_dir_golden, config, "erradicacion_cultivos")
    log_summary(df, erradicacion_fact)

    logging.info("Transformación finalizada correctamente")


if __name__ == "__main__":
    run()