from __future__ import annotations

import logging
import sys
from pathlib import Path

from shapely import Point
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

CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "produc_gas_transform.yaml"
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
    
    #table_fact = clean_text_data(table_fact )

    table_fact = table_fact.sort_values(
        ["year", "month", "latitud", "longitud"]
    ).reset_index(drop=True)
    ## Construir una fecha
    table_fact["date_event"] = pd.to_datetime(
    dict(year=table_fact["year"], month=table_fact["month"], day=1)
    )

    logging.info("Filas finales de table_fact (mensual): %s", len(table_fact))
    
    return table_fact
    
# ============================================================
# RESUMEN
# ============================================================
def log_summary(df: pd.DataFrame, table_fact: pd.DataFrame) -> None:
    logging.info("Resumen de transformación:")
    logging.info("Filas finales en table_fact: %s", len(table_fact))

    if not table_fact.empty:
        logging.info("Fecha mínimo en table_fact: %s", table_fact["date_event"].min())
        logging.info("Fecha máximo en table_fact: %s", table_fact["date_event"].max())


# ============================================================
# MAIN
# ============================================================
def run() -> None:
    setup_logging(LOG_DIR, "produc_gas.log")
    logging.info("Iniciando transformación de produccion de petroleo")

    config = load_transform_config("produc_gas_transform", CONFIG_PATH)

    bronze_dir = PROJECT_ROOT / config["source"]["bronze_dir"]
    fact_dir = PROJECT_ROOT / config["source"]["silver_fact_dir"]
    fact_dir_golden = PROJECT_ROOT / config["source"]["golden_fact_dir"]

    run_dir = get_latest_bronze_run(bronze_dir)
    run_name = extract_run_name(run_dir)

    df = load_latest_bronze_run(run_dir)
    df = clean_columns(df)

    validate_required_columns(df, config["validation"]["required_columns"])

    df = normalize_types(df, config)
    df = deduplicate_by_id(df, config)

    table_fact = build_table(df, config)

    # Golden
    df_golden= table_fact.copy()

    df_golden = (
        df_golden.groupby(["latitud","longitud", "year"], dropna=False, as_index=False)["produc_kpc"]
        .sum()
    )
    df_golden["geometry"] = df_golden.apply(
       lambda r: Point(r["longitud"], r["latitud"]),
        axis=1
    )
    df_golden["geometry"] = df_golden["geometry"].astype(str)
    df_golden = df_golden[df_golden["geometry"] != "POINT (0 0)"]

    df_golden = df_golden.drop(columns=["latitud", "longitud"])

    save_fact_table(table_fact, run_name, fact_dir, config, "produc_gas")
    save_fact_table(df_golden, run_name, fact_dir_golden, config, "produc_gas")
    log_summary(df, table_fact)

    logging.info("Transformación finalizada correctamente")

if __name__ == "__main__":
    run()