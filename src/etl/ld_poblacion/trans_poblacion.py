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

CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "poblacion_transform.yaml"
LOG_DIR = PROJECT_ROOT / "logs"


# ============================================================
# TABLA DE HECHOS
# ============================================================

def build_poblacion_fact(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    fact_cfg = config["fact_table"]

    grain = fact_cfg["grain"]
    metric_column = fact_cfg["metric_column"]
    rename_columns = fact_cfg["rename_columns"]
    output_columns = fact_cfg["output_columns"]
    
    table_fact = (
        df.groupby(grain, dropna=False, as_index=False)[metric_column]
        .sum()
        .rename(columns=rename_columns)
    )
    


    table_fact = table_fact.sort_values(
        ["year", "id_mun"]
    ).reset_index(drop=True)


    table_fact = table_fact[output_columns]

    logging.info("Filas finales de table_fact: %s", len(table_fact))

    return table_fact


# ============================================================
# GUARDADO
# ============================================================
"""
save_table_fact(table_fact, run_name, fact_dir, config)
"""

# ============================================================
# RESUMEN
# ============================================================
def log_summary(df: pd.DataFrame, table_fact: pd.DataFrame) -> None:
    logging.info("Resumen de transformación:")
    logging.info("Filas finales en table_fact: %s", len(table_fact))

    if not table_fact.empty:
        logging.info("Año mínimo en table_fact: %s", table_fact["year"].min())
        logging.info("Año máximo en table_fact: %s", table_fact["year"].max())


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    setup_logging(LOG_DIR, "poblacion_transform.log")
    logging.info("Iniciando transformación de Población")

    config = load_transform_config("poblacion_transform", CONFIG_PATH)

    silver_dir = PROJECT_ROOT / config["source"]["silver_fact_dir"]
    fact_dir = PROJECT_ROOT / config["source"]["golden_fact_dir"]

    #run_dir = get_latest_bronze_run(bronze_dir)
    #run_name = extract_run_name(run_dir)

    df,run_name = load_latest_silver_run(silver_dir,".xlsx")
    df = df[df["Unidad de Medida"] != "Porcentaje (el valor está multiplicado por 100)"]
    df = df[df["Código Entidad"] != 1001]
    df = df[df["Código Entidad"] != 13000]
    df = df[~df["Código Entidad"].astype(str).str.endswith("000", na=False)]
    df["Código Entidad"] = df["Código Entidad"].astype(str).str.strip().str.zfill(5)

    validate_required_columns(df, config["validation"]["required_columns"])
    df["Dato Numérico"] = (
        df["Dato Numérico"]
        .astype(str)
        .str.replace(".", "", regex=False)   # quitar miles
        .str.replace(",", ".", regex=False)  # convertir decimal
    )

    df["Dato Numérico"] = pd.to_numeric(df["Dato Numérico"], errors="coerce")

    df = normalize_types(df, config)
    print(df.head())

    table_fact = build_poblacion_fact(df, config)
    table_fact=round_columns(table_fact, "population", 0)
    save_fact_table(table_fact, run_name, fact_dir, config, "poblacion")
    log_summary(df, table_fact)

    logging.info("Transformación finalizada correctamente")


if __name__ == "__main__":
    main()