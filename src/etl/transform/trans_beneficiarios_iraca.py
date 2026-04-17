from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.config_utils import load_yaml
from src.etl.utils.transform_utils import load_transform_config, get_latest_bronze_run,load_latest_bronze_run,extract_run_name,clean_text_data,clean_columns,parse_fecha_corte, validate_required_columns,normalize_types,deduplicate_by_id,save_fact_table

# ============================================================
# RUTAS DEL PROYECTO
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parents[3]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "beneficiarios_iraca_transform.yaml"
LOG_DIR = PROJECT_ROOT / "logs"


# ============================================================
# TABLA DE HECHOS
# ============================================================

def build_beneficiarios_iraca_fact(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    fact_cfg = config["fact_table"]

    grain = fact_cfg["grain"]
    metric_column = fact_cfg["metric_column"]
    rename_columns = fact_cfg["rename_columns"]
    output_columns = fact_cfg["output_columns"]
   
    beneficiarios_iraca_fact = (
        df.groupby(grain, dropna=False, as_index=False)[metric_column]
        .sum()
        .rename(columns=rename_columns)
    )

    beneficiarios_iraca_fact = beneficiarios_iraca_fact.sort_values(
        ["date_iraca", "id_mun","status","type"]
    ).reset_index(drop=True)

    beneficiarios_iraca_fact["id_beneficiarios_iraca_fact"] = range(1, len(beneficiarios_iraca_fact) + 1)

    beneficiarios_iraca_fact = beneficiarios_iraca_fact[output_columns]

    logging.info("Filas finales de beneficiarios_iraca_fact: %s", len(beneficiarios_iraca_fact))

    return beneficiarios_iraca_fact

def build_beneficiarios_iraca_fact_golden(df: pd.DataFrame, config: dict) -> pd.DataFrame:

    fact_cfg = config["fact_table"]
    grain = fact_cfg["grain"]
    metric_column = fact_cfg["metric_column"]
    rename_columns = fact_cfg["rename_columns"]


    df_golden = df.copy()
    df_golden["fechaultimobeneficioasignado"] = pd.to_datetime(df_golden["fechaultimobeneficioasignado"])
    df_golden["year"] = df_golden["fechaultimobeneficioasignado"].dt.year


    golden_grain = [col for col in grain if col != "fechaultimobeneficioasignado"]
    
    if "year" not in golden_grain:
        golden_grain.append("year")
    
    logging.info("Agrupando en Capa Golden por: %s", golden_grain)

    beneficiarios_iraca_golden = (
        df_golden.groupby(golden_grain, dropna=False, as_index=False)[metric_column]
        .sum()
        .rename(columns=rename_columns)
    )




    logging.info("Filas finales de eneficiarios_iraca_golden: %s", len(beneficiarios_iraca_golden))

    return beneficiarios_iraca_golden

# ============================================================
# GUARDADO
# ============================================================
"""
save_beneficiarios_iraca_fact(beneficiarios_iraca_fact, run_name, fact_dir, config)
"""

# ============================================================
# RESUMEN
# ============================================================
def log_summary(df: pd.DataFrame, beneficiarios_iraca_fact: pd.DataFrame) -> None:
    logging.info("Resumen de transformación:")
    logging.info("Filas finales en beneficiarios_iraca_fact: %s", len(beneficiarios_iraca_fact))

    if not beneficiarios_iraca_fact.empty:
        logging.info("Año mínimo en beneficiarios_iraca_fact: %s", beneficiarios_iraca_fact["date_iraca"].min())
        logging.info("Año máximo en beneficiarios_iraca_fact: %s", beneficiarios_iraca_fact["date_iraca"].max())


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    setup_logging(LOG_DIR, "beneficiarios_iraca_transform.log")
    logging.info("Iniciando transformación de iraca")

    config = load_transform_config("beneficiarios_iraca_transform", CONFIG_PATH)

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
    df = parse_fecha_corte (df, config)
    df = deduplicate_by_id(df, config)
    df =clean_text_data(df)

    beneficiarios_iraca_fact = build_beneficiarios_iraca_fact(df, config)
    beneficiarios_iraca_fact_golden = build_beneficiarios_iraca_fact_golden(df, config)
    
    save_fact_table(beneficiarios_iraca_fact, run_name, fact_dir, config, "beneficiarios_iraca")
    save_fact_table(beneficiarios_iraca_fact_golden, run_name, fact_dir_golden, config, "beneficiarios_iraca")
    log_summary(df, beneficiarios_iraca_fact)

    logging.info("Transformación finalizada correctamente")


if __name__ == "__main__":
    main()