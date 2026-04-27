from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.config_utils import load_yaml
from src.etl.utils.transform_utils import ensure_five_digits, load_latest_silver_run, load_transform_config, get_latest_bronze_run,extract_run_name,clean_columns,clean_text_data,validate_required_columns,normalize_types,save_fact_table
from src.etl.utils.sivigila_transform_utils import load_latest_bronze_run
# ============================================================
# RUTAS DEL PROYECTO
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parents[3]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "mercado_laboral_transform.yaml"

LOG_DIR = PROJECT_ROOT / "logs"



def build_table(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    fact_cfg = config["fact_table"]
    grain = fact_cfg["grain"] 
    metric_column = fact_cfg["metric_column"]
    output_columns = fact_cfg["output_columns"] 

    df_temp = df.copy()
    df_temp= df_temp[output_columns]
    df_temp = ensure_five_digits(df_temp, "id_mun")
    df_temp = clean_text_data(df_temp )

    df_temp["total"] = (
        df_temp["total"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    
    df_temp["total"] = pd.to_numeric(df_temp["total"], errors="coerce")
    
    table_fact = (
        df_temp.groupby(grain, dropna=False, as_index=False)[metric_column]
        .sum()
    )
    

    table_fact = table_fact.sort_values(
        grain
    ).reset_index(drop=True)



    logging.info("Filas finales de table_fact: %s", len(table_fact))
    
    return table_fact

def delete_dep_like_mun(df) -> pd.DataFrame:
        # 1. Crear máscara (iguales PERO excluyendo Bogotá)
    mask = (
        (df["name_dept"] == df["name_mun"]) &
        (df["name_mun"] != "Bogotá")&
        (df["id_mun"] != "11001")
    )

    num_eliminados = mask.sum()
    valores_unicos = df.loc[mask, "name_dept"].unique()

    logging.info("Registros a eliminar:", num_eliminados)
    logging.info("Valores únicos donde coinciden:", valores_unicos)

    df = df[~mask]
    return df
    
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
def main() -> None:

    setup_logging(LOG_DIR, "mercado_laboral.log")
    logging.info("Iniciando transformación")

    config = load_transform_config("mercado_laboral_transform", CONFIG_PATH)

    bronze_dir = PROJECT_ROOT / config["source"]["bronze_dir"]
    fact_dir = PROJECT_ROOT / config["source"]["silver_fact_dir"]
    fact_dir_golden = PROJECT_ROOT / config["source"]["golden_fact_dir"]
    rename_columns = config["fact_table"]["rename_columns"]
    metric_column = config["fact_table"]["metric_column"]
    #run_dir = get_latest_bronze_run(bronze_dir)
    #run_name = extract_run_name(run_dir)

    df, run_name= load_latest_silver_run(bronze_dir,".xlsx")
    #df=pd.read_excel(bronze_dir, "xlsx")
    df = clean_columns(df)
    df= df.rename(columns=rename_columns)
    df["total"] = (
                    df["total"]
                    .astype(str)
                    .str.replace(".", "", regex=False)
                    .str.replace(",", ".", regex=False)
                    )   

    df = normalize_types(df, config)
    df = df.dropna(subset=["total"])
    df = delete_dep_like_mun(df)
    df = df[
    df["sub_categ"] == "Trabajadores cotizantes al sistema general de seguridad social según sexo"
]


    table_fact = build_table(df, config)


    # Golden


    save_fact_table(table_fact, run_name, fact_dir, config, "mercado_laboral")
    save_fact_table(table_fact, run_name, fact_dir_golden, config, "mercado_laboral")
    log_summary(df, table_fact)

    logging.info("Transformación finalizada correctamente")

if __name__ == "__main__":
    main()