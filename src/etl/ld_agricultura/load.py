import os
import logging
import pandas as pd
from pathlib import Path
from src.etl.utils.load_utils import load_parquet_to_postgres
from src.etl.utils.config_utils import load_yaml

# Configuración de rutas
PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "agricola_transform.yaml"

# SQL Definitions
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS agricultural_production (
    agricultural_production SERIAL PRIMARY KEY,
    id_mun VARCHAR(10),
    year INTEGER,
    type VARCHAR(100),
    area_sown FLOAT,
    area_harvested FLOAT,   
    production FLOAT,
    yield FLOAT,


    CONSTRAINT fk_divipola
        FOREIGN KEY (id_mun)
        REFERENCES dim_divipola(id_mun)

);
"""

CREATE_INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_agricultural_production
ON agricultural_production (id_mun, year, type);
"""

def run() -> None:
    logger = logging.getLogger(__name__)
    

    full_config = load_yaml(CONFIG_PATH)
    
    try:
       
    
        relative_output_path = Path(full_config["agricola_transform"]["source"]["golden_fact_dir"]) / full_config["agricola_transform"]["source"]["last_file_transformed"]
        
    except KeyError:
        raise KeyError(f"No se encontró la llave agricola_transform en el YAML simplificado.")

    if not relative_output_path or relative_output_path == "none":
        logger.warning(f"No hay archivos para procesar en: agricola_transform")
        return

    input_file_path = PROJECT_ROOT / relative_output_path


    load_parquet_to_postgres(
        transform_config_path=CONFIG_PATH,
        config_key="agricola_transform", 
        table_name="agricultural_production",
        state_key="agricultural_load",
        log_file_name=f"load_agricultural_agricola_transform.log",
        create_table_sql=CREATE_TABLE_SQL,
        create_index_sql=CREATE_INDEX_SQL,
        load_mode="upsert",
        conflict_columns=["id_mun", "year", "type"],
        update_columns=["area_sown", "area_harvested", "production", "yield"],
        state_field_name="last_incremental_value",
        input_file_path=input_file_path,
    )

if __name__ == "__main__":
    run()