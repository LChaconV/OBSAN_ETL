import os
import logging
import pandas as pd
from pathlib import Path
from src.etl.utils.load_utils import load_parquet_to_postgres
from src.etl.utils.config_utils import load_yaml

# Configuración de rutas
PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "censo_pecuario_transform.yaml"

# SQL Definitions
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS census_livestock (
    id_census_livestock SERIAL PRIMARY KEY,
    id_mun VARCHAR(10),
    year INTEGER,
    type VARCHAR(50),
    total_animals NUMERIC,
    total_farms NUMERIC,

    CONSTRAINT fk_divipola
        FOREIGN KEY (id_mun)
        REFERENCES dim_divipola(id_mun)

);
"""

CREATE_INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_census_livestock
ON census_livestock (id_mun, year, type);
"""

def run() -> None:
    logger = logging.getLogger(__name__)
    
    key = os.environ.get("OBSAN_ANIMAL_TYPE")
    if not key:
        raise EnvironmentError("Falta la variable de entorno OBSAN_ANIMAL_TYPE")


    full_config = load_yaml(CONFIG_PATH)
    
    try:
       
    
        relative_output_path = Path(full_config[key]["source"]["golden_fact_dir"]) / full_config[key]["source"]["last_file_transformed"]
        
    except KeyError:
        raise KeyError(f"No se encontró la llave '{key}' en el YAML simplificado.")

    if not relative_output_path or relative_output_path == "none":
        logger.warning(f"No hay archivos para procesar en: {key}")
        return

    input_file_path = PROJECT_ROOT / relative_output_path

    # Ejecución de la carga
    load_parquet_to_postgres(
        transform_config_path=CONFIG_PATH,
        config_key=key,  # Ahora sí encontrará full_config["aves"]
        table_name="census_livestock",
        state_key="censo_load",
        log_file_name=f"load_censo_{key}.log",
        create_table_sql=CREATE_TABLE_SQL,
        create_index_sql=CREATE_INDEX_SQL,
        load_mode="upsert",
        conflict_columns=["id_mun", "year", "type"],
        update_columns=["total_animals", "total_farms"],
        state_field_name="last_incremental_value",
        input_file_path=input_file_path,
    )

if __name__ == "__main__":
    run()