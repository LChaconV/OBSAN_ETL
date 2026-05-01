from pathlib import Path
from src.etl.utils.load_utils import load_parquet_to_postgres

PROJECT_ROOT = Path(__file__).resolve().parents[3]
TRANSFORM_CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "mercado_laboral_transform.yaml"


create_table_sql = """
CREATE TABLE IF NOT EXISTS employed_population(
    id_employed_population SERIAL PRIMARY KEY,
    year INTEGER,
    id_mun VARCHAR (10),
    total INTEGER,
   
    CONSTRAINT fk_municipio 
        FOREIGN KEY (id_mun) 
        REFERENCES dim_divipola(id_mun)

        );
"""

create_index_sql = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_employed_population
ON employed_population (year, id_mun); 
"""

def run() -> None:
    
    load_parquet_to_postgres(
        transform_config_path=TRANSFORM_CONFIG_PATH,
        config_key="mercado_laboral_transform",
        table_name="employed_population",
        state_key="mercado_laboral_load",
        log_file_name="load_mercado_laboral.log",
        create_table_sql=create_table_sql,
        create_index_sql=create_index_sql,
        load_mode="upsert",
        conflict_columns=["year", "id_mun"],
        update_columns=["total"],
        state_field_name="last_incremental_value",
    )
if __name__ == "__main__":
    run()