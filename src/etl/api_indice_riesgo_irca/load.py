from pathlib import Path
from src.etl.utils.load_utils import load_parquet_to_postgres

PROJECT_ROOT = Path(__file__).resolve().parents[3]
TRANSFORM_CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "irca_transform.yaml"


create_table_sql = """
CREATE TABLE IF NOT EXISTS water_quality_index(
    id_irca_fact INT PRIMARY KEY,
    year INTEGER,
    id_mun VARCHAR(10),
    id_dept VARCHAR(10),
    irca_value FLOAT,

    CONSTRAINT fk_municipio 
    FOREIGN KEY (id_mun) 
    REFERENCES dim_divipola(id_mun)
    
);
"""

create_index_sql = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_water_quality_index
ON water_quality_index (year, id_mun, id_dept);
"""

def run():
    load_parquet_to_postgres(
        transform_config_path=TRANSFORM_CONFIG_PATH,
        config_key="irca_transform",
        table_name="water_quality_index",
        state_key="irca_load",
        log_file_name="load_irca.log",
        create_table_sql=create_table_sql,
        create_index_sql=create_index_sql,
        load_mode="upsert",
        conflict_columns=["year", "id_mun", "id_dept"],
        update_columns=["irca_value"],
        state_field_name="last_incremental_value",
    )
if __name__ == "__main__":
    run()