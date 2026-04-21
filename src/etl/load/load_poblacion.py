from pathlib import Path
from src.etl.utils.load_utils import load_parquet_to_postgres
PROJECT_ROOT = Path(__file__).resolve().parents[3]
TRANSFORM_CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "poblacion_transform.yaml"

create_table_sql = """
CREATE TABLE IF NOT EXISTS population (
    id_population SERIAL PRIMARY KEY,
    year INTEGER,
    id_mun VARCHAR(10),
    population DECIMAL(50,0),
    CONSTRAINT fk_divipola FOREIGN KEY (id_mun) REFERENCES dim_divipola(id_mun)
);
"""

create_index_sql = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_population
ON population (year, id_mun);
"""

if __name__ == "__main__":
    load_parquet_to_postgres(
        transform_config_path=TRANSFORM_CONFIG_PATH,
        config_key="poblacion_transform",
        table_name="population",
        state_key="poblacion_load",
        log_file_name="load_poblacion.log",
        create_table_sql=create_table_sql,
        create_index_sql=create_index_sql,
        load_mode="upsert",
        conflict_columns=["year", "id_mun"],
        update_columns=["population"],
        state_field_name="last_incremental_value",
    )