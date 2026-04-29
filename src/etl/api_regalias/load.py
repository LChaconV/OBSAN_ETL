from pathlib import Path
from src.etl.utils.load_utils import load_parquet_to_postgres

PROJECT_ROOT = Path(__file__).resolve().parents[3]
TRANSFORM_CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "regalias_transform.yaml"


create_table_sql = """
CREATE TABLE IF NOT EXISTS royalties(
    id_royalties SERIAL PRIMARY KEY,
    year INTEGER,
    royalties_cop FLOAT,
    geometry GEOMETRY
    );
"""

create_index_sql = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_royalties
ON royalties (year, geometry );
"""

if __name__ == "__main__":
    
    load_parquet_to_postgres(
        transform_config_path=TRANSFORM_CONFIG_PATH,
        config_key="regalias_transform",
        table_name="royalties",
        state_key="regalias_load",
        log_file_name="load_regalias.log",
        create_table_sql=create_table_sql,
        create_index_sql=create_index_sql,
        load_mode="upsert",
        conflict_columns=["year", "geometry"],
        update_columns=["royalties_cop"],
        state_field_name="last_incremental_value",
    )