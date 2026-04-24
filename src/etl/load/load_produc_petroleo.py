from pathlib import Path
from src.etl.utils.load_utils import load_parquet_to_postgres

PROJECT_ROOT = Path(__file__).resolve().parents[3]
TRANSFORM_CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "produc_petroleo_transform.yaml"


create_table_sql = """
CREATE TABLE IF NOT EXISTS oil_production(
    id_oil_prod SERIAL PRIMARY KEY,
    year INTEGER,
    produc_bls FLOAT,
    geometry GEOMETRY
    );
"""

create_index_sql = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_oil_production
ON oil_production (year, geometry );
"""

if __name__ == "__main__":
    
    load_parquet_to_postgres(
        transform_config_path=TRANSFORM_CONFIG_PATH,
        config_key="produc_petroleo_transform",
        table_name="oil_production",
        state_key="produc_petroleo_load",
        log_file_name="load_produc_petroleo.log",
        create_table_sql=create_table_sql,
        create_index_sql=create_index_sql,
        load_mode="upsert",
        conflict_columns=["year", "geometry"],
        update_columns=["produc_bls"],
        state_field_name="last_incremental_value",
    )