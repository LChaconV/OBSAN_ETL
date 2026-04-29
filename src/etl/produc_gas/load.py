from pathlib import Path
from src.etl.utils.load_utils import load_parquet_to_postgres

PROJECT_ROOT = Path(__file__).resolve().parents[3]
TRANSFORM_CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "produc_gas_transform.yaml"


create_table_sql = """
CREATE TABLE IF NOT EXISTS gas_production(
    id_gas_prod SERIAL PRIMARY KEY,
    year INTEGER,
    produc_kpc FLOAT,
    geometry GEOMETRY
    );
"""

create_index_sql = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_gas_production
ON gas_production (year, geometry );
"""


def run():
    load_parquet_to_postgres(
        transform_config_path=TRANSFORM_CONFIG_PATH,
        config_key="produc_gas_transform",
        table_name="gas_production",
        state_key="produc_gas_load",
        log_file_name="load_produc_gas.log",
        create_table_sql=create_table_sql,
        create_index_sql=create_index_sql,
        load_mode="upsert",
        conflict_columns=["year", "geometry"],
        update_columns=["produc_kpc"],
        state_field_name="last_incremental_value",
    )
if __name__ == "__main__":
    run()