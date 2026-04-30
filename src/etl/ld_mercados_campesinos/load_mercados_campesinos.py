from pathlib import Path
from src.etl.utils.load_utils import load_parquet_to_postgres
PROJECT_ROOT = Path(__file__).resolve().parents[3]
TRANSFORM_CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "mercados_campesinos_transform.yaml"

create_table_sql = """
CREATE TABLE IF NOT EXISTS farmer_market (
    id_market SERIAL PRIMARY KEY,
    name VARCHAR(500),
    geometry GEOMETRY

);
"""


if __name__ == "__main__":
    load_parquet_to_postgres(
        transform_config_path=TRANSFORM_CONFIG_PATH,
        config_key="mercados_campesinos_transform",
        table_name="farmer_market",
        state_key="mercados_campesinos_load",
        log_file_name="load_mercados_campesinos.log",
        create_table_sql=create_table_sql,
        create_index_sql=None,
        load_mode="append",
        conflict_columns= None,
        update_columns= None,
        state_field_name="last_incremental_value",
    )