from pathlib import Path
from src.etl.utils.load_utils import load_parquet_to_postgres
PROJECT_ROOT = Path(__file__).resolve().parents[3]
TRANSFORM_CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "resguardo_indigena_transform.yaml"

create_table_sql = """
CREATE TABLE IF NOT EXISTS dim_indigenous_reserve (
    id_indigenous SERIAL PRIMARY KEY,
    indigenous VARCHAR(500),
    id_mun VARCHAR(100),
    geometry GEOMETRY

);
"""


if __name__ == "__main__":
    load_parquet_to_postgres(
        transform_config_path=TRANSFORM_CONFIG_PATH,
        config_key="resguardo_indigena_transform",
        table_name="dim_indigenous_reserve",
        state_key="resguardo_indigena_load",
        log_file_name="load_resguardo_indigena.log",
        create_table_sql=create_table_sql,
        create_index_sql=None,
        load_mode="append",
        conflict_columns= None,
        update_columns= None,
        state_field_name="last_incremental_value",
    )