from pathlib import Path
from src.etl.utils.load_utils import load_parquet_to_postgres
PROJECT_ROOT = Path(__file__).resolve().parents[3]
TRANSFORM_CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "mun_pdet_transform.yaml"

create_table_sql = """
CREATE TABLE IF NOT EXISTS dim_mun_pdet (
    id_mun_dept SERIAL PRIMARY KEY,
    id_mun VARCHAR(10),

    CONSTRAINT fk_divipola FOREIGN KEY (id_mun) REFERENCES dim_divipola(id_mun)
);
"""


if __name__ == "__main__":
    load_parquet_to_postgres(
        transform_config_path=TRANSFORM_CONFIG_PATH,
        config_key="mun_pdet_transform",
        table_name="dim_mun_pdet",
        state_key="mun_pdet_load",
        log_file_name="load_mun_pdet.log",
        create_table_sql=create_table_sql,
        create_index_sql=None,
        load_mode="append",
        conflict_columns= None,
        update_columns= None,
        state_field_name="last_incremental_value",
    )