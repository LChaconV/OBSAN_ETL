from pathlib import Path
from src.etl.utils.load_utils import load_parquet_to_postgres

PROJECT_ROOT = Path(__file__).resolve().parents[3]
TRANSFORM_CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "erradicacion_cultivos_transform.yaml"


create_table_sql = """
CREATE TABLE IF NOT EXISTS erad_illicit_crops(
    id_crop SERIAL PRIMARY KEY,
    id_illicit_crop VARCHAR(100),
    year INTEGER,
    id_mun VARCHAR(10),
    id_dept VARCHAR(10),
    quantity FLOAT
);
"""

create_index_sql = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_erad_illicit_crops
ON erad_illicit_crops (year, id_mun, id_dept, id_illicit_crop);
"""

def run():
    load_parquet_to_postgres(
        transform_config_path=TRANSFORM_CONFIG_PATH,
        config_key="erradicacion_cultivos_transform",
        table_name="erad_illicit_crops",
        state_key="erradicacion_cultivos_load",
        log_file_name="load_erradicacion_cultivos.log",
        create_table_sql=create_table_sql,
        create_index_sql=create_index_sql,
        load_mode="upsert",
        conflict_columns=["year", "id_mun", "id_dept", "id_illicit_crop"],
        update_columns=["quantity"],
        state_field_name="last_incremental_value",
    )
if __name__ == "__main__":
    run()