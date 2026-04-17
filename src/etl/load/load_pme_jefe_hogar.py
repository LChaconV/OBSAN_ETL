from pathlib import Path
from src.etl.utils.load_utils import load_parquet_to_postgres
PROJECT_ROOT = Path(__file__).resolve().parents[3]
TRANSFORM_CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "pme_jefe_hogar_transform.yaml"

create_table_sql = """
CREATE TABLE IF NOT EXISTS mp_sex_head_hh (
    id_mp_hh SERIAL PRIMARY KEY,
    year INTEGER,
    id_dept VARCHAR(10),
    id_gender INTEGER,
    mp_idx_val DECIMAL(10,2),
    CONSTRAINT fk_gender FOREIGN KEY (id_gender) REFERENCES dim_gender(id_gender)
);
"""

create_index_sql = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_mp_hh
ON mp_sex_head_hh (year, id_dept, id_gender);
"""

if __name__ == "__main__":
    load_parquet_to_postgres(
        transform_config_path=TRANSFORM_CONFIG_PATH,
        config_key="pme_jefe_hogar_transform",
        table_name="mp_sex_head_hh",
        state_key="pme_jefe_hogar_load",
        log_file_name="load_pme_jefe_hogar.log",
        create_table_sql=create_table_sql,
        create_index_sql=create_index_sql,
        load_mode="upsert",
        conflict_columns=["year", "id_dept", "id_gender"],
        update_columns=["mp_idx_val"],
        state_field_name="last_incremental_value",
    )