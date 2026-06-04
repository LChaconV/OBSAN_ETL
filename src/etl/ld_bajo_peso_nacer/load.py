import logging
from pathlib import Path

from sqlalchemy import text

from src.etl.utils.load_utils import load_parquet_to_postgres
from src.etl.utils.db_utils import get_engine

PROJECT_ROOT = Path(__file__).resolve().parents[3]

TRANSFORM_CONFIG_PATH = (
    PROJECT_ROOT
    / "config"
    / "transform"
    / "bajo_peso_nacer_transform.yaml"
)

VIEW_SQL_PATH = PROJECT_ROOT / "sql" / "views" / "v_low_birth_weight.sql"

create_table_sql = """
CREATE TABLE IF NOT EXISTS low_birth_weight (

    id_consecutive varchar(50) PRIMARY KEY,
    date_event DATE,
    year INTEGER,
    confirmed INTEGER,
    id_mun VARCHAR(10),
    id_country VARCHAR(10),
    id_dept VARCHAR(10),
    CONSTRAINT fk_divipola
        FOREIGN KEY (id_mun)
        REFERENCES dim_divipola(id_mun)

);
"""

create_index_sql = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_low_birth_weight
ON low_birth_weight (
    year,
    id_mun,
    date_event,
    id_consecutive
);
"""

def run(**kwargs):

    load_parquet_to_postgres(

        transform_config_path=TRANSFORM_CONFIG_PATH,

        config_key="bajo_peso_nacer_transform",

        table_name="low_birth_weight",

        state_key="bajo_peso_nacer_load",

        log_file_name="bajo_peso_nacer.log",

        create_table_sql=create_table_sql,

        create_index_sql=create_index_sql,

        load_mode="upsert",

        conflict_columns=[
            "year","id_mun", "date_event","id_consecutive"
        ],

        update_columns=[
            "confirmed",
            "id_country",
            "id_dept"
        ],

        state_field_name="last_incremental_value",
    )

    sql = VIEW_SQL_PATH.read_text(encoding="utf-8")
    with get_engine().begin() as conn:
        conn.execute(text(sql))
    logging.info("Vista v_low_birth_weight_pc actualizada")

if __name__ == "__main__":
    run()