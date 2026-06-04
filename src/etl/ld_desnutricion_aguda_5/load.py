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
    / "desnutricion_aguda_5_transform.yaml"
)

VIEW_SQL_PATH = PROJECT_ROOT / "sql" / "views" / "v_acute_malnutrition_5_pc.sql"

create_table_sql = """
CREATE TABLE IF NOT EXISTS acute_malnutrition_5 (

    id_consecutive VARCHAR(50) PRIMARY KEY,
    confirmed      INTEGER,
    condition_end  VARCHAR(100),
    date_event     DATE,
    year           INTEGER,
    age            INTEGER,
    id_mun         VARCHAR(10),
    CONSTRAINT fk_acute5_divipola
        FOREIGN KEY (id_mun)
        REFERENCES dim_divipola(id_mun)

);
"""

def run(**kwargs):

    load_parquet_to_postgres(

        transform_config_path=TRANSFORM_CONFIG_PATH,

        config_key="desnutricion_aguda_5_transform",

        table_name="acute_malnutrition_5",

        state_key="desnutricion_aguda_5_load",

        log_file_name="load_desnutricion_aguda.log",

        create_table_sql=create_table_sql,

        load_mode="upsert",

        conflict_columns=["id_consecutive"],

        insert_columns=[
            "id_consecutive", "confirmed", "condition_end",
            "date_event", "year", "age", "id_mun"
        ],

        update_columns=[
            "confirmed", "condition_end", "date_event", "year", "age", "id_mun"
        ],

        state_field_name="last_incremental_value",
    )

    sql = VIEW_SQL_PATH.read_text(encoding="utf-8")
    with get_engine().begin() as conn:
        conn.execute(text(sql))
        exists = conn.execute(text(
            "SELECT 1 FROM information_schema.views WHERE table_name = 'v_acute_malnutrition_5_pc'"
        )).fetchone()
    if exists:
        logging.info("Vista v_acute_malnutrition_5_pc creada/actualizada correctamente")
    else:
        logging.warning("Vista v_acute_malnutrition_5_pc NO fue creada — la tabla 'population' probablemente no existe en la BD")

if __name__ == "__main__":
    run()
