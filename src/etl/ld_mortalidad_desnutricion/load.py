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
    / "mortalidad_desnutricion_transform.yaml"
)

VIEW_SQL_PATH = PROJECT_ROOT / "sql" / "views" / "v_mortality_malnutrition_pc.sql"

create_table_sql = """
CREATE TABLE IF NOT EXISTS mortality_malnutrition (

    id_consecutive VARCHAR(50) PRIMARY KEY,
    date_event     DATE,
    year           INTEGER,
    age            INTEGER,
    id_mun         VARCHAR(10),
    CONSTRAINT fk_mortality_divipola
        FOREIGN KEY (id_mun)
        REFERENCES dim_divipola(id_mun)

);
"""

create_index_sql = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_mortality_malnutrition
ON mortality_malnutrition (id_consecutive);
"""

def run(**kwargs):

    load_parquet_to_postgres(

        transform_config_path=TRANSFORM_CONFIG_PATH,

        config_key="mortalidad_desnutricion_transform",

        table_name="mortality_malnutrition",

        state_key="mortalidad_desnutricion_load",

        log_file_name="mortalidad_desnutricion.log",

        create_table_sql=create_table_sql,

        create_index_sql=create_index_sql,

        load_mode="upsert",

        conflict_columns=["id_consecutive"],

        insert_columns=["id_consecutive", "date_event", "year", "age", "id_mun"],

        update_columns=["date_event", "year", "age", "id_mun"],

        state_field_name="last_incremental_value",
    )

    sql = VIEW_SQL_PATH.read_text(encoding="utf-8")
    with get_engine().begin() as conn:
        conn.execute(text(sql))
        exists = conn.execute(text(
            "SELECT 1 FROM information_schema.views WHERE table_name = 'v_mortality_malnutrition_pc'"
        )).fetchone()
    if exists:
        logging.info("Vista v_mortality_malnutrition_pc creada/actualizada correctamente")
    else:
        logging.warning("Vista v_mortality_malnutrition_pc NO fue creada — la tabla 'population' probablemente no existe en la BD")

if __name__ == "__main__":
    run()
