import logging
from pathlib import Path
from src.etl.utils.load_utils import load_parquet_to_postgres
from src.etl.utils.db_utils import get_engine
from sqlalchemy import text
PROJECT_ROOT = Path(__file__).resolve().parents[3]
TRANSFORM_CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "edu_escolar_transform.yaml"
VIEW_SQL_PATH = PROJECT_ROOT / "sql" / "views" / "v_school_education_pc.sql"

create_table_sql = """
CREATE TABLE IF NOT EXISTS school_education(
    id_school_education SERIAL PRIMARY KEY,
    year INTEGER,
    id_mun VARCHAR (10),
    total INTEGER,
   
    CONSTRAINT fk_municipio 
        FOREIGN KEY (id_mun) 
        REFERENCES dim_divipola(id_mun)

        );
"""

create_index_sql = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_school_education
ON school_education (year, id_mun); 
"""

def run():
    
    load_parquet_to_postgres(
        transform_config_path=TRANSFORM_CONFIG_PATH,
        config_key="edu_escolar_transform",
        table_name="school_education",
        state_key="edu_escolar_load",
        log_file_name="load_edu_escolar.log",
        create_table_sql=create_table_sql,
        create_index_sql=create_index_sql,
        load_mode="upsert",
        conflict_columns=["year", "id_mun"],
        update_columns=["total"],
        state_field_name="last_incremental_value",
    )
    sql = VIEW_SQL_PATH.read_text(encoding="utf-8")
    with get_engine().begin() as conn:
        conn.execute(text(sql))
        exists = conn.execute(text(
            "SELECT 1 FROM information_schema.views WHERE table_name = 'v_school_education_pc'"
        )).fetchone()
    if exists:
        logging.info("Vista v_school_education_pc creada/actualizada correctamente")
    else:
        logging.warning("Vista v_school_education_pc NO fue creada — la tabla 'population' probablemente no existe en la BD")
if __name__ == "__main__":
    run()