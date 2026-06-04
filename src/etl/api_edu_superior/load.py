import logging
from pathlib import Path
from src.etl.utils.db_utils import get_engine
from src.etl.utils.load_utils import load_parquet_to_postgres
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[3]
TRANSFORM_CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "edu_superior_transform.yaml"
VIEW_SQL_PATH = PROJECT_ROOT / "sql" / "views" / "v_higher_education_pc.sql"


create_table_sql = """
CREATE TABLE IF NOT EXISTS higher_education(
    id_higher_education SERIAL PRIMARY KEY,
    year INTEGER,
    id_mun VARCHAR (10),
    prof_technician INTEGER,
    technologist INTEGER,
    university INTEGER,
    specialization INTEGER,
    master INTEGER,
    doctorate INTEGER,

    
    CONSTRAINT fk_municipio 
        FOREIGN KEY (id_mun) 
        REFERENCES dim_divipola(id_mun)

        );
"""

create_index_sql = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_higher_education
ON higher_education (year, id_mun); 
"""

def run(**kwargs):
    
    load_parquet_to_postgres(
        transform_config_path=TRANSFORM_CONFIG_PATH,
        config_key="edu_superior_transform",
        table_name="higher_education",
        state_key="edu_superior_load",
        log_file_name="load_edu_superior.log",
        create_table_sql=create_table_sql,
        create_index_sql=create_index_sql,
        load_mode="upsert",
        conflict_columns=["year", "id_mun"],
        update_columns=[
            "prof_technician",
            "technologist",
            "university",
            "specialization",
            "master",
            "doctorate"
        ],
        state_field_name="last_incremental_value",
    )
    sql = VIEW_SQL_PATH.read_text(encoding="utf-8")
    with get_engine().begin() as conn:
        conn.execute(text(sql))
        exists = conn.execute(text(
            "SELECT 1 FROM information_schema.views WHERE table_name = 'v_higher_education_pc'"
        )).fetchone()
    if exists:
        logging.info("Vista v_higher_education_pc creada/actualizada correctamente")
    else:
        logging.warning("Vista v_higher_education_pc NO fue creada — la tabla 'population' probablemente no existe en la BD")

if __name__ == "__main__":
    run()