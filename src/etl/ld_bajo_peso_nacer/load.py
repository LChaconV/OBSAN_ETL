from pathlib import Path

from src.etl.utils.load_utils import load_parquet_to_postgres

PROJECT_ROOT = Path(__file__).resolve().parents[3]

TRANSFORM_CONFIG_PATH = (
    PROJECT_ROOT
    / "config"
    / "transform"
    / "bajo_peso_nacer_transform.yaml"
)

create_table_sql = """
CREATE TABLE IF NOT EXISTS low_birth_weight (

    id_low_birth_weight SERIAL PRIMARY KEY,

    date_event DATE,

    year INTEGER,

    total_cases INTEGER,

    id_mun VARCHAR(10),

    CONSTRAINT fk_divipola
        FOREIGN KEY (id_mun)
        REFERENCES dim_divipola(id_mun)

);
"""

create_index_sql = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_low_birth_weight
ON low_birth_weight (
    year,
    id_mun
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
            "year",
            "id_mun"
        ],

        update_columns=[
            "total_cases",
            "date_event"
        ],

        state_field_name="last_incremental_value",
    )

if __name__ == "__main__":
    run()