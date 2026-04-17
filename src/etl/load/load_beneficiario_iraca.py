from pathlib import Path
from src.etl.utils.load_utils import load_parquet_to_postgres
PROJECT_ROOT = Path(__file__).resolve().parents[3]
TRANSFORM_CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "beneficiarios_iraca_transform.yaml"

create_table_sql = """
CREATE TABLE IF NOT EXISTS iraca_beneficiaries(
    id_beneficiary SERIAL PRIMARY KEY,
    year    INTEGER,
    id_mun  VARCHAR(10),
    status  VARCHAR(100),
    type    VARCHAR(100),
    beneficiaries_count INTEGER,

    CONSTRAINT fk_municipio 
        FOREIGN KEY (id_mun) 
        REFERENCES dim_divipola(id_mun),

    CONSTRAINT unique_iraca_beneficiary
        UNIQUE (year, id_mun, type, status)
);
"""


if __name__ == "__main__":
    load_parquet_to_postgres(
        transform_config_path=TRANSFORM_CONFIG_PATH,
        config_key="beneficiarios_iraca_transform",
        table_name="iraca_beneficiaries",
        state_key="beneficiarios_iraca_load",
        log_file_name="load_beneficiarios_iraca.log",
        create_table_sql=create_table_sql,
        load_mode="upsert",
        conflict_columns=["year",  "id_mun", "type", "status"],
        update_columns=["beneficiaries_count"],
        state_field_name="last_incremental_value",
    )