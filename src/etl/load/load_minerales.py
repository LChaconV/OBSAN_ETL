from pathlib import Path
from src.etl.utils.load_utils import load_parquet_to_postgres

PROJECT_ROOT = Path(__file__).resolve().parents[3]
TRANSFORM_CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "minerales_transform.yaml"


create_table_sql = """
CREATE TABLE IF NOT EXISTS mineral_royalties(
    id_min_royal SERIAL PRIMARY KEY,
    year INTEGER,
    id_mun VARCHAR(10),
    mineral_resource VARCHAR(255),
    unit_measure VARCHAR(50),
    royalties_cop FLOAT,

    CONSTRAINT fk_divipola FOREIGN KEY (id_mun) REFERENCES dim_divipola(id_mun)
    );
"""

create_index_sql = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_mineral_royalties
ON mineral_royalties (year, mineral_resource, id_mun);
"""

if __name__ == "__main__":
    
    load_parquet_to_postgres(
        transform_config_path=TRANSFORM_CONFIG_PATH,
        config_key="minerales_transform",
        table_name="mineral_royalties",
        state_key="minerales_load",
        log_file_name="load_minerales.log",
        create_table_sql=create_table_sql,
        create_index_sql=create_index_sql,
        load_mode="upsert",
        conflict_columns=["year", "mineral_resource","id_mun"],
        update_columns=["royalties_cop"],
        state_field_name="last_incremental_value",
    )