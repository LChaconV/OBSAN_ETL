from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.config_utils import load_yaml, load_state, update_state
from src.etl.utils.db_utils import get_engine

# ============================================================
# RUTAS Y CONFIGURACIÓN
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

STATE_DB_PATH = PROJECT_ROOT / "config" / "state_db.yaml"
TRANSFORM_CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "pme_jefe_hogar_transform.yaml"
LOG_DIR = PROJECT_ROOT / "logs"

# ============================================================
# LÓGICA DE CARGA
# ============================================================
def get_latest_silver_file(golden_dir: Path, file_prefix: str) -> Path | None:
    files = list(golden_dir.glob(f"{file_prefix}_run_*.parquet"))
    if not files:
        return None
    return sorted(files)[-1]

def main() -> None:
    setup_logging(LOG_DIR, "load_pme_jefe_hogar.log")
    logging.info("Iniciando carga de PME Jefe de Hogar con lógica UPSERT")

    try:
        engine = get_engine()
        config = load_yaml(TRANSFORM_CONFIG_PATH)["pme_jefe_hogar_transform"]

        golden_dir = PROJECT_ROOT / config["source"]["golden_fact_dir"]
        file_prefix = config["fact_table"]["file_prefix"]
        table_name = "mp_sex_head_hh"
        temp_table = f"temp_{table_name}"

        # 1. Identificar archivo
        latest_file = get_latest_silver_file(golden_dir, file_prefix)
        if not latest_file:
            logging.warning("No se encontraron archivos en la ruta golden: %s", golden_dir)
            return

        # 2. Control de Estado (Portero Lógico)
        db_state = load_state("pme_jefe_hogar_load", STATE_DB_PATH)
        if db_state.get("last_incremental_value") == latest_file.name:
            logging.info("El archivo %s ya fue cargado según el estado. Omitiendo.", latest_file.name)
            return

        # 3. Lectura de datos
        df_silver = pd.read_parquet(latest_file)

        # 4. Definición del Esquema (DDL)
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id_mp_hh SERIAL PRIMARY KEY,
            year INTEGER,
            id_dept VARCHAR(10),
            id_gender INTEGER,
            mp_idx_val DECIMAL(10,2),
            CONSTRAINT fk_gender FOREIGN KEY (id_gender) REFERENCES dim_gender(id_gender)
        );
        """
        
        create_index_query = f"""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_mp_hh 
        ON {table_name} (year, id_dept, id_gender);
        """

        # 5. Ejecución de Carga (Lógica UPSERT vía Tabla Temporal)
        with engine.begin() as conn:
            # A. Preparar la tabla real e índice
            conn.execute(text(create_table_query))
            conn.execute(text(create_index_query))
            
            # B. Cargar datos a una tabla temporal (sin restricciones)
            logging.info("Subiendo datos a tabla temporal...")
            df_silver.to_sql(name=temp_table, con=conn, if_exists="replace", index=False)

            # C. Mover datos de Temp a Real ignorando duplicados (ON CONFLICT DO NOTHING)
            # Nota: id_mp_hh se genera solo en la tabla real
            upsert_query = f"""
            INSERT INTO {table_name} (year, id_dept, id_gender, mp_idx_val)
            SELECT year, id_dept, id_gender, mp_idx_val 
            FROM {temp_table}
            ON CONFLICT (year, id_dept, id_gender) 
            DO UPDATE SET 
                mp_idx_val = EXCLUDED.mp_idx_val;
            """
            
            logging.info("Ejecutando UPSERT en la tabla principal...")
            result = conn.execute(text(upsert_query))
            
            # D. Limpiar tabla temporal
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_table};"))

            # 6. Actualizar estado
            update_state(
                key="pme_jefe_hogar_load",
                incremental_value=latest_file.name,
                incremental_column="file_name",
                row_count=len(df_silver),
                extraction_mode="golden_load",
                path_state=STATE_DB_PATH
            )
            
        logging.info("Carga exitosa con protección de duplicados: %s", latest_file.name)

    except Exception as e:
        logging.critical("Fallo en la carga Golden: %s", e, exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()