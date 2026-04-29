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
TRANSFORM_CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "victimas_transform.yaml"
LOG_DIR = PROJECT_ROOT / "logs"

# ============================================================
# LÓGICA DE CARGA
# ============================================================
def get_latest_silver_file(golden_dir: Path, file_prefix: str) -> Path | None:
    """Identifica el último archivo Parquet generado por la transformación Silver."""
    files = list(golden_dir.glob(f"{file_prefix}_run_*.parquet"))
    if not files:
        return None
    return sorted(files)[-1]

def run() -> None:
    setup_logging(LOG_DIR, "load_victimas_silver.log")
    logging.info("Iniciando carga de Victimas Capa Silver (victim_unit) a PostgreSQL")

    try:
        engine = get_engine()
        config = load_yaml(TRANSFORM_CONFIG_PATH)["victimas_transform"]
        
        golden_dir = PROJECT_ROOT / config["source"]["golden_fact_dir"]
        file_prefix = config["fact_table"]["file_prefix"]
        table_name = "victim_unit"

        # 1. Identificar archivo
        latest_file = get_latest_silver_file(golden_dir, file_prefix)
        if not latest_file:
            logging.warning("No se encontraron archivos en la ruta Silver: %s", golden_dir)
            return

        # 2. Control de Estado
        db_state = load_state("victimas_silver_load", STATE_DB_PATH)
        if db_state.get("last_loaded_file") == latest_file.name:
            logging.info("El archivo %s ya fue cargado. Omitiendo.", latest_file.name)
            return

        # 3. Lectura de datos
        df_silver = pd.read_parquet(latest_file)

        # 4. Definición del Esquema (DDL)
      
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id_victim SERIAL PRIMARY KEY,
            year INTEGER,
            id_mun VARCHAR(10),
            id_victim_event INTEGER,
            sexo VARCHAR(20),
            victim_count INTEGER,

            CONSTRAINT fk_event_type 
                FOREIGN KEY (id_victim_event) 
                REFERENCES dim_victim_event(id_victim_event)
        );
        """

        # 5. Ejecución de Carga
        with engine.begin() as conn:
            logging.info("Verificando esquema de tabla %s", table_name)
            conn.execute(text(create_table_query))
            
            logging.info("Insertando %d registros en %s", len(df_silver), table_name)
            
            df_silver.to_sql(
                name=table_name,
                con=conn,
                if_exists="append",
                index=False
            )

            # 6. Actualizar estado
            update_state(
                key="victimas_golden_load",
                incremental_value=latest_file.name,
                incremental_column="file_name",
                row_count=len(df_silver),
                extraction_mode="golden_load",
                path_state=STATE_DB_PATH
            )
            
        logging.info("Carga exitosa de la capa Golden: %s", latest_file.name)

    except Exception as e:
        logging.critical("Fallo en la carga Golden: %s", e, exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    run()