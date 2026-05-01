from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

# Importación de utilidades del proyecto
from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.config_utils import load_yaml, load_state, update_state
from src.etl.utils.db_utils import get_engine

# ============================================================
# CONFIGURACIÓN DE RUTAS
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parents[3]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

STATE_DB_PATH = PROJECT_ROOT / "config" / "state_db.yaml"
SOURCES_CONFIG_PATH = PROJECT_ROOT / "config" / "sources.yaml"
LOG_DIR = PROJECT_ROOT / "logs"

# ============================================================
# FUNCIONES DE APOYO
# ============================================================
def get_latest_run_file(directory: Path) -> Path | None:
    """
    Escanea el directorio y retorna el archivo CSV con la fecha 
    de corrida más reciente según la nomenclatura estándar.
    """
    # Buscamos archivos que sigan el patrón mortalidad_desnutricion_run_*.csv
    files = list(directory.glob("mortalidad_desnutricion_run_*.csv"))
    
    if not files:
        return None
    
    # Al usar YYYY_MM_DD, el orden alfabético descendente nos da el más reciente
    files.sort(reverse=True)
    return files[0]

# ============================================================
# PROCESO DE CARGA (LOAD GOLD TO DB)
# ============================================================
def run() -> None:
    setup_logging(LOG_DIR, "load_mortalidad_desnutricion.log")
    logging.info("Iniciando proceso de carga para: fact_mortalidad")

    try:
        engine = get_engine()
        sources_config = load_yaml(SOURCES_CONFIG_PATH)
        
        # 1. Localización del directorio Gold
        gold_rel_path = sources_config["mortalidad_desnutricion"]["path_gold"]
        gold_dir = PROJECT_ROOT / gold_rel_path
        
        if not gold_dir.exists():
            logging.error("Directorio Gold no encontrado: %s", gold_dir)
            return

        # 2. Identificar el archivo de la última corrida
        latest_file = get_latest_run_file(gold_dir)
        
        if not latest_file:
            logging.warning("No se encontraron archivos de corrida en %s", gold_dir)
            return

        # 3. Verificar contra state_db.yaml si ya fue procesado
        db_state = load_state("mortalidad_desnutricion", STATE_DB_PATH)
        if db_state.get("last_loaded_file") == latest_file.name:
            logging.info("El archivo %s ya fue cargado anteriormente. Omitiendo proceso.", latest_file.name)
            return

        logging.info("Procesando archivo mas reciente: %s", latest_file.name)
        
        # 4. Lectura con tipado para evitar pérdida de ceros en códigos DANE
        df_source = pd.read_csv(latest_file, dtype={'id_muni': str})
        df_source['date_event'] = pd.to_datetime(df_source['date_event'])
        
        # Identificación de años para limpieza selectiva (Idempotencia)
        years_to_update = df_source['year'].unique().tolist()

        with engine.begin() as conn:
            # 5. Asegurar Infraestructura de la Tabla de Hechos
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS mortality_malnutrition (
                    date_event DATE,
                    year INTEGER,
                    age INTEGER,
                    id_muni VARCHAR(10),
                    total_cases INTEGER
                );
            """))

            # 6. Limpieza de particiones (Evita duplicados del mismo año)
            logging.info("Eliminando registros previos para los años: %s", years_to_update)
            conn.execute(
                text("DELETE FROM mortality_malnutrition WHERE year IN :years"),
                {"years": tuple(years_to_update)}
            )

            # 7. Inserción de datos Gold
            logging.info("Insertando %d registros en la base de datos.", len(df_source))
            df_source.to_sql("mortality_malnutrition", conn, if_exists="append", index=False)
            
            # 8. Actualización de estado de base de datos
            update_state(
                key="mortalidad_desnutricion",
                incremental_value=latest_file.name, # Guardamos el nombre como checkpoint
                incremental_column="run_filename",
                row_count=len(df_source),
                extraction_mode="db_load_gold_run",
                path_state=STATE_DB_PATH
            )
            logging.info("Carga finalizada exitosamente para el archivo: %s", latest_file.name)

    except Exception as e:
        logging.critical("Fallo crítico en la carga de mortalidad: %s", str(e), exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    run()