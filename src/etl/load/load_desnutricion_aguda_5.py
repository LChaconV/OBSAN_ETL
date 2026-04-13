from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

# Importación de utilidades modulares
from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.config_utils import load_yaml, load_state, update_state
from src.etl.utils.db_utils import get_engine

# ============================================================
# CONFIGURACIÓN DE RUTAS Y CONSTANTES
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parents[3]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

STATE_DB_PATH = PROJECT_ROOT / "config" / "state_db.yaml"
SOURCES_CONFIG_PATH = PROJECT_ROOT / "config" / "sources.yaml"
LOG_DIR = PROJECT_ROOT / "logs"

# ============================================================
# UTILIDADES DE BÚSQUEDA
# ============================================================
def get_latest_run_file(directory: Path) -> Path | None:
    """
    Identifica el CSV de la corrida más reciente para desnutrición aguda.
    """
    files = list(directory.glob("desnutricion_aguda_5_run_*.csv"))
    if not files:
        return None
    
    # Ordenar descendente para obtener la fecha de corrida más alta (YYYY_MM_DD)
    files.sort(reverse=True)
    return files[0]

# ============================================================
# PROCESO DE CARGA (LOAD GOLD TO DB)
# ============================================================
def main() -> None:
    setup_logging(LOG_DIR, "load_desnutricion_aguda.log")
    logging.info("Iniciando carga formal para la tabla: acute_malnutrition_5")

    try:
        engine = get_engine()
        sources_config = load_yaml(SOURCES_CONFIG_PATH)
        
        # 1. Localización del directorio en Capa Gold
        # Se asume la llave 'desnutricion_aguda_5' en el yaml
        gold_rel_path = sources_config["desnutricion_aguda_5"]["path_gold"]
        gold_dir = PROJECT_ROOT / gold_rel_path
        
        if not gold_dir.exists():
            logging.error("Ruta de origen Gold no válida: %s", gold_dir)
            return

        # 2. Selección de la última corrida (Run Discovery)
        latest_file = get_latest_run_file(gold_dir)
        if not latest_file:
            logging.warning("No se detectaron archivos de corrida en %s", gold_dir)
            return

        # 3. Control de Sincronización (Decoupled State)
        db_state = load_state("desnutricion_aguda_5", STATE_DB_PATH)
        if db_state.get("last_loaded_file") == latest_file.name:
            logging.info("Archivo %s ya procesado previamente. Finalizando ejecución.", latest_file.name)
            return

        logging.info("Archivo seleccionado para sincronización: %s", latest_file.name)
        
        # 4. Lectura con preservación de tipos (DANE codes como string)
        df_source = pd.read_csv(latest_file, dtype={'id_muni': str})
        df_source['date_event'] = pd.to_datetime(df_source['date_event'])
        
        # Deducción dinámica de años para la limpieza
        years_to_clean = df_source['year'].unique().tolist()

        with engine.begin() as conn:
            # 5. Asegurar Infraestructura SQL
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS acute_malnutrition_5 (
                    confirmed INTEGER,
                    condition_end VARCHAR(100),
                    date_event DATE,
                    year INTEGER,
                    age INTEGER,
                    id_muni VARCHAR(10),
                    total_cases INTEGER
                );
            """))

            # 6. Operación Idempotente: Borrado por Partición
            logging.info("Limpiando particiones para los años: %s", years_to_clean)
            conn.execute(
                text("DELETE FROM acute_malnutrition_5 WHERE year IN :years"),
                {"years": tuple(years_to_clean)}
            )

            # 7. Inserción de Datos Transformados
            logging.info("Insertando %d registros en acute_malnutrition_5", len(df_source))
            df_source.to_sql("acute_malnutrition_5", conn, if_exists="append", index=False)
            
            # 8. Actualización de Checkpoint en state_db.yaml
            update_state(
                key="desnutricion_aguda_5",
                incremental_value=latest_file.name,
                incremental_column="run_filename",
                row_count=len(df_source),
                extraction_mode="db_load_gold_run",
                path_state=STATE_DB_PATH
            )
            logging.info("Proceso de carga completado exitosamente para %s", latest_file.name)

    except Exception as e:
        logging.critical("Fallo crítico en el pipeline de carga (Aguda): %s", str(e), exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()