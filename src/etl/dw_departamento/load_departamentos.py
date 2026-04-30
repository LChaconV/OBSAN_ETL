from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd
import geopandas as gpd
from sqlalchemy import text

from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.config_utils import load_yaml, load_state, update_state
from src.etl.utils.db_utils import get_engine

# ============================================================
# CONFIGURACIÓN DE RUTAS Y CONSTANTES
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

DB_CONFIG_PATH = PROJECT_ROOT / "config" / "db.yaml"
SOURCES_CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "departamentos_transform.yaml"
STATE_DB_PATH = PROJECT_ROOT / "config" / "state_db.yaml"
LOG_DIR = PROJECT_ROOT / "logs"

# ============================================================
# GESTIÓN DE INFRAESTRUCTURA SQL
# ============================================================
def ensure_db_infrastructure(engine) -> None:

    logging.info("Validando infraestructura de base de datos para dim_departament")
    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
 
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_departament (
                id_dept VARCHAR(10) PRIMARY KEY,
                name_dept VARCHAR(150),
                geometry GEOMETRY(Geometry, 4326)
            );
        """))
        
        # Creación de índice GIST para optimizar consultas espaciales
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_departament_geom 
            ON dim_departament USING GIST (geometry);
        """))
    logging.info("Infraestructura de base de datos confirmada.")

# ============================================================
# PROCESO DE CARGA INCREMENTAL (LOAD)
# ============================================================
def main() -> None:
    # Inicialización de logging formal
    setup_logging(LOG_DIR, "load_departamentos.log")
    logging.info("Iniciando proceso de carga: dim_departament")

    try:
        # Carga de motor y configuraciones
        engine = get_engine()
        sources_config = load_yaml(SOURCES_CONFIG_PATH)
        
        # Asegurar que la infraestructura esté lista
        ensure_db_infrastructure(engine)

        # Carga de estado específico de base de datos (Decoupled State)
        db_state = load_state("departamentos", STATE_DB_PATH)
        last_loaded_id = db_state.get("last_loaded_id")
        logging.info("Estado actual en state_db.yaml: last_loaded_id=%s", last_loaded_id)

        # Resolución de ruta del archivo fuente en capa Silver
        silver_path = PROJECT_ROOT / sources_config["departamentos"]["source"]["silver_fact_dir"]
        
        if not silver_path.exists():
            logging.error("Archivo fuente no encontrado en: %s", silver_path)
            return

        logging.info("Leyendo datos desde capa Silver: %s", silver_path)
        gdf_source = gpd.read_parquet(silver_path)

        # Normalización de sistema de referencia de coordenadas
        if gdf_source.crs is None or gdf_source.crs != "EPSG:4326":
            logging.info("Reproyectando coordenadas a EPSG:4326")
            gdf_source = gdf_source.to_crs(epsg=4326)

        with engine.begin() as conn:
            # Consulta de IDs existentes para evitar duplicados y asegurar incrementalidad
            query_existing = text("SELECT id_dept FROM dim_departament")
            existing_ids = pd.read_sql(query_existing, conn)["id_dept"].tolist()
            
            # Identificación de registros para inserción
            df_to_load = gdf_source[~gdf_source["id_dept"].isin(existing_ids)]

            if not df_to_load.empty:
                logging.info("Detectados %d registros nuevos para inserción.", len(df_to_load))
                
                # Carga efectiva a PostgreSQL/PostGIS
                df_to_load.to_postgis("dim_departament", conn, if_exists="append", index=False)
                
                # Actualización de estado de base de datos post-transacción exitosa
                current_max_id = df_to_load["id_dept"].max()
                update_state(
                    key="departamentos",
                    incremental_value=current_max_id,
                    incremental_column="id_dept",
                    row_count=len(df_to_load),
                    extraction_mode="db_load",
                    path_state=STATE_DB_PATH
                )
                logging.info("Carga incremental finalizada exitosamente. Nuevo last_loaded_id: %s", current_max_id)
            else:
                logging.info("La base de datos se encuentra actualizada. No se requiere inserción.")

    except Exception as e:
        logging.critical("Error crítico en el proceso de carga: %s", str(e), exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()