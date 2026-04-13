from __future__ import annotations

import logging
import sys
from pathlib import Path
import pandas as pd
import geopandas as gpd
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
# INFRAESTRUCTURA Y CARGA
# ============================================================
def ensure_subregion_infrastructure(engine) -> None:
    """Garantiza la existencia de la tabla geoespacial para subregiones."""
    logging.info("Validando infraestructura para la tabla subregion")
    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS subregion (
                id_subregion VARCHAR(20) PRIMARY KEY,
                id_dept VARCHAR(10),
                name_subregion VARCHAR(150),
                geometry GEOMETRY(MultiPolygon, 4326)
            );
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_subregion_geom 
            ON subregion USING GIST (geometry);
        """))

def main() -> None:
    setup_logging(LOG_DIR, "load_subregion.log")
    logging.info("Iniciando carga de geografía: subregion")

    try:
        engine = get_engine()
        sources_config = load_yaml(SOURCES_CONFIG_PATH)
        ensure_subregion_infrastructure(engine)

        # 1. Resolución de ruta desde configuración
        # Se espera en sources.yaml: subregion -> path_gold: "data/golden/subregion.parquet"
        parquet_path = PROJECT_ROOT / sources_config["subregion"]["path_gold"]
        
        if not parquet_path.exists():
            logging.error("Archivo Parquet no encontrado en: %s", parquet_path)
            return

        # 2. Lectura geoespacial
        gdf = gpd.read_parquet(parquet_path)

        # 3. Normalización CRS (PostGIS requiere 4326 para este esquema)
        if gdf.crs is None or gdf.crs != "EPSG:4326":
            logging.info("Reproyectando subregiones a EPSG:4326")
            gdf = gdf.to_crs(epsg=4326)

        with engine.begin() as conn:
            # 4. Lógica de Sincronización (Evitar duplicados)
            existing_ids = pd.read_sql("SELECT id_subregion FROM subregion", conn)["id_subregion"].tolist()
            df_to_load = gdf[~gdf["id_subregion"].isin(existing_ids)]

            if not df_to_load.empty:
                logging.info("Insertando %d nuevas subregiones.", len(df_to_load))
                df_to_load.to_postgis("subregion", conn, if_exists="append", index=False)
                
                # 5. Actualización de estado de DB
                update_state(
                    key="subregion",
                    incremental_value=max(df_to_load["id_subregion"]),
                    incremental_column="id_subregion",
                    row_count=len(df_to_load),
                    extraction_mode="db_load_geo_parquet",
                    path_state=STATE_DB_PATH
                )
                logging.info("Carga de subregiones finalizada exitosamente.")
            else:
                logging.info("La tabla subregion ya está actualizada.")

    except Exception as e:
        logging.critical("Error en la carga de subregiones: %s", str(e), exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()