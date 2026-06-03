from __future__ import annotations

import logging
import sys
from pathlib import Path
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

def run() -> None:
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
            # 4. Sincronización: insertar nuevas subregiones y completar
            # placeholders creados por cargas de perfil_antioquia.
            staging_table = "_staging_subregion"
            conn.execute(text(f'DROP TABLE IF EXISTS "{staging_table}";'))
            gdf.to_postgis(staging_table, conn, if_exists="replace", index=False)
            result = conn.execute(text(f"""
                INSERT INTO subregion (id_subregion, id_dept, name_subregion, geometry)
                SELECT id_subregion, id_dept, name_subregion, geometry
                FROM "{staging_table}"
                WHERE id_subregion IS NOT NULL
                ON CONFLICT (id_subregion) DO UPDATE SET
                    id_dept = EXCLUDED.id_dept,
                    name_subregion = EXCLUDED.name_subregion,
                    geometry = EXCLUDED.geometry;
            """))
            conn.execute(text(f'DROP TABLE IF EXISTS "{staging_table}";'))

            row_count = result.rowcount if result.rowcount is not None else len(gdf)
            update_state(
                key="subregion",
                incremental_value=max(gdf["id_subregion"]),
                incremental_column="id_subregion",
                row_count=row_count,
                extraction_mode="db_upsert_geo_parquet",
                path_state=STATE_DB_PATH
            )
            logging.info("Carga de subregiones finalizada: %d filas sincronizadas.", row_count)

    except Exception as e:
        logging.critical("Error en la carga de subregiones: %s", str(e), exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    run()
