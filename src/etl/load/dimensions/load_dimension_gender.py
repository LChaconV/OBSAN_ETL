from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.config_utils import load_yaml
from src.etl.utils.db_utils import get_engine

# ============================================================
# RUTAS DEL PROYECTO
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

TRANSFORM_CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "pme_jefe_hogar_transform.yaml"
LOG_DIR = PROJECT_ROOT / "logs"

# ============================================================
# PROCESO DE CARGA
# ============================================================
def main() -> None:
    setup_logging(LOG_DIR, "load_gender_dimensions.log")
    logging.info("Iniciando carga de dimensión: dim_gender")

    try:
        engine = get_engine()
        config = load_yaml(TRANSFORM_CONFIG_PATH)["pme_jefe_hogar_transform"]
        
        # Extraer rutas y nombres desde el YAML
        dim_cfg = config["dimensions"]["gender_type"]
        dim_dir = PROJECT_ROOT / config["source"]["dimension_dir"]
        dim_file = dim_dir / dim_cfg["file_name"]
        
        table_name = "dim_gender"

        if not dim_file.exists():
            logging.error("No se encontró el archivo de dimensión en: %s", dim_file)
            return

        # 1. Lectura de la dimensión consolidada
        df_dim = pd.read_csv(dim_file)

        # 2. Definición del DDL (Esquema)
        # Usamos id_gender como Primary Key
        create_dim_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id_gender INTEGER PRIMARY KEY,
            gender TEXT NOT NULL
        );
        """


        with engine.begin() as conn:
            logging.info("Verificando tabla %s", table_name)
            conn.execute(text(create_dim_query))

            # limpiar y recargar el maestro

            logging.info("Sincronizando registros en %s...", table_name)
  
            conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE;"))
            df_dim.to_sql(table_name, conn, if_exists="append", index=False)

        logging.info("Dimensión %s cargada exitosamente. Total registros: %s", table_name, len(df_dim))

    except Exception as e:
        logging.critical("Error cargando dimensión de víctimas: %s", e, exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()