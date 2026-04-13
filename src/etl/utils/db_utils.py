from __future__ import annotations

import logging
from pathlib import Path
from sqlalchemy import create_engine
from src.etl.utils.config_utils import load_yaml

# Definición de ruta relativa al proyecto
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DB_CONFIG_PATH = PROJECT_ROOT / "config" / "db.yaml"

def get_engine():
    """
    Carga las credenciales desde db.yaml y retorna el motor de SQLAlchemy.
    """
    if not DB_CONFIG_PATH.exists():
        logging.error("Archivo de configuración de base de datos no encontrado: %s", DB_CONFIG_PATH)
        raise FileNotFoundError(f"Falta el archivo {DB_CONFIG_PATH}")

    config = load_yaml(DB_CONFIG_PATH)
    db = config['postgresql']
    
    # Logging formal de la conexión (sin mostrar la contraseña)
    logging.info("Estableciendo motor de conexión para: %s@%s:%s/%s", 
                 db['user'], db['host'], db['port'], db['database'])
    
    connection_url = (
        f"postgresql://{db['user']}:{db['pass']}@"
        f"{db['host']}:{db['port']}/{db['database']}"
    )
    
    return create_engine(connection_url)