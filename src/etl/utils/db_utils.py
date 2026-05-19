from __future__ import annotations

import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# Definición de ruta relativa al proyecto
PROJECT_ROOT = Path(__file__).resolve().parents[3]
ENV_PATH = PROJECT_ROOT / ".env"


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise ValueError(f"Falta la variable de entorno obligatoria: {name}")
    return value.strip()


def get_engine():
    """
    Carga credenciales de base de datos desde variables de entorno y
    retorna el motor de SQLAlchemy.
    """
    load_dotenv(dotenv_path=ENV_PATH, override=False)

    db_host = _require_env("DB_HOST")
    db_name = _require_env("DB_NAME")
    db_user = _require_env("DB_USER")
    db_password = _require_env("DB_PASSWORD")
    db_port_raw = os.getenv("DB_PORT", "5432").strip()

    try:
        db_port = int(db_port_raw)
    except ValueError as exc:
        raise ValueError(
            f"DB_PORT debe ser un entero válido. Valor recibido: {db_port_raw!r}"
        ) from exc

    # Logging formal de la conexión (sin mostrar la contraseña)
    logging.info(
        "Estableciendo motor de conexión para: %s@%s:%s/%s",
        db_user,
        db_host,
        db_port,
        db_name,
    )

    connection_url = URL.create(
        "postgresql+psycopg2",
        username=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
        database=db_name,
    )

    return create_engine(connection_url)
