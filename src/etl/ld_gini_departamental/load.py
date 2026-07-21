"""
load.py — Carga Índice de Gini Departamental a Postgres.

El parquet golden contiene (id_dept, year, gini).
Upsert sobre (id_dept, year) como clave única.
"""

import logging
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.etl.utils.db_utils import get_engine
from src.etl.utils.config_utils import load_state, update_state
from src.etl.utils.logging_utils import setup_logging

PROJECT_ROOT  = Path(__file__).resolve().parents[3]
GOLDEN_DIR    = PROJECT_ROOT / "data" / "golden" / "gini_departamental"
STATE_DB_PATH = PROJECT_ROOT / "config" / "state_db.yaml"
LOG_DIR       = PROJECT_ROOT / "logs"
STATE_KEY     = "gini_departamental_load"
FILE_PREFIX   = "gini_departamental"

create_table_sql = """
CREATE TABLE IF NOT EXISTS gini_departamental (
    id_gini  SERIAL PRIMARY KEY,
    id_dept  VARCHAR(2),
    year     INTEGER,
    gini     DOUBLE PRECISION,
    CONSTRAINT ux_gini_dept UNIQUE (id_dept, year)
);
"""

create_index_sql = """
CREATE INDEX IF NOT EXISTS ix_gini_dept_year
ON gini_departamental (id_dept, year);
"""


def _get_latest_golden() -> Path | None:
    files = sorted(GOLDEN_DIR.glob(f"{FILE_PREFIX}_*.parquet"))
    return files[-1] if files else None


def run() -> None:
    setup_logging(LOG_DIR, "load_gini_departamental.log")
    logging.info("Iniciando carga de gini_departamental")

    try:
        latest = _get_latest_golden()
        if latest is None:
            logging.warning("No hay archivos golden en %s", GOLDEN_DIR)
            return

        state = load_state(STATE_KEY, STATE_DB_PATH)
        if state.get("last_incremental_value") == latest.name:
            logging.info("Archivo %s ya cargado. Omitiendo.", latest.name)
            return

        df = pd.read_parquet(latest)
        logging.info("Registros en golden: %d", len(df))

        engine = get_engine()

        with engine.begin() as conn:
            conn.execute(text(create_table_sql))
            conn.execute(text(create_index_sql))

            logging.info("Subiendo a tabla temporal")
            df.to_sql("temp_gini_departamental", conn, if_exists="replace", index=False)

            logging.info("Ejecutando upsert sobre gini_departamental")
            result = conn.execute(text("""
                INSERT INTO gini_departamental (id_dept, year, gini)
                SELECT t.id_dept, t.year, t.gini
                FROM temp_gini_departamental t
                INNER JOIN dim_departament d ON d.id_dept = t.id_dept
                ON CONFLICT (id_dept, year)
                DO UPDATE SET gini = EXCLUDED.gini;
            """))
            logging.info("Registros insertados/actualizados: %d", result.rowcount)

            conn.execute(text("DROP TABLE IF EXISTS temp_gini_departamental;"))

        update_state(
            key=STATE_KEY,
            incremental_value=latest.name,
            incremental_column="file_name",
            row_count=len(df),
            extraction_mode="golden_load",
            path_state=STATE_DB_PATH,
        )

        logging.info("Carga finalizada correctamente para %s", latest.name)

    except Exception as e:
        logging.critical("Fallo en la carga de gini_departamental: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    run()
