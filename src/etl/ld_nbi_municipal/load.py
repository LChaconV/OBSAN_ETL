"""
load.py — Carga NBI municipal a Postgres.

El parquet golden ya contiene id_mun construido en transform.py.
Upsert sobre (id_mun, year, indicador) como clave única.
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
GOLDEN_DIR    = PROJECT_ROOT / "data" / "golden" / "nbi_municipal"
STATE_DB_PATH = PROJECT_ROOT / "config" / "state_db.yaml"
LOG_DIR       = PROJECT_ROOT / "logs"
STATE_KEY     = "nbi_municipal_load"
FILE_PREFIX   = "nbi_municipal"


create_table_sql = """
CREATE TABLE IF NOT EXISTS nbi_municipal (
    id_nbi    SERIAL PRIMARY KEY,
    id_mun    VARCHAR(5),
    year      INTEGER,
    indicador VARCHAR(50),
    valor     DOUBLE PRECISION,
    CONSTRAINT fk_mun_nbi
        FOREIGN KEY (id_mun) REFERENCES dim_divipola(id_mun),
    CONSTRAINT ux_nbi_municipal UNIQUE (id_mun, year, indicador)
);
"""

create_index_sql = """
CREATE INDEX IF NOT EXISTS ix_nbi_mun_year
ON nbi_municipal (id_mun, year);
"""


def _get_latest_golden() -> Path | None:
    files = sorted(GOLDEN_DIR.glob(f"{FILE_PREFIX}_*.parquet"))
    return files[-1] if files else None


def run() -> None:
    setup_logging(LOG_DIR, "load_nbi_municipal.log")
    logging.info("Iniciando carga de nbi_municipal")

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
            df.to_sql("temp_nbi_municipal", conn, if_exists="replace", index=False)

            logging.info("Ejecutando upsert sobre nbi_municipal")
            conn.execute(text("""
                INSERT INTO nbi_municipal (id_mun, year, indicador, valor)
                SELECT id_mun, year, indicador, valor
                FROM temp_nbi_municipal
                ON CONFLICT (id_mun, year, indicador)
                DO UPDATE SET valor = EXCLUDED.valor;
            """))

            conn.execute(text("DROP TABLE IF EXISTS temp_nbi_municipal;"))

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
        logging.critical("Fallo en la carga de nbi_municipal: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    run()
