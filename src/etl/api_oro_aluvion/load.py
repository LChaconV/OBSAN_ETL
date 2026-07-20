"""
load.py — Operaciones de base de datos para Explotación de Oro de Aluvión.

"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

from sqlalchemy import text

from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.db_utils import get_engine
from src.etl.utils.load_utils import write_frame_to_db

from . import extract, transform

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

LOG_DIR = PROJECT_ROOT / "logs"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS alluvial_gold_mining (
    id_gold             SERIAL PRIMARY KEY,
    id_mun             VARCHAR(5),
    year               INTEGER,
    illicit_hectares DOUBLE PRECISION,
    total_evidence    DOUBLE PRECISION,
    CONSTRAINT ux_oro_aluvion UNIQUE (id_mun, year)
);
"""

_VIEW_SQL_PATH = PROJECT_ROOT / "sql" / "views" / "v_alluvial_gold_illicit_pct.sql"


def create_table(engine) -> None:
    """Crea la tabla alluvial_gold_mining si no existe."""
    with engine.begin() as conn:
        conn.execute(text(CREATE_TABLE_SQL))
    logging.info("Tabla alluvial_gold_mining verificada/creada.")


def upsert_year(df_golden, engine) -> int:
    """
    UPSERT del golden de un año en alluvial_gold_mining.
    Omite municipios sin entrada en dim_divipola.
    Retorna el número de filas insertadas/actualizadas.
    """
    with engine.begin() as conn:
        write_frame_to_db(df_golden, table_name="temp_oro_aluvion", conn=conn, if_exists="replace")
        result = conn.execute(text("""
            INSERT INTO alluvial_gold_mining (id_mun, year, illicit_hectares, total_evidence)
            SELECT t.id_mun, t.year, t.illicit_hectares, t.total_evidence
            FROM temp_oro_aluvion t
            INNER JOIN dim_divipola d ON d.id_mun = t.id_mun
            ON CONFLICT (id_mun, year)
            DO UPDATE SET
                illicit_hectares = EXCLUDED.illicit_hectares,
                total_evidence    = EXCLUDED.total_evidence;
        """))
        conn.execute(text("DROP TABLE IF EXISTS temp_oro_aluvion;"))
    return result.rowcount


def run() -> None:
    """Carga inicial completa: DROP + CREATE + todos los años de la API + vista."""
    setup_logging(LOG_DIR, "load_oro_aluvion.log")
    logging.info("=== Inicio carga completa oro_aluvion ===")

    engine = get_engine()

    # ── 1. Recrear tabla ──────────────────────────────────────
    logging.info("Eliminando tabla alluvial_gold_mining si existe...")
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS alluvial_gold_mining;"))
    create_table(engine)

    # ── 2. Años disponibles en la API ─────────────────────────
    years = extract.find_all_years_in_api()
    if not years:
        logging.warning("No se encontraron años en la API. Carga abortada.")
        return
    logging.info("Años a cargar: %s", years)

    # ── 3. Extract → Transform → Load por año ─────────────────
    transform_cfg = transform.get_config()
    total_rows = 0

    for year in years:
        logging.info("--- Procesando año %s ---", year)

        df_raw = extract.fetch_year_data(year)
        if df_raw.empty:
            logging.warning("Año %s: sin datos en la API. Se omite.", year)
            continue

        table_fact = transform.build_table(df_raw, transform_cfg)
        if table_fact.empty:
            logging.warning("Año %s: sin filas tras transformar. Se omite.", year)
            continue

        df_golden = transform.build_golden(table_fact)
        rows = upsert_year(df_golden, engine)
        total_rows += rows
        logging.info("Año %s: %s filas cargadas.", year, rows)

    logging.info("Total registros insertados/actualizados: %s", total_rows)

    # ── 4. Crear vista ────────────────────────────────────────
    logging.info("Creando vista v_alluvial_gold_illicit_pct...")
    with engine.begin() as conn:
        conn.execute(text(_VIEW_SQL_PATH.read_text(encoding="utf-8")))
    logging.info("Vista creada correctamente.")

    logging.info("=== Carga completa finalizada ===")


if __name__ == "__main__":
    run()
