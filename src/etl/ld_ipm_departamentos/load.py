"""
load.py — carga IPM departamental a Postgres.

El parquet golden no contiene id_dept (el Excel DANE solo trae nombre).
Este módulo resuelve id_dept haciendo JOIN con dim_departament por nombre
normalizado (upper, sin tildes) en Python antes del upsert.
"""

import logging
import sys
import unicodedata
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.etl.utils.db_utils import get_engine
from src.etl.utils.config_utils import load_state, update_state
from src.etl.utils.logging_utils import setup_logging

PROJECT_ROOT  = Path(__file__).resolve().parents[4]
GOLDEN_DIR    = PROJECT_ROOT / "data" / "golden" / "ipm_departamental"
STATE_DB_PATH = PROJECT_ROOT / "config" / "state_db.yaml"
LOG_DIR       = PROJECT_ROOT / "logs"
STATE_KEY     = "ipm_departamental_load"
FILE_PREFIX   = "ipm_departamental"


create_table_sql = """
CREATE TABLE IF NOT EXISTS ipm_departamental (
    id_ipm    SERIAL PRIMARY KEY,
    id_dept   VARCHAR(10),
    name_dept VARCHAR(200),
    year      INTEGER,
    total     DOUBLE PRECISION,
    cabeceras DOUBLE PRECISION,
    rural     DOUBLE PRECISION,
    CONSTRAINT fk_dept_ipm
        FOREIGN KEY (id_dept)
        REFERENCES dim_departament(id_dept)
);
"""

create_index_sql = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_ipm_departamental
ON ipm_departamental (id_dept, year);
"""


def _normalize(s) -> str:
    text = str(s).strip().upper()
    text = unicodedata.normalize("NFKD", text)
    return "".join(c for c in text if not unicodedata.combining(c))


# Nombres que el DANE escribe de forma abreviada pero que en dim_departament
# aparecen con la denominación oficial completa.
# Claves y valores deben estar YA normalizados (_normalize aplicado).
_NAME_ALIASES: dict[str, str] = {
    "SAN ANDRES":                         "SAN ANDRES PROVIDENCIA Y SANTA CATALINA",
    "SAN ANDRES, PROVIDENCIA Y SANTA CATALINA": "SAN ANDRES PROVIDENCIA Y SANTA CATALINA",
    "ARCHIPIELAGO DE SAN ANDRES PROVIDENCIA Y SANTA CATALINA":
        "SAN ANDRES PROVIDENCIA Y SANTA CATALINA",
}

# Departamentos que legítimamente no tienen código en dim_departament
# (se descartan sin emitir WARNING).
_KNOWN_NO_DEPT: frozenset[str] = frozenset({
    "BOGOTA D.C.",
    "BOGOTA",
    "DISTRITO CAPITAL",
})


def _get_latest_golden() -> Path | None:
    files = sorted(GOLDEN_DIR.glob(f"{FILE_PREFIX}_run_*.parquet"))
    return files[-1] if files else None


def run() -> None:
    setup_logging(LOG_DIR, "load_ipm_departamental.log")
    logging.info("Iniciando carga de ipm_departamental")

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

        # ── Obtener mapeo nombre normalizado → id_dept ─────────────────────
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT id_dept, name_dept FROM dim_departament")
            ).fetchall()

        name_to_id: dict[str, str] = {_normalize(r.name_dept): r.id_dept for r in rows}

        def _resolve(name: str) -> str | None:
            key = _normalize(name)
            key = _NAME_ALIASES.get(key, key)
            return name_to_id.get(key)

        df["id_dept"] = df["name_dept"].apply(_resolve)

        unmatched_mask = df["id_dept"].isna()
        if unmatched_mask.any():
            unmatched_names = sorted(df.loc[unmatched_mask, "name_dept"].unique())
            unexpected = [n for n in unmatched_names if _normalize(n) not in _KNOWN_NO_DEPT]
            expected   = [n for n in unmatched_names if _normalize(n) in _KNOWN_NO_DEPT]
            if expected:
                logging.info(
                    "Departamentos descartados (sin código en dim_departament): %s",
                    expected,
                )
            if unexpected:
                logging.warning(
                    "Departamentos sin coincidencia inesperada en dim_departament (%d): %s",
                    len(unexpected),
                    unexpected,
                )

        df = df[df["id_dept"].notna()].copy()
        if df.empty:
            raise ValueError("No quedaron registros tras el join con dim_departament.")

        logging.info("Registros a cargar: %d", len(df))

        # ── Upsert ─────────────────────────────────────────────────────────
        cols = ["id_dept", "name_dept", "year", "total", "cabeceras", "rural"]
        df_load = df[cols]

        with engine.begin() as conn:
            conn.execute(text(create_table_sql))
            conn.execute(text(create_index_sql))

            logging.info("Subiendo a tabla temporal")
            df_load.to_sql("temp_ipm_departamental", conn, if_exists="replace", index=False)

            logging.info("Ejecutando upsert sobre ipm_departamental")
            conn.execute(text("""
                INSERT INTO ipm_departamental (id_dept, name_dept, year, total, cabeceras, rural)
                SELECT id_dept, name_dept, year, total, cabeceras, rural
                FROM temp_ipm_departamental
                ON CONFLICT (id_dept, year)
                DO UPDATE SET
                    total     = EXCLUDED.total,
                    cabeceras = EXCLUDED.cabeceras,
                    rural     = EXCLUDED.rural,
                    name_dept = EXCLUDED.name_dept;
            """))

            conn.execute(text("DROP TABLE IF EXISTS temp_ipm_departamental;"))

        update_state(
            key=STATE_KEY,
            incremental_value=latest.name,
            incremental_column="file_name",
            row_count=len(df_load),
            extraction_mode="golden_load",
            path_state=STATE_DB_PATH,
        )

        logging.info("Carga finalizada correctamente para %s", latest.name)

    except Exception as e:
        logging.critical("Fallo en la carga de ipm_departamental: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    run()
