"""
pipeline.py — Minerales: extracción, transformación y carga por año.

Estrategia:
  1. Consulta a la API qué años tienen registros con :updated_at posterior
     al último checkpoint guardado en state/state.yaml.
  2. Siempre añade el año calendario actual (datos aún incompletos).
  3. Para cada año a procesar: descarga el año completo desde la API,
     agrega al grano municipio/mineral, y hace UPSERT en mineral_royalties.
  4. Actualiza el checkpoint con el max(:updated_at) actual de la API.

Garantía de correctitud:
  - Cada año se descarga completo → el total en DB refleja el año entero.
  - La deduplicación por :id (Socrata) dentro del año elimina registros
    duplicados o actualizados antes de agregar.
  - El UPSERT reemplaza: como el valor ya es el total real del año,
    reemplazar es siempre correcto.
  - Re-correr la misma corrida produce el mismo resultado (idempotente).
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

from sqlalchemy import text

from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.config_utils import load_state, update_state, load_yaml, save_yaml
from src.etl.utils.db_utils import get_engine
from src.etl.utils.load_utils import build_upsert_query, write_frame_to_db
from src.etl.utils.pipeline_cleanup import run_with_cleanup

from . import extract, transform

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

STATE_PATH   = PROJECT_ROOT / "state" / "state.yaml"
LOG_DIR      = PROJECT_ROOT / "logs"
STATE_KEY    = "minerales"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS mineral_royalties (
    id_min_royal  SERIAL PRIMARY KEY,
    year          INTEGER,
    id_mun        VARCHAR(10),
    mineral_resource VARCHAR(255),
    unit_measure  VARCHAR(50),
    royalties_cop FLOAT,
    CONSTRAINT fk_divipola FOREIGN KEY (id_mun) REFERENCES dim_divipola(id_mun)
);
"""

CREATE_INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_mineral_royalties
ON mineral_royalties (year, mineral_resource, id_mun);
"""

CONFLICT_COLUMNS = ["year", "mineral_resource", "id_mun"]
UPDATE_COLUMNS   = ["royalties_cop"]


def _upsert_year(df_golden, engine) -> None:
    """Hace UPSERT del golden de un año en mineral_royalties."""
    cols = list(df_golden.columns)
    with engine.begin() as conn:
        write_frame_to_db(df_golden, table_name="temp_mineral_royalties", conn=conn, if_exists="replace")

        upsert_sql = build_upsert_query(
            table_name="mineral_royalties",
            temp_table="temp_mineral_royalties",
            insert_columns=cols,
            conflict_columns=CONFLICT_COLUMNS,
            update_columns=UPDATE_COLUMNS,
        )
        conn.execute(text(upsert_sql))
        conn.execute(text("DROP TABLE IF EXISTS temp_mineral_royalties;"))


def _load_years_loaded() -> set[int]:
    """Lee los años ya procesados exitosamente desde state.yaml."""
    full = load_yaml(STATE_PATH) if STATE_PATH.exists() else {}
    return set(full.get(STATE_KEY, {}).get("years_loaded", []))


def _save_years_loaded(years_loaded: set[int], new_checkpoint: str | None) -> None:
    """Guarda years_loaded y checkpoint en state.yaml preservando las demás claves."""
    full = load_yaml(STATE_PATH) if STATE_PATH.exists() else {}
    entry = full.get(STATE_KEY, {})
    entry["years_loaded"] = sorted(years_loaded)
    if new_checkpoint:
        entry["last_incremental_value"] = new_checkpoint
        entry["incremental_column"] = ":updated_at"
        entry["extraction_mode"] = "incremental"
    import pandas as pd
    entry["last_run_at"] = pd.Timestamp.utcnow().isoformat()
    full[STATE_KEY] = entry
    save_yaml(STATE_PATH, full)


def _run_steps(**kwargs) -> None:
    setup_logging(LOG_DIR, "minerales.log")
    logging.info("=== Inicio pipeline minerales ===")

    # ── 1. Estado actual ──────────────────────────────────────
    state        = load_state(STATE_KEY, STATE_PATH)
    last_checkpoint = state.get("last_incremental_value")
    years_loaded = _load_years_loaded()
    logging.info("Años ya cargados en DB: %s", sorted(years_loaded))

    # ── 2. Años con cambios recientes en la API ───────────────
    years_changed = set(extract.find_years_to_process(last_checkpoint))

    # ── 3. Años en la API que nunca se han procesado ──────────
    all_api_years = set(extract.find_all_years_in_api())
    years_never_loaded = all_api_years - years_loaded
    if years_never_loaded:
        logging.info("Años nuevos nunca cargados: %s", sorted(years_never_loaded))

    # ── 4. Unión: cambios + nunca cargados + año actual ───────
    years = sorted(years_changed | years_never_loaded)
    if not years:
        logging.info("Sin años a procesar. Pipeline finalizado.")
        _save_years_loaded(years_loaded, None)
        return

    logging.info("Años a procesar en esta corrida: %s", years)

    # ── 5. Preparar tabla en DB ───────────────────────────────
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(CREATE_TABLE_SQL))
        conn.execute(text(CREATE_INDEX_SQL))

    # ── 6. Procesar cada año ──────────────────────────────────
    transform_cfg = transform.get_config()

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
        logging.info("Año %s: %s filas a cargar en DB.", year, len(df_golden))

        _upsert_year(df_golden, engine)
        years_loaded.add(year)
        logging.info("Año %s: cargado correctamente.", year)

    # ── 7. Guardar estado ─────────────────────────────────────
    new_checkpoint = extract.get_api_max_updated_at()
    _save_years_loaded(years_loaded, new_checkpoint)
    logging.info("Estado actualizado. Checkpoint: %s", new_checkpoint)

    logging.info("=== Pipeline minerales finalizado ===")


def run(**kwargs):
    return run_with_cleanup(__name__, _run_steps, **kwargs)


if __name__ == "__main__":
    run()
