"""
pipeline.py — Regalías por campo de hidrocarburos: extracción, transformación y carga por año.

Estrategia:
  1. Consulta a la API qué años tienen registros con :updated_at posterior
     al último checkpoint guardado en state/state.yaml.
  2. Siempre añade el año calendario actual (datos aún incompletos).
  3. Para cada año a procesar: descarga el año completo desde la API,
     agrega al grano campo (latitud/longitud), y hace UPSERT en royalties.
  4. Actualiza el checkpoint con el max(:updated_at) actual de la API.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.config_utils import load_yaml, save_yaml
from src.etl.utils.db_utils import get_engine
from src.etl.utils.load_utils import build_upsert_query, write_frame_to_db
from src.etl.utils.pipeline_cleanup import run_with_cleanup

from . import extract, transform

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

STATE_PATH = PROJECT_ROOT / "state" / "state.yaml"
LOG_DIR    = PROJECT_ROOT / "logs"
STATE_KEY  = "regalias"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS royalties (
    id_royalties  SERIAL PRIMARY KEY,
    year          INTEGER,
    royalties_cop FLOAT,
    geometry      GEOMETRY,
    CONSTRAINT ux_royalties UNIQUE (year, geometry)
);
"""

CONFLICT_COLUMNS = ["year", "geometry"]
UPDATE_COLUMNS   = ["royalties_cop"]


def _upsert_year(df_golden: pd.DataFrame, engine) -> None:
    cols = list(df_golden.columns)
    with engine.begin() as conn:
        write_frame_to_db(df_golden, table_name="temp_royalties", conn=conn, if_exists="replace")
        upsert_sql = build_upsert_query(
            table_name="royalties",
            temp_table="temp_royalties",
            insert_columns=cols,
            conflict_columns=CONFLICT_COLUMNS,
            update_columns=UPDATE_COLUMNS,
        )
        conn.execute(text(upsert_sql))
        conn.execute(text("DROP TABLE IF EXISTS temp_royalties;"))


def _load_years_loaded() -> set[int]:
    full = load_yaml(STATE_PATH) if STATE_PATH.exists() else {}
    return set(full.get(STATE_KEY, {}).get("years_loaded", []))


def _save_years_loaded(years_loaded: set[int], new_checkpoint: str | None) -> None:
    full = load_yaml(STATE_PATH) if STATE_PATH.exists() else {}
    entry = full.get(STATE_KEY, {})
    entry["years_loaded"] = sorted(years_loaded)
    if new_checkpoint:
        entry["last_incremental_value"] = new_checkpoint
        entry["incremental_column"] = ":updated_at"
        entry["extraction_mode"] = "incremental"
    entry["last_run_at"] = pd.Timestamp.utcnow().isoformat()
    full[STATE_KEY] = entry
    save_yaml(STATE_PATH, full)


def _run_steps(**kwargs) -> None:
    setup_logging(LOG_DIR, "regalias.log")
    logging.info("=== Inicio pipeline regalías ===")

    full = load_yaml(STATE_PATH) if STATE_PATH.exists() else {}
    last_checkpoint = full.get(STATE_KEY, {}).get("last_incremental_value")
    years_loaded = _load_years_loaded()
    logging.info("Años ya cargados en DB: %s", sorted(years_loaded))

    years_changed = set(extract.find_years_to_process(last_checkpoint))

    all_api_years      = set(extract.find_all_years_in_api())
    years_never_loaded = all_api_years - years_loaded
    if years_never_loaded:
        logging.info("Años nuevos nunca cargados: %s", sorted(years_never_loaded))

    years = sorted(years_changed | years_never_loaded)
    if not years:
        logging.info("Sin años a procesar. Pipeline finalizado.")
        _save_years_loaded(years_loaded, None)
        return

    logging.info("Años a procesar en esta corrida: %s", years)

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(CREATE_TABLE_SQL))

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

    new_checkpoint = extract.get_api_max_updated_at()
    _save_years_loaded(years_loaded, new_checkpoint)
    logging.info("Estado actualizado. Checkpoint: %s", new_checkpoint)
    logging.info("=== Pipeline regalías finalizado ===")


def run(**kwargs):
    return run_with_cleanup(__name__, _run_steps, **kwargs)


if __name__ == "__main__":
    run()
