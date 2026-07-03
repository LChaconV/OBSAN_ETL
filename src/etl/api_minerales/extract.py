from __future__ import annotations

import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.etl.utils.request_utils import fetch_api_page
from src.etl.utils.config_utils import load_yaml

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

CONFIG_PATH = PROJECT_ROOT / "config" / "sources.yaml"


def _load_config() -> dict:
    return load_yaml(CONFIG_PATH)["minerales"]


def find_years_to_process(last_checkpoint: str | None) -> list[int]:
    """
    Retorna los años que deben procesarse en esta corrida:
    - Años cuyo :updated_at en la API es posterior al último checkpoint.
    - Siempre incluye el año calendario actual (datos incompletos).
    - Si no existe checkpoint (primera corrida), retorna todos los años disponibles.
    """
    cfg = _load_config()
    base_url = cfg["base_url"]
    timeout = cfg["request"]["timeout_seconds"]
    max_retries = cfg["request"]["max_retries"]

    params: dict = {
        "$select": "a_o_liquidado",
        "$group": "a_o_liquidado",
        "$limit": 1000,
    }
    if last_checkpoint:
        params["$where"] = f":updated_at > '{last_checkpoint}'"

    logging.info(
        "Consultando años con cambios desde checkpoint: %s",
        last_checkpoint or "ninguno (primera corrida)",
    )

    rows = fetch_api_page(base_url=base_url, params=params, timeout=timeout, max_retries=max_retries)

    years: set[int] = set()
    for row in rows:
        raw = row.get("a_o_liquidado")
        if raw is not None:
            try:
                years.add(int(float(raw)))
            except (ValueError, TypeError):
                pass

    # El año actual siempre se reprocesa (datos aún incompletos)
    years.add(datetime.now().year)

    result = sorted(years)
    logging.info("Años a procesar: %s", result)
    return result


def fetch_year_data(year: int) -> pd.DataFrame:
    """
    Descarga todos los registros de la API para un año dado (paginación completa).
    """
    cfg = _load_config()
    base_url = cfg["base_url"]
    page_size = cfg["pagination"]["page_size"]
    sleep_seconds = cfg["pagination"]["sleep_seconds"]
    timeout = cfg["request"]["timeout_seconds"]
    max_retries = cfg["request"]["max_retries"]
    select_columns = cfg.get("select_columns", [])
    select_clause = ",".join(select_columns) if select_columns else None

    logging.info("Descargando datos del año %s desde la API...", year)

    all_rows: list[dict] = []
    offset = 0
    page = 1

    while True:
        params: dict = {
            "$where": f"a_o_liquidado='{year}'",
            "$limit": page_size,
            "$offset": offset,
        }
        if select_clause:
            params["$select"] = select_clause

        rows = fetch_api_page(base_url=base_url, params=params, timeout=timeout, max_retries=max_retries)
        logging.info("Año %s — página %s: %s filas", year, page, len(rows))

        if not rows:
            break

        all_rows.extend(rows)

        if len(rows) < page_size:
            break

        offset += page_size
        page += 1
        time.sleep(sleep_seconds)

    logging.info("Total filas descargadas para año %s: %s", year, len(all_rows))
    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame()


def find_all_years_in_api() -> list[int]:
    """Retorna todos los años distintos presentes en la API (sin filtro de fecha)."""
    cfg = _load_config()
    base_url = cfg["base_url"]
    timeout = cfg["request"]["timeout_seconds"]
    max_retries = cfg["request"]["max_retries"]

    rows = fetch_api_page(
        base_url=base_url,
        params={"$select": "a_o_liquidado", "$group": "a_o_liquidado", "$limit": 1000},
        timeout=timeout,
        max_retries=max_retries,
    )

    years: list[int] = []
    for row in rows:
        raw = row.get("a_o_liquidado")
        if raw is not None:
            try:
                years.append(int(float(raw)))
            except (ValueError, TypeError):
                pass

    years = sorted(set(years))
    logging.info("Años disponibles en la API: %s", years)
    return years


def get_api_max_updated_at() -> str | None:
    """
    Retorna el valor máximo de :updated_at en toda la API (para actualizar el checkpoint).
    """
    cfg = _load_config()
    base_url = cfg["base_url"]
    timeout = cfg["request"]["timeout_seconds"]
    max_retries = cfg["request"]["max_retries"]

    rows = fetch_api_page(
        base_url=base_url,
        params={"$select": "max(:updated_at) AS max_updated_at", "$limit": 1},
        timeout=timeout,
        max_retries=max_retries,
    )
    if rows and rows[0].get("max_updated_at"):
        return rows[0]["max_updated_at"]
    return None
