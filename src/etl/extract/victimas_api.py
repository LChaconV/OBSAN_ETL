from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path

import pandas as pd
import requests
import yaml

from src.etl.utils.logging_utils import setup_logging



# ============================================================
# RUTAS DEL PROYECTO
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parents[3]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

CONFIG_PATH = PROJECT_ROOT / "config" / "sources.yaml"
STATE_PATH = PROJECT_ROOT / "state" / "state.yaml"
LOG_DIR = PROJECT_ROOT / "logs"


# ============================================================
# YAML
# ============================================================
def load_yaml(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo: {path}")

    with path.open("r", encoding="utf-8") as file:
        data = yaml.safe_load(file) or {}

    if not isinstance(data, dict):
        raise ValueError(f"El archivo {path} no tiene una estructura válida.")

    return data


def save_yaml(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as file:
        yaml.safe_dump(data, file, sort_keys=False, allow_unicode=True)


# ============================================================
# CONFIG Y STATE
# ============================================================

def load_source_config() -> dict:
    config = load_yaml(CONFIG_PATH)

    if "victimas" not in config:
        raise KeyError("No existe la configuración 'victimas' en config/sources.yaml")

    return config["victimas"]



def load_state() -> dict:
    if not STATE_PATH.exists():
        return {
            "last_incremental_value": None,
            "incremental_column": None,
            "last_run_at": None,
            "last_row_count": 0,
            "extraction_mode": None,
        }

    state = load_yaml(STATE_PATH)

    return state.get(
        "victimas",
        {
            "last_incremental_value": None,
            "incremental_column": None,
            "last_run_at": None,
            "last_row_count": 0,
            "extraction_mode": None,
        },
    )


def update_state(
    *,
    incremental_value: str | None,
    incremental_column: str | None,
    row_count: int,
    extraction_mode: str,
) -> None:
    full_state = {}

    if STATE_PATH.exists():
        full_state = load_yaml(STATE_PATH)

    full_state["victimas"] = {
        "last_incremental_value": incremental_value,
        "incremental_column": incremental_column,
        "last_run_at": pd.Timestamp.utcnow().isoformat(),
        "last_row_count": int(row_count),
        "extraction_mode": extraction_mode,
    }

    save_yaml(STATE_PATH, full_state)
    logging.info("State actualizado correctamente.")


# ============================================================
# LÓGICA INCREMENTAL
# ============================================================
def get_extraction_mode(config: dict) -> str:
    incremental_cfg = config.get("incremental", {})
    enabled = incremental_cfg.get("enabled", False)
    column = incremental_cfg.get("column")

    if enabled and column:
        return "incremental"

    return "full_refresh"


def get_reference_value(config: dict, state: dict) -> str | None:
    last_value = state.get("last_incremental_value")

    if last_value:
        logging.info("Usando checkpoint desde state.yaml: %s", last_value)
        return last_value

    start_value = config.get("incremental", {}).get("start_value")

    if start_value:
        logging.info("No existe checkpoint. Usando start_value de sources.yaml: %s", start_value)
        return start_value

    logging.info("No existe checkpoint ni start_value.")
    return None


def build_where_clause(config: dict, state: dict) -> str | None:
    extraction_mode = get_extraction_mode(config)

    if extraction_mode != "incremental":
        return None

    incremental_column = config["incremental"]["column"]
    reference_value = get_reference_value(config, state)

    if not reference_value:
        return None

    return f"{incremental_column} > '{reference_value}'"


# ============================================================
# REQUESTS
# ============================================================
def build_headers() -> dict:
    headers = {
        "Accept": "application/json",
        "User-Agent": "observatorio-san-etl/1.0",
    }

    app_token = os.getenv("SOCRATA_APP_TOKEN")
    if app_token:
        headers["X-App-Token"] = app_token

    return headers


def fetch_api_page(
    base_url: str,
    params: dict,
    timeout: int,
    max_retries: int = 5,
) -> list[dict]:
    headers = build_headers()
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(
                base_url,
                params=params,
                timeout=timeout,
                headers=headers,
            )
            response.raise_for_status()

            data = response.json()

            if not isinstance(data, list):
                raise ValueError("La respuesta de la API no es una lista de registros.")

            return data

        except requests.exceptions.HTTPError as e:
            last_error = e
            status_code = e.response.status_code if e.response is not None else None

            if status_code in {429, 500, 502, 503, 504} and attempt < max_retries:
                wait_seconds = 2 ** (attempt - 1)
                logging.warning(
                    "Error HTTP %s. Reintentando en %s segundos. Intento %s/%s",
                    status_code,
                    wait_seconds,
                    attempt,
                    max_retries,
                )
                time.sleep(wait_seconds)
                continue

            raise

        except Exception as e:
            last_error = e

            if attempt < max_retries:
                wait_seconds = 2 ** (attempt - 1)
                logging.warning(
                    "Error de conexión. Reintentando en %s segundos. Intento %s/%s",
                    wait_seconds,
                    attempt,
                    max_retries,
                )
                time.sleep(wait_seconds)
                continue

            raise

    raise RuntimeError(f"No se pudo consultar la API después de varios intentos: {last_error}")


# ============================================================
# BRONZE
# ============================================================
def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df.columns = [str(col).strip().lower() for col in df.columns]
    df["_etl_source"] = "victimas"
    df["_etl_loaded_at"] = pd.Timestamp.utcnow()

    return df


def create_run_directory(config: dict) -> Path:
    bronze_dir = PROJECT_ROOT / config["storage"]["bronze_dir"]
    run_id = pd.Timestamp.utcnow().strftime("run_%Y%m%d_%H%M%S")

    run_dir = bronze_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    return run_dir


def save_bronze_page(df: pd.DataFrame, run_dir: Path, file_prefix: str, page_number: int) -> Path:
    output_path = run_dir / f"{file_prefix}_page_{page_number}.parquet"
    df.to_parquet(output_path, index=False)

    logging.info("Página %s guardada en %s", page_number, output_path)
    return output_path


# ============================================================
# CHECKPOINT
# ============================================================
def get_incremental_max_value(df: pd.DataFrame, config: dict) -> str | None:
    incremental_column = config.get("incremental", {}).get("column")

    if df.empty or not incremental_column:
        return None

    col_name = incremental_column.lower()

    if col_name not in df.columns:
        logging.warning(
            "La columna incremental '%s' no está en los datos descargados.",
            incremental_column,
        )
        return None

    parsed = pd.to_datetime(df[col_name], errors="coerce", utc=True)
    max_value = parsed.dropna().max()

    if pd.isna(max_value):
        return None

    return pd.Timestamp(max_value).isoformat()


# ============================================================
# EXTRACT
# ============================================================
def fetch_and_save_pages(config: dict, state: dict) -> tuple[int, Path, list[Path]]:
    base_url = config["base_url"]
    page_size = config["pagination"]["page_size"]
    sleep_seconds = config["pagination"]["sleep_seconds"]
    timeout = config["request"]["timeout_seconds"]
    max_retries = config["request"].get("max_retries", 5)
    file_prefix = config["storage"]["file_prefix"]

    extraction_mode = get_extraction_mode(config)
    where_clause = build_where_clause(config, state)

    order_column = None
    if extraction_mode == "incremental":
        order_column = config["incremental"]["column"]

    select_columns = config.get("select_columns", [])
    select_clause = ",".join(select_columns) if select_columns else None

    run_dir = create_run_directory(config)

    total_rows = 0
    offset = 0
    page_number = 1
    saved_files: list[Path] = []

    while True:
        params = {
            "$limit": page_size,
            "$offset": offset,
        }

        if select_clause:
            params["$select"] = select_clause

        if order_column:
            params["$order"] = order_column

        if where_clause:
            params["$where"] = where_clause

        logging.info("Consultando página %s con offset=%s", page_number, offset)

        try:
            rows = fetch_api_page(
                base_url=base_url,
                params=params,
                timeout=timeout,
                max_retries=max_retries,
            )
        except Exception as e:
            logging.error("Error al consultar la API: %s", e)
            raise

        logging.info("Página %s descargada: %s filas", page_number, len(rows))

        if not rows:
            break

        df_page = pd.DataFrame(rows)
        df_page = normalize_dataframe(df_page)

        logging.info("Columnas descargadas en página %s: %s", page_number, df_page.columns.tolist())

        saved_path = save_bronze_page(df_page, run_dir, file_prefix, page_number)
        saved_files.append(saved_path)

        total_rows += len(df_page)

        if len(rows) < page_size:
            break

        offset += page_size
        page_number += 1
        time.sleep(sleep_seconds)

    return total_rows, run_dir, saved_files


def load_run_parquets(files: list[Path]) -> pd.DataFrame:
    if not files:
        return pd.DataFrame()

    dfs = [pd.read_parquet(file) for file in files]
    return pd.concat(dfs, ignore_index=True)


def log_run_summary(df: pd.DataFrame, config: dict) -> None:
    if df.empty:
        logging.info("No se descargaron filas.")
        return

    logging.info("Filas totales descargadas: %s", len(df))
    logging.info("Columnas descargadas: %s", df.columns.tolist())

    incremental_column = config.get("incremental", {}).get("column")
    if incremental_column and incremental_column.lower() in df.columns:
        col = incremental_column.lower()
        parsed = pd.to_datetime(df[col], errors="coerce", utc=True)
        logging.info("Valor mínimo incremental: %s", parsed.min())
        logging.info("Valor máximo incremental: %s", parsed.max())


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    setup_logging(LOG_DIR, "victimas_etl.log")
    logging.info("Iniciando extracción de víctimas")

    config = load_source_config()
    state = load_state()

    extraction_mode = get_extraction_mode(config)
    logging.info("Modo de extracción: %s", extraction_mode)
    logging.info("Último valor incremental en state.yaml: %s", state.get("last_incremental_value"))

    total_rows, run_dir, files = fetch_and_save_pages(config, state)

    if total_rows == 0:
        logging.info("No hubo datos nuevos para descargar.")

        if extraction_mode == "incremental":
            update_state(
                incremental_value=state.get("last_incremental_value"),
                incremental_column=config["incremental"].get("column"),
                row_count=0,
                extraction_mode=extraction_mode,
            )
        return

    df_run = load_run_parquets(files)
    log_run_summary(df_run, config)

    if extraction_mode == "incremental":
        max_incremental_value = get_incremental_max_value(df_run, config)

        update_state(
            incremental_value=max_incremental_value,
            incremental_column=config["incremental"].get("column"),
            row_count=total_rows,
            extraction_mode=extraction_mode,
        )
    else:
        logging.info("Full refresh: no se actualiza checkpoint incremental.")

    logging.info("Extracción finalizada.")
    logging.info("Directorio de la corrida: %s", run_dir)


if __name__ == "__main__":
    main()