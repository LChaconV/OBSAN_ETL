import logging
import time
import pandas as pd
from pathlib import Path
from src.etl.utils.extract_utils.file_utils import normalize_dataframe, create_run_directory, save_bronze_page
from src.etl.utils.request_utils import  fetch_api_page
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

def fetch_and_save_pages(config: dict, state: dict,path_root:Path) -> tuple[int, Path, list[Path]]:
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

    run_dir = create_run_directory(config,path_root)

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
