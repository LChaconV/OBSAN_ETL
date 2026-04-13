from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path

import pandas as pd
import requests
import yaml

from src.etl.utils.logging_utils import setup_logging, log_run_summary
from src.etl.utils.config_utils import load_yaml, save_yaml, load_source_config, load_state, update_state,get_incremental_max_value
from src.etl.utils.extraction_api_utils import get_extraction_mode, get_reference_value, build_where_clause,fetch_and_save_pages,load_run_parquets
from src.etl.utils.request_utils import build_headers, fetch_api_page
from src.etl.utils.extract_utils.file_utils import normalize_dataframe, create_run_directory, save_bronze_page


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
""" load_yaml() 
    save_yaml() 
"""
# ============================================================
# CONFIG Y STATE
# ============================================================
"""
def load_source_config()
load_state()
update_state()
"""

# ============================================================
# LÓGICA INCREMENTAL
# ============================================================
"""
get_extraction_mode()
get_reference_value()
build_where_clause()
"""

# ============================================================
# REQUESTS
# ============================================================
"""
build_headers()
fetch_api_page()
"""

# ============================================================
# BRONZE
# ============================================================
"""
normalize_dataframe()
create_run_directory()
save_bronze_page()
"""

# ============================================================
# CHECKPOINT
# ============================================================
"""
get_incremental_max_value()
"""


# ============================================================
# EXTRACT
# ============================================================


"""
fetch_and_save_pages()
load_run_parquets()
log_run_summary()
"""

# ============================================================
# MAIN
# ============================================================
def main() -> None:
    setup_logging(LOG_DIR, "beneficiarios_familias_en_accion.log")
    logging.info("Iniciando extracción de Beneficiarios de Familias en Acción")

    config = load_source_config("beneficiarios_familias_en_accion", CONFIG_PATH)
    state = load_state("beneficiarios_familias_en_accion", STATE_PATH)


    extraction_mode = get_extraction_mode(config)
    logging.info("Modo de extracción: %s", extraction_mode)
    logging.info("Último valor incremental en state.yaml: %s", state.get("last_incremental_value"))

    total_rows, run_dir, files = fetch_and_save_pages(config, state,PROJECT_ROOT)

    if total_rows == 0:
        logging.info("No hubo datos nuevos para descargar.")

        if extraction_mode == "incremental":
            update_state(
                key="beneficiarios_familias_en_accion",
                incremental_value=state.get("last_incremental_value"),
                incremental_column=config["incremental"].get("column"),
                row_count=0,
                extraction_mode=extraction_mode,
                path_state =STATE_PATH,
            )
        return

    df_run = load_run_parquets(files)
    log_run_summary(df_run, config)

    if extraction_mode == "incremental":
        max_incremental_value = get_incremental_max_value(df_run, config)

        update_state(
            key="beneficiarios_familias_en_accion",
            incremental_value=max_incremental_value,
            incremental_column=config["incremental"].get("column"),
            row_count=total_rows,
            extraction_mode=extraction_mode,
            path_state =STATE_PATH,
        )
    else:
        logging.info("Full refresh: no se actualiza checkpoint incremental.")

    logging.info("Extracción finalizada.")
    logging.info("Directorio de la corrida: %s", run_dir)


if __name__ == "__main__":
    main()