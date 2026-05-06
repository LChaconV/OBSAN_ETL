from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.config_utils import load_yaml
from src.etl.utils.sivigila_transform_utils import transform_manual, build_fact_table_golden, save_fact_golden_parquet
# ============================================================
# RUTAS
# ============================================================
def run() -> None:
    PROJECT_ROOT = Path(__file__).resolve().parents[3]

    if str(PROJECT_ROOT) not in sys.path:
        sys.path.append(str(PROJECT_ROOT))

    CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "bajo_peso_nacer_transform.yaml"
    LOG_DIR = PROJECT_ROOT / "logs"
    df, config,run_name = transform_manual("bajo_peso_nacer_transform", LOG_DIR, CONFIG_PATH)
    print(df.head())
    print(df.dtypes)
    
    df["year"]= df["date_event"].dt.year
    df = build_fact_table_golden(df, config)
    save_fact_golden_parquet(df, run_name, PROJECT_ROOT / config["source"]["golden_fact_dir"], config)

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logging.exception("Error en la ejecución del proceso de transformación: %s", e)                 