from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.config_utils import load_yaml
from src.etl.utils.sivigila_transform_utils import silver_transform, build_fact_table_golden, save_fact_golden
# ============================================================
# RUTAS
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parents[3]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "bajo_peso_nacer_transform.yaml"
LOG_DIR = PROJECT_ROOT / "logs"
df, config,run_name = silver_transform("bajo_peso_nacer_transform", LOG_DIR, CONFIG_PATH)
df["year"]= df["date_event"].dt.year
build_fact_table_golden(df, config)
save_fact_golden(df, run_name, PROJECT_ROOT / config["source"]["golden_fact_dir"], config)
                       