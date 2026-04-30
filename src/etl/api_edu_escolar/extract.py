from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path

import pandas as pd
import requests
import yaml

from src.etl.utils.extraction_api_utils import extract_data



# ============================================================
# RUTAS DEL PROYECTO
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parents[3]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

CONFIG_PATH = PROJECT_ROOT / "config" / "sources.yaml"
STATE_PATH = PROJECT_ROOT / "state" / "state.yaml"
LOG_DIR = PROJECT_ROOT / "logs"
def run(**kwargs):
    return extract_data(
        PROJECT_ROOT=PROJECT_ROOT,
        LOG_DIR=LOG_DIR,
        STATE_PATH=STATE_PATH,
        CONFIG_PATH=CONFIG_PATH,
        key="edu_escolar"
    )

if __name__ == "__main__":
    run()