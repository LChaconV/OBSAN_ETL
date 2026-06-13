"""
pages/3_Ejecuciones_ETL.py — Ejecución manual de pipelines ETL.
"""

import sys
from pathlib import Path

import streamlit as st

STREAMLIT_ROOT = Path(__file__).resolve().parents[1]
if str(STREAMLIT_ROOT) not in sys.path:
    sys.path.insert(0, str(STREAMLIT_ROOT))

from manual_etl import render_manual_etl_page
from styles import apply_global_styles

st.set_page_config(
    page_title="Ejecuciones ETL — Observatorio",
    page_icon="⚙️",
    layout="wide",
)

apply_global_styles(compact_top=True)

render_manual_etl_page()
