"""
pages/4_Analisis_Temporal.py — Series temporales y correlación entre variables.
"""

import sys
from pathlib import Path

import streamlit as st

STREAMLIT_ROOT = Path(__file__).resolve().parents[1]
if str(STREAMLIT_ROOT) not in sys.path:
    sys.path.insert(0, str(STREAMLIT_ROOT))

from analisis_temporal import render_analisis_temporal
from styles import apply_global_styles

st.set_page_config(
    page_title="Análisis Temporal — Observatorio",
    page_icon="📊",
    layout="wide",
)

apply_global_styles(compact_top=True)

render_analisis_temporal()
