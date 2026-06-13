"""
pages/3_Ejecuciones_ETL.py — Ejecución manual de pipelines ETL.
"""

import streamlit as st

from manual_etl import render_manual_etl_page
from styles import apply_global_styles

st.set_page_config(
    page_title="Ejecuciones ETL — Observatorio",
    page_icon="⚙️",
    layout="wide",
)

apply_global_styles(compact_top=True)

render_manual_etl_page()
