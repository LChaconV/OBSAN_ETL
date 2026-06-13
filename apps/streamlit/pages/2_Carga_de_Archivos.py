"""
pages/2_Carga_de_Archivos.py — Página de carga de archivos

"""

import sys
from pathlib import Path

import streamlit as st

STREAMLIT_ROOT = Path(__file__).resolve().parents[1]
if str(STREAMLIT_ROOT) not in sys.path:
    sys.path.insert(0, str(STREAMLIT_ROOT))

from styles import apply_global_styles
from upload.ui import render_upload_page

st.set_page_config(
    page_title = "Carga de archivos — Observatorio",
    page_icon  = "📂",
    layout     = "wide",
)

apply_global_styles(compact_top=True)

render_upload_page()
