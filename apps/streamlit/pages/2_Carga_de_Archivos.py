"""
pages/2_Carga_de_Archivos.py — Página de carga de archivos

"""

import streamlit as st
from styles import apply_global_styles
from upload.ui import render_upload_page

st.set_page_config(
    page_title = "Carga de archivos — Observatorio",
    page_icon  = "📂",
    layout     = "wide",
)

apply_global_styles(compact_top=True)

render_upload_page()
