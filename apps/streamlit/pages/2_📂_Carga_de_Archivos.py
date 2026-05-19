"""
pages/2_Carga_de_Archivos.py — Página de carga de archivos

"""

import streamlit as st
from upload.ui import render_upload_page

st.set_page_config(
    page_title = "Carga de archivos — Observatorio",
    page_icon  = "📂",
    layout     = "wide",
)

st.markdown("""
<style>
    #MainMenu { visibility: hidden; }
    footer     { visibility: hidden; }
    .block-container { padding-top: 1rem; }
</style>
""", unsafe_allow_html=True)

render_upload_page()
