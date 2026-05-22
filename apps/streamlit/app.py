"""
app.py — Observatorio de Seguridad Alimentaria de Antioquia
Ejecutar con: uv run streamlit run apps/streamlit/app.py
"""

import streamlit as st
from components.sidebar import render_sidebar
from components.map_view import render_map
from components.info_panel import render_info_panel, render_detail_panel

st.set_page_config(
    page_title = "Observatorio Seguridad Alimentaria ",
    page_icon  = "🌽",
    layout     = "wide",
    initial_sidebar_state = "expanded",
)


st.markdown("""
<style>
    #MainMenu { visibility: hidden; }
    footer     { visibility: hidden; }
    .block-container {
        padding-top:    2.0rem !important;
        padding-bottom: 0rem   !important;
        padding-left:   0.5rem !important;
        padding-right:  0.5rem !important;
    }
    iframe {
        display: block;
    }

    /* Sidebar */
    [data-testid="stSidebar"] { background-color: #e8e8ea; !important;}
    [data-testid="stSidebar"] *          { color: #000000 !important; }
    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3         { color: #000000 !important; }
    [data-testid="stSidebar"] label      { color: #c9d1d9 !important; }
    [data-testid="stSidebar"] .stCaption { color: #8b949e !important; }
    

    /* Panel derecho: scroll si el contenido es largo */
    [data-testid="column"]:last-child {
        overflow-y: auto;
        max-height: 620px;
    }


</style>
""", unsafe_allow_html=True)

# ── Estado inicial ────────────────────────────────────────────
defaults = {
    "active_layers":             ["food_insecurity"],
    "selected_year":             None,
    "clicked_coords":            None,
    "selected_data":             None,
    "selected_data_key":         None,
    "dept_filter":               (),
    "active_exclusive_category": None,
    "clicked_muni_coords":       None,
    "clicked_muni_id":           None,
    "clicked_muni_name":         None,
    "panel_b_data":              None,
    "panel_b_key":               None,
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

with st.sidebar:
    render_sidebar()


has_panel_b = st.session_state.get("clicked_muni_coords") is not None

if has_panel_b:
    col_map, col_panel = st.columns([3, 1])
    with col_map:
        render_map()
    with col_panel:
        render_detail_panel()

else:
    render_map()