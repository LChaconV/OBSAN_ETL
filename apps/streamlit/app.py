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
    .block-container { padding-top: 0.8rem; padding-bottom: 0; }


    /* Panel derecho: scroll si el contenido es largo */
    [data-testid="column"]:last-child {
        overflow-y: auto;
        max-height: 620px;
    }


</style>
""", unsafe_allow_html=True)

# ── Estado inicial ────────────────────────────────────────────
defaults = {
    "active_layers":               ["food_insecurity"],
    "selected_year":               None,
    "clicked_coords":              None,
    "selected_data":               None,
    "selected_data_key":           None,
    "dept_filter":                 (),
    "active_exclusive_category":   None,   
    "clicked_muni_coords":         None,    
    "clicked_muni_data_key":       None,   
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

st.markdown(
    
    "##Observatorio de Seguridad Alimentaria"
    "<span style='font-size:14px;color:#888;'>Vista subregional</span>",
    unsafe_allow_html=True,
)

with st.sidebar:
    render_sidebar()

has_panel_a = st.session_state.get("clicked_coords") is not None
has_panel_b = st.session_state.get("clicked_muni_coords") is not None

if has_panel_b:
    col_map, col_b = st.columns([3, 1])
    with col_map:
        render_map()
    with col_b:
        render_detail_panel()

elif has_panel_a:
    col_map, col_a = st.columns([3, 1])
    with col_map:
        render_map()
    with col_a:
        render_info_panel()

else:
    render_map()