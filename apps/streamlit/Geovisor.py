"""
Geovisor.py — Observatorio de Seguridad Alimentaria de Antioquia
Ejecutar con: uv run streamlit run apps/streamlit/Geovisor.py
"""

import streamlit as st
from components.sidebar import render_sidebar, show_layer_warning_if_needed
from components.map_view import render_map
from styles import apply_global_styles

st.set_page_config(
    page_title = "Observatorio Seguridad Alimentaria ",
    page_icon  = "🌽",
    layout     = "wide",
    initial_sidebar_state = "expanded",
)

apply_global_styles(map_layout=True)

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
    "clicked_panel_cat":         None,
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

with st.sidebar:
    render_sidebar()

show_layer_warning_if_needed()
render_map()
