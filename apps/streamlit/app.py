"""
app.py — Observatorio de Seguridad Alimentaria de Antioquia
Ejecutar con: uv run streamlit run apps/streamlit/app.py
"""

import streamlit as st
from components.sidebar import render_sidebar
from components.map_view import render_map
from components.info_panel import render_info_panel

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
    "active_layers":    ["food_insecurity"],
    "selected_year":    None,
    "clicked_coords":   None,
    "selected_data":    None,
    "selected_data_key": None,
    "dept_filter":      (),
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── Encabezado ────────────────────────────────────────────────
st.markdown(
    
    "##Observatorio de Seguridad Alimentaria"
    "<span style='font-size:14px;color:#888;'>Vista subregional</span>",
    unsafe_allow_html=True,
)

# ── Layout ────────────────────────────────────────────────────
with st.sidebar:
    render_sidebar()

# Panel de detalle solo aparece si hay un clic activo
has_selection = st.session_state.get("clicked_coords") is not None

if has_selection:
    col_map, col_info = st.columns([3, 1])
    with col_map:
        render_map()
    with col_info:
        render_info_panel()
else:
    render_map()
