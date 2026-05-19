"""
components/sidebar.py — Panel lateral del Observatorio
Incluye: filtro de año, selector de capas, mapa base, estado de conexión.
"""

import streamlit as st
from config.layers_config import LAYER_TREE, MAP_CONFIG
from core.layer import GeoLayer, ChoroplethLayer, BubbleLayer
from core.layer_group import LayerGroup
from core.db import test_connection, query_rows


def render_sidebar():
    _render_header()
    st.markdown("---")
    _render_year_filter()
    st.markdown("---")
    _render_dept_filter()       
    st.markdown("---")
    _render_layer_search()
    st.markdown("---")
    _render_layer_tree(LAYER_TREE)
    st.markdown("---")
    _render_layer_order()
    st.markdown("---")
    _render_basemap_selector()
    st.markdown("---")
    #_render_footer()


# ─────────────────────────────────────────────────────────────

def _render_header():
    """Logo y estado de la conexión."""
    st.markdown("### Observatorio")
    st.markdown("**Seguridad Alimentaria de Antioquia**")

    if "db_status" not in st.session_state:
        ok, msg = test_connection()
        st.session_state.db_status = (ok, msg)

    ok, msg = st.session_state.db_status
    if ok:
        st.success(f"✅ Base de datos conectada", icon=None)
    else:
        st.error(f"❌ Sin conexión: {msg}")


def _render_year_filter():
    """
    Selector de año. Consulta los años disponibles en la BD
    y los muestra como un slider o selectbox.
    """
    st.markdown("**📅 Año**")

    if "available_years" not in st.session_state:
        rows = query_rows("SELECT DISTINCT year FROM perfil_antioquia ORDER BY year DESC")
        st.session_state.available_years = [r["year"] for r in rows] if rows else []

    years = st.session_state.available_years

    if not years:
        st.warning("No se encontraron años en la base de datos.")
        st.session_state.selected_year = None
        return

    selected = st.selectbox(
        label            = "Selecciona el año",
        options          = years,
        index            = 0,
        label_visibility = "collapsed",
    )

    if st.session_state.get("selected_year") != selected:
        st.session_state.selected_year = selected
        st.cache_data.clear()

def _render_dept_filter():
    """
    Multiselect de departamentos. Consulta los disponibles en dim_divipola.
    Las capas con filterable_by_dept=True aplican el filtro automáticamente.
    """
    st.markdown("**🏛️ Departamento**")

    if "available_depts" not in st.session_state:
        rows = query_rows("""
            SELECT DISTINCT id_dept, name_dept
            FROM dim_divipola
            WHERE id_dept IS NOT NULL
            ORDER BY name_dept
        """)
        st.session_state.available_depts = rows or []

    depts = st.session_state.available_depts
    if not depts:
        st.caption("No se encontraron departamentos.")
        st.session_state.dept_filter = ()
        return

    options = {}
    for dept in depts:
        raw_id = dept.get("id_dept")
        if raw_id is None:
            continue

        dept_id = str(raw_id).strip()
        if not dept_id:
            continue

        raw_name = dept.get("name_dept")
        dept_name = str(raw_name).strip() if raw_name is not None else ""
        if not dept_name:
            dept_name = f"Depto {dept_id}"

        options[dept_id] = dept_name

    if not options:
        st.caption("No se encontraron departamentos válidos.")
        st.session_state.dept_filter = ()
        return

    selected    = st.multiselect(
        label            = "Filtrar por departamento",
        options          = list(options.keys()),
        format_func      = lambda k: options[k],
        default          = [],
        label_visibility = "collapsed",
        placeholder      = "Todos los departamentos",
    )

    new_filter = tuple(selected)
    if st.session_state.get("dept_filter") != new_filter:
        st.session_state.dept_filter = new_filter
        st.cache_data.clear()

def _render_layer_search():
    """
    Buscador de capas. Filtra todas las GeoLayer del árbol por nombre
    y permite activarlas directamente desde los resultados.
    """
    st.markdown("**🔍 Buscar capa**")

    query = st.text_input(
        label       = "Buscar capa",
        placeholder = "Escribe el nombre de una capa...",
        label_visibility = "collapsed",
        key         = "layer_search_query",
    )

    if not query:
        return

    # Buscar en todas las capas del árbol (insensible a mayúsculas)
    todas = LAYER_TREE.all_layers()
    q     = query.strip().lower()
    resultados = [l for l in todas if q in l.label.lower()]

    if not resultados:
        st.caption("Sin resultados.")
        return

    st.caption(f"{len(resultados)} capa(s) encontrada(s)")

    for layer in resultados:
        is_active = layer.id in st.session_state.get("active_layers", [])

        col_color, col_check = st.columns([1, 9])

        with col_color:
            if isinstance(layer, ChoroplethLayer):
                st.markdown(
                    f'<div style="width:14px;height:38px;'
                    f'background:linear-gradient(to bottom,{layer.color_high},{layer.color_low});'
                    f'border-radius:3px;margin-top:4px;"></div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f'<div style="width:14px;height:14px;'
                    f'background:{layer.color};'
                    f'border-radius:3px;margin-top:10px;"></div>',
                    unsafe_allow_html=True,
                )

        with col_check:
            checked = st.checkbox(
                label = layer.label,
                value = is_active,
                key   = f"search_chk_{layer.id}",
                help  = layer.description or None,
            )

        if checked and layer.id not in st.session_state.active_layers:
            st.session_state.active_layers.append(layer.id)
            st.session_state[f"chk_{layer.id}"] = True
            st.rerun()
        elif not checked and layer.id in st.session_state.active_layers:
            st.session_state.active_layers.remove(layer.id)
            st.session_state[f"chk_{layer.id}"] = False
            st.rerun()


def _render_layer_tree(node, depth: int = 0):
    """Renderiza el árbol de capas recursivamente."""
    if isinstance(node, LayerGroup) and node.id == "root":
        st.markdown("**🗂️ Capas**")
        for item in node.items:
            _render_layer_tree(item, depth=0)
    elif isinstance(node, LayerGroup):
        with st.expander(f"{node.icon} {node.label}", expanded=node.expanded):
            for item in node.items:
                _render_layer_tree(item, depth + 1)
    elif isinstance(node, GeoLayer):
        _render_layer_checkbox(node)


def _render_layer_checkbox(layer: GeoLayer):
    """Checkbox para activar/desactivar una capa."""
    is_active = layer.id in st.session_state.get("active_layers", [])

    col_color, col_check = st.columns([1, 9])

    with col_color:
        if isinstance(layer, (ChoroplethLayer, BubbleLayer)):
            st.markdown(
                f'<div style="'
                f'  width:14px; height:38px; '
                f'  background: linear-gradient(to bottom, {layer.color_high}, {layer.color_low}); '
                f'  border-radius:3px; margin-top:4px;'
                f'"></div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f'<div style="'
                f'  width:14px; height:14px; '
                f'  background:{layer.color}; '
                f'  border-radius:3px; margin-top:10px;'
                f'"></div>',
                unsafe_allow_html=True,
            )

    with col_check:
        checked = st.checkbox(
            label = layer.label,
            value = is_active,
            key   = f"chk_{layer.id}",
            help  = layer.description or None,
        )

    if checked and layer.id not in st.session_state.active_layers:
        st.session_state.active_layers.append(layer.id)
        st.session_state[f"search_chk_{layer.id}"] = True
        st.rerun()
    elif not checked and layer.id in st.session_state.active_layers:
        st.session_state.active_layers.remove(layer.id)
        st.session_state[f"search_chk_{layer.id}"] = False
        st.rerun()


def _render_layer_order():
    """
    Controla el orden de renderizado de las capas activas en el mapa.
    Solo aparece cuando hay 2 o más capas activas.
    La primera capa de la lista se dibuja encima en el mapa.
    """
    active = st.session_state.get("active_layers", [])

    if len(active) < 2:
        return

    st.markdown("**🔢 Orden de capas**")
    st.caption("La primera capa se dibuja encima")

    for i, layer_id in enumerate(active):
        layer  = LAYER_TREE.find_layer(layer_id)
        label  = layer.label if layer else layer_id

        col_label, col_up, col_down = st.columns([6, 1, 1])

        with col_label:
            st.markdown(
                f'<div style="font-size:12px;padding:6px 0;">'
                f'{"⬆ " if i == 0 else ""}{label}'
                f'</div>',
                unsafe_allow_html=True,
            )

        with col_up:
            if i > 0:
                if st.button("↑", key=f"up_{layer_id}", use_container_width=True):
                    active[i], active[i - 1] = active[i - 1], active[i]
                    st.session_state.active_layers = active
                    st.rerun()

        with col_down:
            if i < len(active) - 1:
                if st.button("↓", key=f"down_{layer_id}", use_container_width=True):
                    active[i], active[i + 1] = active[i + 1], active[i]
                    st.session_state.active_layers = active
                    st.rerun()


def _render_basemap_selector():
    st.markdown("**🗺️ Mapa base**")
    basemaps = list(MAP_CONFIG["basemaps"].keys())
    selected = st.selectbox(
        label            = "Mapa base",
        options          = basemaps,
        index            = basemaps.index(MAP_CONFIG["default_basemap"]),
        label_visibility = "collapsed",
    )
    st.session_state.basemap = selected

"""
def _render_footer():
    n     = len(st.session_state.get("active_layers", []))
    total = len(LAYER_TREE.all_layers())
    st.caption(f"{n} de {total} capas activas")
    if n > 0:
        if st.button("🗑️ Limpiar capas", use_container_width=True):
            st.session_state.active_layers = []
            st.rerun()
"""
