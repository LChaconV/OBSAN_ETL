"""
components/sidebar.py — Panel lateral del Observatorio
Categorías exclusivas: solo una activa a la vez (excepto
seguridad_alimentaria y contexto que son siempre activables).
"""

import streamlit as st
from config.layers_config import LAYER_TREE, MAP_CONFIG, CATEGORIES
from core.layer import GeoLayer, ChoroplethLayer, BubbleLayer
from core.layer_group import LayerGroup
from core.db import test_connection, query_rows


# ─── Categorías que no bloquean a las demás ──────────────────
NON_EXCLUSIVE = {"seguridad_alimentaria", "contexto"}


def render_sidebar():
    _render_header()
    st.markdown("---")
    _render_year_filter()
    st.markdown("---")
    _render_dept_filter()
    st.markdown("---")
    _render_categories()
    st.markdown("---")
    _render_basemap_selector()
    st.markdown("---")
    _render_footer()


def _render_header():
    st.markdown("### 🌽 Observatorio")
    st.markdown("**Seguridad Alimentaria de Antioquia**")

    if "db_status" not in st.session_state:
        ok, msg = test_connection()
        st.session_state.db_status = (ok, msg)

    ok, msg = st.session_state.db_status
    st.markdown("🟢" if ok else "🔴",
                help=f"{'Conectado' if ok else 'Sin conexión'} — {msg}")


def _render_year_filter():
    st.markdown("**📅 Año**")

    if "available_years" not in st.session_state:
        rows = query_rows(
            "SELECT DISTINCT year FROM perfil_antioquia ORDER BY year DESC"
        )
        st.session_state.available_years = [r["year"] for r in rows] if rows else []

    years = st.session_state.available_years
    if not years:
        st.warning("No se encontraron años.")
        st.session_state.selected_year = None
        return

    selected = st.selectbox(
        label            = "Año",
        options          = years,
        index            = 0,
        label_visibility = "collapsed",
    )
    if st.session_state.get("selected_year") != selected:
        st.session_state.selected_year = selected
        st.cache_data.clear()


def _render_dept_filter():
    st.markdown("**🏛️ Departamento**")

    if "available_depts" not in st.session_state:
        rows = query_rows("""
            SELECT DISTINCT id_dept, name_dept
            FROM dim_divipola
            WHERE id_dept IS NOT NULL
            ORDER BY name_dept
        """)
        st.session_state.available_depts = rows or []

    depts   = st.session_state.available_depts
    options = {d["id_dept"]: d["name_dept"] for d in depts}

    selected = st.multiselect(
        label            = "Departamento",
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


def _render_categories():
    """
    Renderiza cada categoría como un expander con checkboxes.
    Aplica la lógica de exclusividad: si el usuario activa una capa
    de una categoría exclusiva, desactiva la categoría exclusiva anterior.
    """
    st.markdown("**🗂️ Capas**")

    all_layers = LAYER_TREE.all_layers()

    # Agrupar capas por categoría
    layers_by_cat: dict[str, list] = {}
    for layer in all_layers:
        cat = layer.category or "contexto"
        if cat not in layers_by_cat:
            layers_by_cat[cat] = []
        layers_by_cat[cat].append(layer)

    # Categoría exclusiva actualmente activa
    active_exclusive = st.session_state.get("active_exclusive_category", None)

    for cat_id, cat_cfg in CATEGORIES.items():
        layers_in_cat = layers_by_cat.get(cat_id, [])
        if not layers_in_cat:
            continue

        # Contar capas activas en esta categoría
        active_in_cat = sum(
            1 for l in layers_in_cat
            if l.id in st.session_state.get("active_layers", [])
        )

        label = f"{cat_cfg['icon']} {cat_cfg['label']}"
        if active_in_cat > 0:
            label += f" ({active_in_cat})"

        # Categorías exclusivas bloqueadas aparecen deshabilitadas
        is_blocked = (
            cat_cfg["exclusive"]
            and active_exclusive is not None
            and active_exclusive != cat_id
        )

        with st.expander(label, expanded=(active_in_cat > 0)):
            if is_blocked:
                st.caption(
                    f"⚠️ Desactiva **{CATEGORIES[active_exclusive]['label']}** "
                    f"para activar esta categoría."
                )
                continue

            for layer in layers_in_cat:
                _render_layer_checkbox(layer, cat_id, cat_cfg)


def _render_layer_checkbox(layer: GeoLayer, cat_id: str, cat_cfg: dict):
    """Checkbox de una capa con lógica de exclusividad."""
    is_active = layer.id in st.session_state.get("active_layers", [])

    # Swatch de color
    col_color, col_check = st.columns([1, 9])
    with col_color:
        if hasattr(layer, "color_high"):
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
            key   = f"chk_{layer.id}",
            help  = layer.description or None,
        )

    if checked and not is_active:
        _activate_layer(layer.id, cat_id, cat_cfg)

    elif not checked and is_active:
        _deactivate_layer(layer.id, cat_id)


def _activate_layer(layer_id: str, cat_id: str, cat_cfg: dict):
    """
    Activa una capa. Si la categoría es exclusiva y hay otra
    categoría exclusiva activa, la desactiva primero y notifica.
    """
    active_exclusive = st.session_state.get("active_exclusive_category")
    active_layers    = st.session_state.get("active_layers", [])

    if cat_cfg["exclusive"] and active_exclusive and active_exclusive != cat_id:
        # Desactivar todas las capas de la categoría anterior
        all_layers = LAYER_TREE.all_layers()
        prev_layers = [
            l.id for l in all_layers
            if l.category == active_exclusive
        ]
        active_layers = [l for l in active_layers if l not in prev_layers]

        # Notificar al usuario
        prev_label = CATEGORIES[active_exclusive]["label"]
        new_label  = CATEGORIES[cat_id]["label"]
        st.toast(
            f"📂 Categoría cambiada: **{prev_label}** → **{new_label}**",
            icon="🔄",
        )

    # Activar la nueva capa
    if layer_id not in active_layers:
        active_layers.append(layer_id)

    st.session_state.active_layers = active_layers

    if cat_cfg["exclusive"]:
        st.session_state.active_exclusive_category = cat_id

    st.rerun()


def _deactivate_layer(layer_id: str, cat_id: str):
    """
    Desactiva una capa. Si era la última de su categoría exclusiva,
    limpia la categoría activa.
    """
    active_layers = st.session_state.get("active_layers", [])
    active_layers = [l for l in active_layers if l != layer_id]
    st.session_state.active_layers = active_layers

    # Si no quedan capas activas de esta categoría, liberar exclusividad
    all_layers      = LAYER_TREE.all_layers()
    remaining       = [
        l for l in all_layers
        if l.category == cat_id and l.id in active_layers
    ]
    if not remaining and cat_id == st.session_state.get("active_exclusive_category"):
        st.session_state.active_exclusive_category = None

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


def _render_footer():
    n     = len(st.session_state.get("active_layers", []))
    total = len(LAYER_TREE.all_layers())
    st.caption(f"{n} de {total} capas activas")
    if n > 0:
        if st.button("🗑️ Limpiar capas", use_container_width=True):
            st.session_state.active_layers              = []
            st.session_state.active_exclusive_category  = None
            st.rerun()