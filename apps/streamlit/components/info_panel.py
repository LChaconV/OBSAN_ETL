"""
components/info_panel.py — Paneles de detalle del Observatorio

Panel A: datos de seguridad alimentaria (perfil_antioquia)
         se abre al hacer clic en una subregión

Panel B: datos de la categoría activa para un municipio
         se abre al hacer clic en cualquier capa activa
"""

import streamlit as st
from core.db import get_subregion_at_point, CATEGORY_QUERY_MAP
from config.layers_config import CATEGORIES


# ─────────────────────────────────────────────────────────────
#  COLORES
# ─────────────────────────────────────────────────────────────
COLOR_INSECURITY = "#cb181d"
COLOR_SECURITY   = "#2ca25f"
COLOR_NEUTRAL    = "#6baed6"
COLOR_WARN       = "#fd8d3c"


# ─────────────────────────────────────────────────────────────
#  UTILIDADES COMPARTIDAS
# ─────────────────────────────────────────────────────────────

def _fmt(value, decimals: int = 1, unit: str = "%") -> str:
    """
    Formatea un valor numérico.
    Por defecto retorna porcentaje: _fmt(38.4) → "38.4%"
    Sin unidad:                     _fmt(1200, unit="") → "1,200.0"
    Con unidad:                     _fmt(1200, unit="bls") → "1,200.0 bls"
    """
    if value is None:
        return "—"
    try:
        formatted = f"{float(value):,.{decimals}f}"
        return f"{formatted} {unit}".strip() if unit else formatted
    except Exception:
        return str(value)


def _section(title: str):
    """Título de sección dentro de un panel."""
    st.markdown(
        f"<div style='font-size:13px;font-weight:700;color:#444;"
        f"margin:10px 0 6px 0;'>{title}</div>",
        unsafe_allow_html=True,
    )


def _section_header(title: str):
    """Alias de _section para compatibilidad con Panel A."""
    _section(title)


def _kv(label: str, value, unit: str = ""):
    """
    Fila clave-valor para Panel B.
    Muestra etiqueta a la izquierda y valor a la derecha.
    """
    st.markdown(
        f"<div style='display:flex;justify-content:space-between;"
        f"font-size:12px;padding:2px 0;border-bottom:1px solid #f0f0f0;'>"
        f"<span style='color:#555;'>{label}</span>"
        f"<span style='font-weight:600;color:#222;'>{_fmt(value, unit=unit)}</span>"
        f"</div>",
        unsafe_allow_html=True,
    )


def _indicator_bar(label: str, value, color: str, bold: bool = False):
    """Fila con barra de progreso proporcional al valor (Panel A)."""
    if value is None:
        return
    pct    = min(float(value), 100)
    weight = "700" if bold else "500"
    st.markdown(
        f"""
        <div style="margin-bottom:8px;">
            <div style="display:flex;justify-content:space-between;
                        font-size:12px;font-weight:{weight};
                        color:#333;margin-bottom:3px;">
                <span>{label}</span>
                <span style="color:{color};">{pct:.1f}%</span>
            </div>
            <div style="background:#f0f0f0;border-radius:4px;height:7px;">
                <div style="width:{pct}%;background:{color};
                            border-radius:4px;height:7px;"></div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _indicator_row(label: str, value, indent: bool = False, color: str = "#555"):
    """Fila simple etiqueta-valor con porcentaje (Panel A)."""
    if value is None:
        return
    pad = "padding-left:14px;" if indent else ""
    st.markdown(
        f"""
        <div style="display:flex;justify-content:space-between;
                    font-size:12px;color:#555;padding:2px 0;{pad}">
            <span>{label}</span>
            <span style="color:{color};font-weight:600;">{_fmt(value)}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ═════════════════════════════════════════════════════════════
#  PANEL A — Seguridad alimentaria (perfil_antioquia)
# ═════════════════════════════════════════════════════════════

def _close_panel_a():
    st.session_state.clicked_coords    = None
    st.session_state.selected_data     = None
    st.session_state.selected_data_key = None

def _hide_panel_a():
    st.session_state.panel_hidden = True

def render_info_panel():
    """Panel A — indicadores de perfil_antioquia para la subregión clickeada."""

    coords = st.session_state.get("clicked_coords")
    year   = st.session_state.get("selected_year")

    if not coords:
        st.markdown(
            """
            <div style="padding:20px 12px;text-align:center;
                        color:#888;font-size:13px;line-height:1.6;">
                <div style="font-size:28px;margin-bottom:10px;">🗺️</div>
                Haz clic sobre una subregión del mapa para ver sus indicadores
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    if not year:
        st.warning("Selecciona un año en el panel izquierdo.")
        return

    # ── Consulta espacial ─────────────────────────────────────
    cache_key = f"panel_a_{coords}_{year}"
    if st.session_state.get("selected_data_key") != cache_key:
        with st.spinner("Consultando..."):
            data = get_subregion_at_point(coords[0], coords[1], year)
        st.session_state.selected_data     = data
        st.session_state.selected_data_key = cache_key
    else:
        data = st.session_state.get("selected_data")

    if not data:
        st.markdown(
            """
            <div style="padding:16px;text-align:center;color:#888;font-size:13px;">
                No se encontraron datos para este punto.<br>
                Haz clic dentro de una subregión.
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.button(
            "✖ Cerrar panel",
            key      = "close_panel_a",
            on_click = _close_panel_a,
            use_container_width = True,
        )
        return

    # ── Encabezado ────────────────────────────────────────────
    nombre = data.get("name_subregion", "—")
    year_v = data.get("year", year)

    st.markdown(
        f"""
        <div style="background:#1a1f2e;color:white;padding:14px 16px;
                    border-radius:10px;margin-bottom:12px;">
            <div style="font-size:11px;color:#8b949e;text-transform:uppercase;
                        letter-spacing:1px;margin-bottom:4px;">Subregión</div>
            <div style="font-size:17px;font-weight:700;line-height:1.2;">{nombre}</div>
            <div style="font-size:12px;color:#58a6ff;margin-top:4px;">Año {year_v}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Sección 1: Seguridad Alimentaria ─────────────────────
    _section_header("🍽️ Seguridad Alimentaria")

    _indicator_bar(
        label = "Inseguridad alimentaria",
        value = data.get("pct_u18_food_insecurity"),
        color = COLOR_INSECURITY,
        bold  = True,
    )
    _indicator_row("↳ Leve",     data.get("pct_u18_food_insecurity_mild"),     indent=True)
    _indicator_row("↳ Moderada", data.get("pct_u18_food_insecurity_moderate"), indent=True)
    _indicator_row("↳ Severa",   data.get("pct_u18_food_insecurity_severe"),   indent=True, color=COLOR_INSECURITY)
    _indicator_bar(
        label = "Seguridad alimentaria",
        value = data.get("pct_u18_food_security"),
        color = COLOR_SECURITY,
    )

    st.markdown("<hr style='margin:12px 0;border-color:#eee;'>", unsafe_allow_html=True)

    # ── Sección 2: Nutrición <5 años ─────────────────────────
    _section_header("👶 Nutrición menores de 5 años")

    _indicator_row("Desnutrición aguda severa",   data.get("pct_u5_wasting_severe"),    color=COLOR_INSECURITY)
    _indicator_row("Desnutrición aguda moderada", data.get("pct_u5_wasting_moderate"),  color=COLOR_WARN)
    _indicator_row("Riesgo desnutrición aguda",   data.get("pct_u5_wasting_risk"))
    _indicator_row("Estado normal",               data.get("pct_u5_wasting_normal"),    color=COLOR_SECURITY)
    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
    _indicator_row("Bajo peso",                   data.get("pct_u5_underweight"),       color=COLOR_WARN)
    _indicator_row("Riesgo bajo peso",            data.get("pct_u5_underweight_risk"))
    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
    _indicator_row("Retraso en talla",            data.get("pct_u5_stunting"),          color=COLOR_WARN)
    _indicator_row("Riesgo retraso en talla",     data.get("pct_u5_stunting_risk"))
    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
    _indicator_row("Sobrepeso",                   data.get("pct_u5_overweight"),        color=COLOR_NEUTRAL)
    _indicator_row("Riesgo sobrepeso",            data.get("pct_u5_overweight_risk"))
    _indicator_row("Obesidad",                    data.get("pct_u5_obesity"),           color=COLOR_NEUTRAL)

    st.markdown("<hr style='margin:12px 0;border-color:#eee;'>", unsafe_allow_html=True)

    # ── Sección 3: Nutrición 5-18 años ───────────────────────
    _section_header("🧒 Nutrición 5 a 18 años")

    _indicator_row("Retraso en talla",     data.get("pct_5_18_stunting"),        color=COLOR_WARN)
    _indicator_row("Riesgo retraso talla", data.get("pct_5_18_stunting_risk"))
    _indicator_row("Talla normal",         data.get("pct_5_18_stunting_normal"), color=COLOR_SECURITY)
    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
    _indicator_row("Delgadez (riesgo)",    data.get("pct_5_18_thinness_risk"),   color=COLOR_WARN)
    _indicator_row("IMC normal",           data.get("pct_5_18_bmi_normal"),      color=COLOR_SECURITY)
    _indicator_row("Sobrepeso",            data.get("pct_5_18_overweight"),      color=COLOR_NEUTRAL)
    _indicator_row("Obesidad",             data.get("pct_5_18_obesity"),         color=COLOR_NEUTRAL)

    # ── Botón cerrar ─────────────────────────────────────────
    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
    st.button(
        "⟵ Ocultar",
        key      = "hide_panel_a",
        on_click = _hide_panel_a,
        use_container_width = True,
    )


# ═════════════════════════════════════════════════════════════
#  PANEL B — Variables activas por categoría
# ═════════════════════════════════════════════════════════════

def _close_panel_b():
    st.session_state.clicked_muni_coords = None
    st.session_state.clicked_muni_id     = None
    st.session_state.clicked_muni_name   = None
    st.session_state.panel_b_data        = None
    st.session_state.panel_b_key         = None

def _hide_panel_b():
    st.session_state.panel_hidden = True

def _close_button_b():
    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
    st.button(
        "⟵ Ocultar",
        key      = "hide_panel_b",
        on_click = _hide_panel_b,
        use_container_width = True,
    )


def render_detail_panel():
    """Panel B — todos los indicadores de la categoría activa para el municipio."""

    coords   = st.session_state.get("clicked_muni_coords")
    year     = st.session_state.get("selected_year")
    cat_id   = st.session_state.get("active_exclusive_category")
    muni_id  = st.session_state.get("clicked_muni_id")

    if not coords or not cat_id or not muni_id:
        return

    cat_cfg  = CATEGORIES.get(cat_id, {})
    query_fn = CATEGORY_QUERY_MAP.get(cat_id)

    # ── Encabezado ────────────────────────────────────────────
    muni_name = st.session_state.get("clicked_muni_name", muni_id)
    st.markdown(
        f"""
        <div style="background:#1a1f2e;color:white;padding:14px 16px;
                    border-radius:10px;margin-bottom:12px;">
            <div style="font-size:11px;color:#8b949e;text-transform:uppercase;
                        letter-spacing:1px;margin-bottom:4px;">Municipio</div>
            <div style="font-size:17px;font-weight:700;">{muni_name}</div>
            <div style="font-size:12px;color:#58a6ff;margin-top:4px;">
                {cat_cfg.get('icon','')} {cat_cfg.get('label','')} · {year or ''}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if query_fn is None:
        st.caption("No hay datos disponibles para esta categoría.")
        #_close_button_b()
        return

    # ── Consultar y cachear datos ─────────────────────────────
    cache_key = f"panel_b_{muni_id}_{cat_id}_{year}"
    if st.session_state.get("panel_b_key") != cache_key:
        with st.spinner("Consultando..."):
            data = query_fn(muni_id, year)
        st.session_state.panel_b_data = data
        st.session_state.panel_b_key  = cache_key
    else:
        data = st.session_state.get("panel_b_data", {})

    if not data:
        st.caption("Sin datos disponibles para este municipio y año.")
        #_close_button_b()
        return

    # ── Renderizar según categoría ────────────────────────────
    if cat_id == "salud":
        _render_salud(data)
    elif cat_id == "socioeconomico":
        _render_socioeconomico(data)
    elif cat_id == "ambiente":
        _render_ambiente(data)
    elif cat_id == "agropecuario":
        _render_agropecuario(data)
    elif cat_id == "conflicto":
        _render_conflicto(data)

    #_close_button_b()


# ─────────────────────────────────────────────────────────────
#  SECCIONES DEL PANEL B
# ─────────────────────────────────────────────────────────────

def _render_salud(data: dict):
    _section("🏥 Salud")
    _kv("Desnutrición aguda <5 años",  data.get("desnutricion_aguda"),       unit="casos")
    _kv("Mortalidad por malnutrición", data.get("mortalidad_malnutricion"),   unit="casos")
    _kv("Bajo peso al nacer",          data.get("bajo_peso_nacer"),           unit="casos")


def _render_socioeconomico(data: dict):
    _section("📊 Condiciones Socioeconómicas")
    _kv("Pobreza monetaria",  data.get("pobreza_monetaria"))   # viene en %
    _kv("Población empleada", data.get("poblacion_empleada"),  unit="personas")
    _kv("Cobertura escolar",  data.get("cobertura_escolar"),   unit="estudiantes")

    edu = data.get("educacion_superior", {})
    if edu and any(v for v in edu.values() if v):
        _section("📚 Educación Superior")
        _kv("Técnico profesional", edu.get("prof_technician"), unit="")
        _kv("Tecnólogo",           edu.get("technologist"),    unit="")
        _kv("Universitario",       edu.get("university"),      unit="")
        _kv("Especialización",     edu.get("specialization"),  unit="")
        _kv("Maestría",            edu.get("master"),          unit="")
        _kv("Doctorado",           edu.get("doctorate"),       unit="")


def _render_ambiente(data: dict):
    irca = data.get("irca", {}).get("irca_value")
    if irca is not None:
        _section("💧 Calidad del Agua")
        _kv("IRCA", irca)

    _section("🛢️ Hidrocarburos")
    _kv("Producción petróleo", data.get("petroleo", {}).get("produccion"), unit="bls")
    _kv("Producción gas",      data.get("gas", {}).get("produccion"),      unit="")
    _kv("Regalías",            data.get("regalias", {}).get("total"),      unit="COP")

    minerales = data.get("minerales", [])
    if minerales:
        _section("⛏️ Regalías por mineral")
        for m in minerales:
            _kv(m.get("mineral_resource", "—"), m.get("total"), unit="COP")

    cultivos = data.get("cultivos", [])
    if cultivos:
        _section("🌿 Cultivos ilícitos erradicados")
        for c in cultivos:
            _kv(c.get("id_illicit_crop", "—"), c.get("total"), unit="ha")

    clima = data.get("clima", [])
    if clima:
        _section("🌦️ Clima")
        for c in clima:
            _kv(
                f"{c.get('variable','')} ({c.get('annual_aggregation','')})",
                c.get("value"),
                unit="",
            )


def _render_agropecuario(data: dict):
    pecuario = data.get("pecuario", [])
    if pecuario:
        _section("🐄 Censo Pecuario")
        for p in pecuario:
            animales = _fmt(p.get("total_animals"), unit="animales")
            fincas   = _fmt(p.get("total_farms"),   unit="fincas")
            _kv(p.get("type", "—"), f"{animales} · {fincas}")

    mercados = data.get("mercados", [])
    if mercados:
        _section("🛒 Mercados Campesinos")
        for m in mercados:
            st.markdown(
                f"<div style='font-size:12px;padding:2px 0;'>"
                f"📍 {m.get('name','—')}</div>",
                unsafe_allow_html=True,
            )


def _render_conflicto(data: dict):
    victimas = data.get("victimas", [])
    if victimas:
        _section("🕊️ Víctimas por tipo de evento")
        for v in victimas:
            _kv(v.get("event_name", "—"), v.get("total"), unit="personas")

    iraca = data.get("iraca", [])
    if iraca:
        _section("👥 Beneficiarios IRACA")
        for i in iraca:
            label = f"{i.get('type','—')} · {i.get('status','—')}"
            _kv(label, i.get("total"), unit="personas")