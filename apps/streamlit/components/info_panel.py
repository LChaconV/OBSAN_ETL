

import streamlit as st
from core.db import get_subregion_at_point


# Colores para barras de progreso por categoría
COLOR_INSECURITY = "#cb181d"   # rojo
COLOR_SECURITY   = "#2ca25f"   # verde
COLOR_NEUTRAL    = "#6baed6"   # azul suave
COLOR_WARN       = "#fd8d3c"   # naranja


def render_info_panel():
    coords = st.session_state.get("clicked_coords")
    year   = st.session_state.get("selected_year")

    # ── Sin clic: mensaje de ayuda ────────────────────────────
    if not coords:
        st.markdown(
            """
            <div style="
                padding: 20px 12px;
                text-align: center;
                color: #888;
                font-size: 13px;
                line-height: 1.6;
            ">
                <div style="font-size:28px; margin-bottom:10px;">🗺️</div>
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

    cache_key = f"data_{coords}_{year}"
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
        return

    # ── Encabezado ────────────────────────────────────────────
    nombre = data.get("name_subregion", "—")
    year_v = data.get("year", year)

    st.markdown(
        f"""
        <div style="
            background: #1a1f2e;
            color: white;
            padding: 14px 16px;
            border-radius: 10px;
            margin-bottom: 12px;
        ">
            <div style="font-size:11px; color:#8b949e; text-transform:uppercase;
                        letter-spacing:1px; margin-bottom:4px;">Subregión</div>
            <div style="font-size:17px; font-weight:700; line-height:1.2;">{nombre}</div>
            <div style="font-size:12px; color:#58a6ff; margin-top:4px;">Año {year_v}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Sección 1: Seguridad Alimentaria ─────────────────────
    _section_header("🍽️ Seguridad Alimentaria")

    insec_total = data.get("pct_u18_food_insecurity")
    sec_total   = data.get("pct_u18_food_security")

    # Barra principal de inseguridad
    _indicator_bar(
        label    = "Inseguridad alimentaria",
        value    = insec_total,
        color    = COLOR_INSECURITY,
        bold     = True,
    )

    # Desglose por severidad (indentado)
    _indicator_row("↳ Leve",     data.get("pct_u18_food_insecurity_mild"),     indent=True)
    _indicator_row("↳ Moderada", data.get("pct_u18_food_insecurity_moderate"), indent=True)
    _indicator_row("↳ Severa",   data.get("pct_u18_food_insecurity_severe"),   indent=True, color=COLOR_INSECURITY)

    _indicator_bar(
        label = "Seguridad alimentaria",
        value = sec_total,
        color = COLOR_SECURITY,
    )

    st.markdown("<hr style='margin:12px 0;border-color:#eee;'>", unsafe_allow_html=True)

    # ── Sección 2: Nutrición <5 años ─────────────────────────
    _section_header("👶 Nutrición menores de 5 años")

    _indicator_row("Desnutrición aguda severa",  data.get("pct_u5_wasting_severe"),    color=COLOR_INSECURITY)
    _indicator_row("Desnutrición aguda moderada",data.get("pct_u5_wasting_moderate"),  color=COLOR_WARN)
    _indicator_row("Riesgo desnutrición aguda",  data.get("pct_u5_wasting_risk"))
    _indicator_row("Estado normal",              data.get("pct_u5_wasting_normal"),    color=COLOR_SECURITY)
    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
    _indicator_row("Bajo peso",                  data.get("pct_u5_underweight"),       color=COLOR_WARN)
    _indicator_row("Riesgo bajo peso",           data.get("pct_u5_underweight_risk"))
    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
    _indicator_row("Retraso en talla",           data.get("pct_u5_stunting"),          color=COLOR_WARN)
    _indicator_row("Riesgo retraso en talla",    data.get("pct_u5_stunting_risk"))
    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
    _indicator_row("Sobrepeso",                  data.get("pct_u5_overweight"),        color=COLOR_NEUTRAL)
    _indicator_row("Riesgo sobrepeso",           data.get("pct_u5_overweight_risk"))
    _indicator_row("Obesidad",                   data.get("pct_u5_obesity"),           color=COLOR_NEUTRAL)

    st.markdown("<hr style='margin:12px 0;border-color:#eee;'>", unsafe_allow_html=True)

    # ── Sección 3: Nutrición 5-18 años ───────────────────────
    _section_header("🧒 Nutrición 5 a 18 años")

    _indicator_row("Retraso en talla",     data.get("pct_5_18_stunting"),       color=COLOR_WARN)
    _indicator_row("Riesgo retraso talla", data.get("pct_5_18_stunting_risk"))
    _indicator_row("Talla normal",         data.get("pct_5_18_stunting_normal"),color=COLOR_SECURITY)
    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
    _indicator_row("Delgadez (riesgo)",    data.get("pct_5_18_thinness_risk"),  color=COLOR_WARN)
    _indicator_row("IMC normal",           data.get("pct_5_18_bmi_normal"),     color=COLOR_SECURITY)
    _indicator_row("Sobrepeso",            data.get("pct_5_18_overweight"),     color=COLOR_NEUTRAL)
    _indicator_row("Obesidad",             data.get("pct_5_18_obesity"),        color=COLOR_NEUTRAL)

    # ── Botón cerrar ─────────────────────────────────────────
    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
    if st.button("✖ Cerrar panel", use_container_width=True):
        st.session_state.clicked_coords    = None
        st.session_state.selected_data     = None
        st.session_state.selected_data_key = None
        st.rerun()


# ─────────────────────────────────────────────────────────────
#  COMPONENTES DE UI
# ─────────────────────────────────────────────────────────────

def _section_header(title: str):
    st.markdown(
        f"<div style='font-size:13px;font-weight:700;color:#444;"
        f"margin-bottom:8px;'>{title}</div>",
        unsafe_allow_html=True,
    )


def _fmt(value) -> str:
    """Formatea un valor numérico como porcentaje."""
    if value is None:
        return "—"
    return f"{float(value):.1f}%"


def _indicator_bar(label: str, value, color: str, bold: bool = False):
    """Fila con barra de progreso proporcional al valor."""
    if value is None:
        return
    pct = min(float(value), 100)
    weight = "700" if bold else "500"
    st.markdown(
        f"""
        <div style="margin-bottom:8px;">
            <div style="display:flex;justify-content:space-between;
                        font-size:12px;font-weight:{weight};color:#333;margin-bottom:3px;">
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
    """Fila simple: etiqueta a la izquierda, valor a la derecha."""
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