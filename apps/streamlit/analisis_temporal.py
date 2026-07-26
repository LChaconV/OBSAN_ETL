"""
analisis_temporal.py — Análisis de series temporales y correlación entre variables.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

STREAMLIT_ROOT = Path(__file__).resolve().parent
if str(STREAMLIT_ROOT) not in sys.path:
    sys.path.insert(0, str(STREAMLIT_ROOT))

from config.layers_config import CATEGORIES, LAYER_TREE
from core.db import query_rows
from core.layer import BubbleLayer, ChoroplethLayer, IconScaleLayer

# ─── Paleta categórica (8 colores, orden fijo) ───────────────────────────────
_PALETTE = [
    "#2563eb", "#dc2626", "#16a34a", "#d97706",
    "#7c3aed", "#0891b2", "#be185d", "#65a30d",
]

_QUERYABLE_TYPES = (ChoroplethLayer, BubbleLayer, IconScaleLayer)


# ─── Helpers de capas ────────────────────────────────────────────────────────

def get_queryable_layers() -> list:
    return [
        layer for layer in LAYER_TREE.all_layers()
        if isinstance(layer, _QUERYABLE_TYPES)
        and getattr(layer, "data_table", "")
        and getattr(layer, "value_col", "")
        and getattr(layer, "year_col", "")
    ]


def _mun_col(layer) -> str | None:
    """Columna id_mun del layer, o None si no admite filtro por municipio."""
    if isinstance(layer, BubbleLayer):
        col = getattr(layer, "data_id_col", "")
        return col if col == "id_mun" else None
    if isinstance(layer, IconScaleLayer):
        if getattr(layer, "has_own_geometry", False):
            return None
        col = getattr(layer, "muni_id_col", "")
        return col if col == "id_mun" else None
    if isinstance(layer, ChoroplethLayer):
        col = getattr(layer, "data_id_col", "")
        return col if col == "id_mun" else None
    return None


def _dept_filter_clause(layer, id_dept: str) -> str | None:
    """
    Retorna la cláusula SQL para filtrar por departamento, o None si la capa
    no tiene una columna geográfica compatible con filtro departamental.

    - Capas con data_id_col = "id_dept": filtro directo.
    - Capas con data_id_col / muni_id_col = "id_mun": los primeros 2 dígitos
      del código municipal corresponden al departamento (DIVIPOLA).
    - Capas con geometría propia (has_own_geometry=True) o nivel subregión:
      no se puede filtrar por departamento → retorna None.
    """
    if isinstance(layer, ChoroplethLayer):
        data_id = getattr(layer, "data_id_col", "")
        if data_id == "id_dept":
            return f"id_dept = '{id_dept}'"
        if data_id == "id_mun":
            return f"LEFT(id_mun, 2) = '{id_dept}'"
        return None  # id_subregion u otro

    if isinstance(layer, BubbleLayer):
        data_id = getattr(layer, "data_id_col", "")
        if data_id == "id_mun":
            return f"LEFT(id_mun, 2) = '{id_dept}'"
        return None

    if isinstance(layer, IconScaleLayer):
        if getattr(layer, "has_own_geometry", False):
            return None  # geometría propia, sin id_mun/id_dept
        muni_col = getattr(layer, "muni_id_col", "")
        if muni_col == "id_mun":
            return f"LEFT(id_mun, 2) = '{id_dept}'"
        return None

    return None


def can_filter_by_mun(layer) -> bool:
    return _mun_col(layer) is not None


# ─── Consultas ───────────────────────────────────────────────────────────────

@st.cache_data(ttl=600, show_spinner=False)
def get_municipalities() -> list[dict]:
    rows = query_rows("""
        SELECT id_mun, name_mun || ' (' || id_mun || ')' AS label
        FROM dim_divipola
        WHERE id_mun IS NOT NULL AND name_mun IS NOT NULL
        ORDER BY name_mun
    """)
    return rows or []


@st.cache_data(ttl=600, show_spinner=False)
def get_departments() -> list[dict]:
    rows = query_rows("""
        SELECT id_dept, name_dept
        FROM dim_departament
        WHERE id_dept IS NOT NULL AND name_dept IS NOT NULL
        ORDER BY name_dept
    """)
    return rows or []


@st.cache_data(ttl=300, show_spinner=False)
def fetch_time_series(
    layer_id: str,
    id_mun:  str | None,
    id_dept: str | None,
) -> pd.DataFrame:
    layer = LAYER_TREE.find_layer(layer_id)
    if layer is None:
        return pd.DataFrame(columns=["year", "value"])

    table     = getattr(layer, "data_table", "")
    year_col  = getattr(layer, "year_col", "year")
    value_col = getattr(layer, "value_col", "")
    agg       = getattr(layer, "agg_func", "AVG")

    # Las tasas per cápita no deben sumarse cuando se agrega sobre varios
    # municipios o departamentos — la suma de tasas no tiene significado estadístico.
    if agg == "SUM" and "per_capita" in value_col and not id_mun:
        agg = "AVG"

    row_filter = (
        getattr(layer, "row_filter", "")
        or getattr(layer, "filter_sql", "")
        or ""
    )

    where_parts: list[str] = []
    if row_filter:
        where_parts.append(row_filter)

    if id_mun:
        mun_col = _mun_col(layer)
        if mun_col:
            where_parts.append(f"{mun_col} = '{id_mun}'")
    elif id_dept:
        clause = _dept_filter_clause(layer, id_dept)
        if clause:
            where_parts.append(clause)

    where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    sql = f"""
        SELECT "{year_col}" AS year, {agg}("{value_col}") AS value
        FROM "{table}"
        {where}
        GROUP BY "{year_col}"
        HAVING {agg}("{value_col}") IS NOT NULL
        ORDER BY "{year_col}"
    """
    rows = query_rows(sql)
    if not rows:
        return pd.DataFrame(columns=["year", "value"])

    df = pd.DataFrame(rows)
    df["year"]  = pd.to_numeric(df["year"],  errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df.dropna().reset_index(drop=True)


# ─── Descripciones automáticas ───────────────────────────────────────────────

def _describe_trend(df: pd.DataFrame, label: str, value_label: str = "") -> dict:
    """
    Devuelve:
      text     -> texto principal en lenguaje llano
      method   -> "insuficiente" | "pocos_datos" | "sin_tendencia" | "tendencia"
      r2       -> R² (float) o None si no aplica
      n_years  -> número de años con dato
    """
    n = len(df)
    if n < 2:
        return {"text": f"**{label}**: datos insuficientes para determinar tendencia.",
                "method": "insuficiente", "r2": None, "n_years": n}

    is_rate   = value_label and ("%" in value_label or "tasa" in value_label.lower())
    unit_note = f" (medido en {value_label})" if is_rate else ""

    peak_yr  = int(df.loc[df["value"].idxmax(), "year"])
    peak_val = df["value"].max()
    min_yr   = int(df.loc[df["value"].idxmin(), "year"])
    min_val  = df["value"].min()
    stats_txt = (
        f"El valor más alto se registró en **{peak_yr}** ({peak_val:,.2f}) "
        f"y el más bajo en **{min_yr}** ({min_val:,.2f})."
    )

    y0, y1 = int(df["year"].iloc[0]), int(df["year"].iloc[-1])
    v0, v1 = df["value"].iloc[0], df["value"].iloc[-1]

    if n < 4:
        pct_endpoint = ((v1 - v0) / abs(v0) * 100) if v0 != 0 else float("nan")
        if np.isnan(pct_endpoint):
            change = f"pasó de {v0:,.2f} a {v1:,.2f}"
        elif pct_endpoint >= 0:
            change = f"un aumento del {pct_endpoint:.1f}%"
        else:
            change = f"una reducción del {abs(pct_endpoint):.1f}%"
        text = (
            f"**{label}**{unit_note} solo cuenta con {n} años de dato ({y0}–{y1}). "
            f"Entre esos años registró {change} (de {v0:,.2f} a {v1:,.2f}), calculado "
            f"directamente entre el primer y el último dato disponible ℹ️. {stats_txt}"
        )
        return {"text": text, "method": "pocos_datos", "r2": None, "n_years": n}

    slope, intercept = np.polyfit(df["year"], df["value"], 1)
    pred   = slope * df["year"] + intercept
    ss_res = float(np.sum((df["value"] - pred) ** 2))
    ss_tot = float(np.sum((df["value"] - df["value"].mean()) ** 2))
    r2     = 1.0 - ss_res / ss_tot if ss_tot != 0 else 1.0

    R2_MIN            = 0.35
    PCT_STABLE        = 3.0
    OSCILLATION_RATIO = 0.6

    if r2 < R2_MIN:
        text = (
            f"**{label}**{unit_note} no muestra un patrón sostenido de aumento o disminución ℹ️ "
            f"— los valores fluctúan bastante entre {y0} y {y1}. {stats_txt}"
        )
        return {"text": text, "method": "sin_tendencia", "r2": r2, "n_years": n}

    pred_start = slope * df["year"].iloc[0] + intercept
    pred_end   = slope * df["year"].iloc[-1] + intercept
    pct_trend  = (
        (pred_end - pred_start) / abs(pred_start) * 100
        if pred_start != 0 else float("nan")
    )

    diffs      = np.diff(df["value"].to_numpy())
    signs_nz   = np.sign(diffs)
    signs_nz   = signs_nz[signs_nz != 0]
    n_posibles = max(len(signs_nz) - 1, 0)
    n_cambios  = int(np.sum(np.diff(signs_nz) != 0)) if len(signs_nz) > 1 else 0
    ratio_osc  = (n_cambios / n_posibles) if n_posibles > 0 else 0.0

    if not np.isnan(pct_trend) and abs(pct_trend) < PCT_STABLE:
        direction = "estable"
        change    = f"una variación mínima ({pct_trend:+.1f}%)"
    elif slope > 0:
        direction = "creciente"
        change    = (f"un aumento del {abs(pct_trend):.1f}% en la tendencia general"
                      if not np.isnan(pct_trend) else "una tendencia al alza")
    else:
        direction = "decreciente"
        change    = (f"una reducción del {abs(pct_trend):.1f}% en la tendencia general"
                      if not np.isnan(pct_trend) else "una tendencia a la baja")

    oscil_note = ""
    if direction != "estable" and ratio_osc >= OSCILLATION_RATIO:
        oscil_note = ", aunque con altibajos importantes de un año a otro"

    text = (
        f"**{label}**{unit_note} presenta una tendencia **{direction}**{oscil_note}, "
        f"con {change} entre {y0} y {y1} ℹ️. {stats_txt}"
    )
    return {"text": text, "method": "tendencia", "r2": r2, "n_years": n}
def _metodologia_texto(method: str, r2, n_years: int) -> str:
    if method == "pocos_datos":
        return (
            f"Esta variable solo tiene **{n_years} años** con dato disponible. "
            "Con tan pocos puntos no es posible calcular una tendencia estadísticamente "
            "confiable, así que el porcentaje se calculó **directamente entre el primer "
            "y el último dato registrado**, sin ajustar ninguna línea de tendencia."
        )
    if method == "sin_tendencia":
        return (
            "Se intentó ajustar una **línea de tendencia** (una técnica llamada "
            "regresión lineal) a todos los años disponibles, pero los valores suben y "
            "bajan de forma tan irregular que la línea no logra explicar bien el "
            "comportamiento de la serie. Esto se mide con un indicador llamado **R²** "
            f"(va de 0 a 1; entre más cercano a 1, mejor explica la línea los datos). "
            f"Aquí el R² fue de solo **{r2:.2f}**, por eso no se reporta un porcentaje "
            "de tendencia — solo se muestran el valor más alto y más bajo registrados."
        )
    if method == "tendencia":
        return (
            "Se ajustó una **línea de tendencia** a todos los años disponibles (una "
            "técnica llamada regresión lineal), y el porcentaje mostrado se calculó "
            "**a partir de esa línea**"
            "Esto evita que un solo año atípico (por ejemplo, un dato "
            "incompleto o un pico inusual) distorsione el porcentaje reportado. "
            f"La línea explica razonablemente bien el comportamiento de los datos "
            f"(indicador **R² = {r2:.2f}**, donde 1 sería un ajuste perfecto)."
        )
    return "Datos insuficientes para aplicar un método de cálculo."
def _describe_correlation(r: float, label_x: str, label_y: str, n: int) -> str:
    if n < 3:
        return "Se necesitan al menos 3 años con datos comunes para calcular la correlación."

    abs_r = abs(r)
    strength  = "fuerte" if abs_r >= 0.7 else ("moderada" if abs_r >= 0.4 else "débil")
    direction = "positiva" if r >= 0 else "negativa"

    extra = ""
    if abs_r >= 0.7:
        extra = (
            f" Cuando **{label_x}** aumenta, **{label_y}** tiende a "
            f"{'aumentar' if r > 0 else 'disminuir'} también."
        )
    elif abs_r < 0.4:
        extra = " Las dos variables no muestran una asociación lineal clara."

    return (
        f"La correlación entre **{label_x}** y **{label_y}** es "
        f"**{strength}** y **{direction}** (r = {r:.2f}, n = {n} años).{extra}"
    )


# ─── Gráficos ────────────────────────────────────────────────────────────────

def _single_line_chart(label: str, df: pd.DataFrame, color: str, y_label: str) -> go.Figure:
    years = df["year"].astype(int).tolist()
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x    = years,
        y    = df["value"],
        mode = "lines+markers",
        name = label,
        line = dict(color=color, width=2),
        marker = dict(size=7, color=color, line=dict(color="#ffffff", width=1.5)),
        hovertemplate = "Año: %{x}<br>Valor: %{y:,.2f}<extra></extra>",
        showlegend = False,
    ))
    fig.update_layout(
        xaxis  = dict(
            title    = "Año",
            tickmode = "array",
            tickvals = years,
            ticktext = [str(y) for y in years],
            showgrid = False,
        ),
        yaxis  = dict(title=y_label or "Valor", gridcolor="#f0f0f0"),
        margin = dict(t=10, b=40, l=60, r=20),
        plot_bgcolor  = "#ffffff",
        paper_bgcolor = "#ffffff",
        hovermode     = "x unified",
        height        = 280,
    )
    return fig


def _scatter_chart(
    df_x: pd.DataFrame, df_y: pd.DataFrame,
    label_x: str, label_y: str,
) -> tuple[go.Figure, float, int]:
    merged = df_x.merge(df_y, on="year", suffixes=("_x", "_y")).dropna()
    r = merged["value_x"].corr(merged["value_y"]) if len(merged) >= 2 else float("nan")

    fig = go.Figure()

    if len(merged) >= 2:
        m, b = np.polyfit(merged["value_x"], merged["value_y"], 1)
        x_line = np.linspace(merged["value_x"].min(), merged["value_x"].max(), 80)
        fig.add_trace(go.Scatter(
            x=x_line, y=m * x_line + b,
            mode="lines",
            line=dict(color="#94a3b8", width=1.5, dash="dot"),
            showlegend=False,
            hoverinfo="skip",
        ))

    fig.add_trace(go.Scatter(
        x    = merged["value_x"],
        y    = merged["value_y"],
        mode = "markers+text",
        text = merged["year"].astype(int).astype(str),
        textposition = "top center",
        textfont     = dict(size=10, color="#475569"),
        marker = dict(
            size=10,
            color=merged["year"],
            colorscale=[[0, "#bfdbfe"], [1, "#1d4ed8"]],
            line=dict(color="#ffffff", width=1.5),
            showscale=True,
            colorbar=dict(title="Año", thickness=12, len=0.6),
        ),
        hovertemplate=(
            "Año: %{text}<br>"
            f"{label_x}: %{{x:,.2f}}<br>"
            f"{label_y}: %{{y:,.2f}}<extra></extra>"
        ),
        showlegend=False,
    ))

    fig.update_layout(
        xaxis  = dict(title=label_x, showgrid=False),
        yaxis  = dict(title=label_y, gridcolor="#f0f0f0"),
        plot_bgcolor  = "#ffffff",
        paper_bgcolor = "#ffffff",
        margin = dict(t=20, b=50, l=60, r=20),
        height = 400,
    )
    return fig, r, len(merged)


def _matrix_chart(frames: dict[str, pd.DataFrame]) -> go.Figure | None:
    if len(frames) < 2:
        return None

    combined = pd.DataFrame()
    for label, df in frames.items():
        s = df.set_index("year")["value"].rename(label)
        combined = s.to_frame() if combined.empty else combined.join(s, how="outer")

    corr   = combined.corr()
    labels = list(corr.columns)
    z      = corr.values
    text   = [[f"{v:.2f}" for v in row] for row in z]

    fig = go.Figure(go.Heatmap(
        z           = z,
        x           = labels,
        y           = labels,
        text        = text,
        texttemplate= "%{text}",
        textfont    = dict(size=12),
        colorscale  = [[0, "#b91c1c"], [0.5, "#f8fafc"], [1, "#1d4ed8"]],
        zmid=0, zmin=-1, zmax=1,
        colorbar    = dict(title="r", thickness=14, len=0.8),
        hovertemplate="<b>%{y}</b> × <b>%{x}</b><br>r = %{z:.2f}<extra></extra>",
    ))
    fig.update_layout(
        margin = dict(t=20, b=20, l=20, r=20),
        height = max(300, 80 * len(labels)),
        xaxis  = dict(tickangle=-35),
        plot_bgcolor  = "#ffffff",
        paper_bgcolor = "#ffffff",
    )
    return fig


# ─── Render principal ────────────────────────────────────────────────────────

def render_analisis_temporal() -> None:
    st.markdown("## 📊 Análisis Temporal")

    layers      = get_queryable_layers()
    cat_label   = {k: v["label"] for k, v in CATEGORIES.items()}
    layer_by_id = {l.id: l for l in layers}

    options    = [l.id for l in layers]
    format_map = {
        l.id: f"{cat_label.get(l.category, l.category)} · {l.label}"
        for l in layers
    }

    # ── Filtros ──────────────────────────────────────────────────────────────
    with st.expander("⚙️ Configuración", expanded=True):
        col_geo, col_geo_sel, col_vars = st.columns([1, 2, 3])

        with col_geo:
            nivel = st.radio(
                "Nivel geográfico",
                ["Por departamento", "Por municipio"],
                index=0,
            )

        id_mun:  str | None = None
        id_dept: str | None = None

        with col_geo_sel:
            if nivel == "Por departamento":
                depts       = get_departments()
                dept_opts   = [d["id_dept"] for d in depts]
                dept_labels = {d["id_dept"]: d["name_dept"] for d in depts}
                id_dept = st.selectbox(
                    "Departamento",
                    options=dept_opts,
                    format_func=lambda k: dept_labels.get(k, k),
                )
            else:
                munis       = get_municipalities()
                mun_opts    = [m["id_mun"] for m in munis]
                mun_labels  = {m["id_mun"]: m["label"] for m in munis}
                id_mun = st.selectbox(
                    "Municipio",
                    options=mun_opts,
                    format_func=lambda k: mun_labels.get(k, k),
                )

        with col_vars:
            if nivel == "Por municipio":
                avail = [l.id for l in layers if can_filter_by_mun(layer_by_id[l.id])]
                st.caption(
                    f"En modo municipio solo se muestran las {len(avail)} variables "
                    "con dato a nivel municipal."
                )
            else:
                avail = options

            selected_ids: list[str] = st.multiselect(
                "Variables",
                options=avail,
                format_func=lambda k: format_map.get(k, k),
                placeholder="Selecciona una o más variables…",
            )

    if not selected_ids:
        st.info("Selecciona al menos una variable para comenzar el análisis.")
        return

    # ── Cargar series ─────────────────────────────────────────────────────────
    with st.spinner("Cargando datos…"):
        frames:   dict[str, pd.DataFrame] = {}
        skipped:  list[str] = []
        no_dept:  list[str] = []

        for lid in selected_ids:
            layer = layer_by_id[lid]

            # Advertir si la variable no puede filtrarse por departamento
            if nivel == "Por departamento" and id_dept:
                if _dept_filter_clause(layer, id_dept) is None:
                    no_dept.append(layer.label)

            df = fetch_time_series(lid, id_mun, id_dept)
            if df.empty:
                skipped.append(layer.label)
            else:
                frames[layer.label] = df

    if no_dept:
        st.info(
            f"Las siguientes variables no tienen columna departamental y se muestran "
            f"con el total general: {', '.join(no_dept)}.",
            icon="ℹ️",
        )
    if skipped:
        st.warning(
            f"Sin datos para el nivel seleccionado: {', '.join(skipped)}.",
            icon="⚠️",
        )
    if not frames:
        st.error("Ninguna de las variables seleccionadas tiene datos disponibles.")
        return

    tab_serie, tab_corr = st.tabs(["📈 Serie temporal", "🔗 Correlación"])

    # ══════════════════════════════════════════════════════════════════════════
    #  TAB 1 — SERIE TEMPORAL
    # ══════════════════════════════════════════════════════════════════════════
    with tab_serie:
        for i, (label, df) in enumerate(frames.items()):
            layer      = next((l for l in layers if l.label == label), None)
            y_label    = getattr(layer, "value_label", "") if layer else ""
            color      = _PALETTE[i % len(_PALETTE)]

            st.markdown(f"#### {label}")
            fig = _single_line_chart(label, df, color, y_label)
            st.plotly_chart(fig, use_container_width=True)
            #st.markdown(_describe_trend(df, label, y_label))
            desc = _describe_trend(df, label, y_label)
            st.markdown(desc["text"])
            with st.expander("ℹ️ ¿Cómo se calculó este porcentaje?"):
                st.markdown(_metodologia_texto(desc["method"], desc["r2"], desc["n_years"]))
            if i < len(frames) - 1:
                st.divider()

    # ══════════════════════════════════════════════════════════════════════════
    #  TAB 2 — CORRELACIÓN
    # ══════════════════════════════════════════════════════════════════════════
    with tab_corr:
        if len(frames) < 2:
            st.info("Selecciona al menos **2 variables** para analizar correlaciones.")
        else:
            sub_scatter, sub_matrix = st.tabs(["🔵 Dispersión", "🟦 Matriz"])
            labels_list = list(frames.keys())

            # ── Dispersión ──
            with sub_scatter:
                col_x, col_y = st.columns(2)
                with col_x:
                    sel_x = st.selectbox("Variable X", labels_list, index=0, key="cx")
                with col_y:
                    default_y = 1 if len(labels_list) > 1 else 0
                    sel_y = st.selectbox("Variable Y", labels_list, index=default_y, key="cy")

                if sel_x == sel_y:
                    st.warning("Selecciona dos variables distintas.")
                else:
                    fig_s, r, n = _scatter_chart(
                        frames[sel_x], frames[sel_y], sel_x, sel_y
                    )
                    st.plotly_chart(fig_s, use_container_width=True)

                    st.markdown("### 📝 Descripción de resultados")
                    if not np.isnan(r):
                        st.markdown(_describe_correlation(r, sel_x, sel_y, n))
                    else:
                        st.warning("No hay suficientes años en común para calcular la correlación.")

            # ── Matriz ──
            with sub_matrix:
                fig_m = _matrix_chart(frames)
                if fig_m:
                    st.plotly_chart(fig_m, use_container_width=True)

                    st.markdown("### 📝 Descripción de resultados")
                    labels   = list(frames.keys())
                    combined = pd.DataFrame()
                    for lbl, df in frames.items():
                        s = df.set_index("year")["value"].rename(lbl)
                        combined = s.to_frame() if combined.empty else combined.join(s, how="outer")
                    corr = combined.corr()

                    pairs_strong, pairs_mod, pairs_weak = [], [], []
                    for i in range(len(labels)):
                        for j in range(i + 1, len(labels)):
                            r_val = corr.iloc[i, j]
                            pair  = f"**{labels[i]}** y **{labels[j]}** (r = {r_val:.2f})"
                            if abs(r_val) >= 0.7:
                                pairs_strong.append(pair)
                            elif abs(r_val) >= 0.4:
                                pairs_mod.append(pair)
                            else:
                                pairs_weak.append(pair)

                    if pairs_strong:
                        st.markdown(f"**Correlación fuerte** (|r| ≥ 0.7): {'; '.join(pairs_strong)}.")
                    if pairs_mod:
                        st.markdown(f"**Correlación moderada** (0.4 ≤ |r| < 0.7): {'; '.join(pairs_mod)}.")
                    if pairs_weak:
                        st.markdown(f"**Correlación débil** (|r| < 0.4): {'; '.join(pairs_weak)}.")
