"""
components/map_view.py — Mapa principal con leyenda unificada y colapsable.
"""

import streamlit as st
import folium
from streamlit_folium import st_folium
import branca.colormap as cm

from config.layers_config import LAYER_TREE, MAP_CONFIG
from core.layer import (ChoroplethLayer, PolygonLayer, PointLayer, BubbleLayer,
                        BarChartLayer, BeneficiaryLayer, LineLayer, VictimLayer,
                        HatchLayer, IconScaleLayer)
def _get_muni_at_point(lat: float, lng: float) -> dict | None:
    """Identifica el municipio en un punto dado."""
    from core.db import query_rows
    rows = query_rows("""
        SELECT id_mun, name_mun
        FROM dim_divipola
        WHERE ST_Contains(
            geometry,
            ST_SetSRID(ST_Point(%s, %s), 4326)
        )
        LIMIT 1
    """, (lng, lat))
    return rows[0] if rows else None

def _is_on_subregion(lat: float, lng: float) -> bool:
    """Retorna True si el punto cae dentro de una subregión."""
    from core.db import query_rows
    rows = query_rows("""
        SELECT id_subregion
        FROM subregion
        WHERE ST_Contains(
            geometry,
            ST_SetSRID(ST_Point(%s, %s), 4326)
        )
        LIMIT 1
    """, (lng, lat))
    return len(rows) > 0

def render_map():
    center  = MAP_CONFIG["center"]
    zoom    = MAP_CONFIG["zoom"]
    basemap = st.session_state.get("basemap", MAP_CONFIG["default_basemap"])
    year    = st.session_state.get("selected_year")

    m = folium.Map(location=center, zoom_start=zoom, tiles=None, control_scale=True)
    _add_basemap(m, basemap)

    active_ids = st.session_state.get("active_layers", [])

    if not active_ids:
        st.info("☝️ Activa una capa desde el panel izquierdo.")

    active_ids_sorted = sorted(
        active_ids,
        key=lambda lid: getattr(LAYER_TREE.find_layer(lid), "z_index", 10)
    )
    beneficiary_layers = [
        LAYER_TREE.find_layer(lid)
        for lid in active_ids_sorted
        if isinstance(LAYER_TREE.find_layer(lid), BeneficiaryLayer)
    ]
    non_beneficiary_ids = [
        lid for lid in active_ids_sorted
        if not isinstance(LAYER_TREE.find_layer(lid), BeneficiaryLayer)
    ]

    
    legend_items = []

    dept_ids = st.session_state.get("dept_filter", ())

    for layer_id in non_beneficiary_ids:
        layer = LAYER_TREE.find_layer(layer_id)
        if layer is None:
            continue

        # Aplicar dept_ids solo si la capa lo soporta
        layer_dept_ids = dept_ids if layer.filterable_by_dept else ()

        with st.spinner(f"Cargando {layer.label}..."):
            try:
                if isinstance(layer, ChoroplethLayer):
                    info = _add_choropleth_layer(m, layer, year,
                                                 dept_ids=layer_dept_ids)
                    if info:
                        legend_items.append(info)
                elif isinstance(layer, BubbleLayer):
                    info = _add_bubble_layer(m, layer, year,
                                             dept_ids=layer_dept_ids)
                    if info:
                        legend_items.append(info)
                elif isinstance(layer, BarChartLayer):
                    info = _add_bar_chart_layer(m, layer, year,
                                                dept_ids=layer_dept_ids)
                    if info:
                        legend_items.append(info)
                elif isinstance(layer, PolygonLayer):
                    _add_polygon_layer(m, layer,
                                       dept_ids=layer_dept_ids)
                    legend_items.append({
                        "type":  "solid",
                        "label": layer.label,
                        "color": layer.color,
                    })
                elif isinstance(layer, PointLayer):
                    _add_point_layer(m, layer)
                    legend_items.append({
                        "type":  "solid",
                        "label": layer.label,
                        "color": layer.color,
                    })
                elif isinstance(layer, LineLayer):
                    _add_line_layer(m, layer)
                    legend_items.append({
                        "type":       "line",
                        "label":      layer.label,
                        "color":      layer.color,
                        "dash_array": layer.dash_array,
                    })
                elif isinstance(layer, VictimLayer):
                    info = _add_victim_layer(m, layer, year,
                                             dept_ids=layer_dept_ids)
                    if info:
                        legend_items.append(info)
                elif isinstance(layer, HatchLayer):
                    info = _add_hatch_layer(m, layer,
                                            dept_ids=layer_dept_ids)
                    if info:
                        legend_items.append(info)
                elif isinstance(layer, IconScaleLayer):
                    info = _add_icon_scale_layer(m, layer, year,
                                                 dept_ids=layer_dept_ids)
                    if info:
                        legend_items.append(info)
            except Exception as e:
                st.warning(f"No se pudieron cargar los datos de **{layer.label}**.")

    # Beneficiarios con filtro
    if beneficiary_layers:
        with st.spinner("Cargando beneficiarios..."):
            info = _add_beneficiary_layers(m, beneficiary_layers, year,
                                           dept_ids=dept_ids)
            if info:
                legend_items.append(info)
    # Inyectar leyenda unificada si hay al menos una capa
    if legend_items:
        m.get_root().html.add_child(
            folium.Element(_build_unified_legend(legend_items))
        )
    # ── Panel A flotante ──────────────────────────────────────
    coords_a = st.session_state.get("clicked_coords")
    if coords_a:
        cache_key = f"panel_a_{coords_a}_{year}"
        if st.session_state.get("selected_data_key") != cache_key:
            from core.db import get_subregion_at_point
            panel_data = get_subregion_at_point(coords_a[0], coords_a[1], year)
            st.session_state.selected_data     = panel_data
            st.session_state.selected_data_key = cache_key
        else:
            panel_data = st.session_state.get("selected_data")

        if panel_data:
            m.get_root().html.add_child(
                folium.Element(_build_panel_a_html(panel_data, year))
            )

    # Key dinámico para forzar rerenderizado cuando cambia el panel
    map_key = f"main_map_{st.session_state.get('selected_data_key', '')}_{st.session_state.get('clicked_muni_id', '')}_{st.session_state.get('clicked_muni_coords', '')}"
    # ── Panel B flotante ──────────────────────────────────────
    muni_id  = st.session_state.get("clicked_muni_id")
    cat_id   = st.session_state.get("active_exclusive_category")
    if muni_id and cat_id:
        cache_key_b = f"panel_b_{muni_id}_{cat_id}_{year}"
        if st.session_state.get("panel_b_key") != cache_key_b:
            from core.db import CATEGORY_QUERY_MAP
            query_fn = CATEGORY_QUERY_MAP.get(cat_id)
            if query_fn:
                panel_data_b = query_fn(muni_id, year)
                st.session_state.panel_b_data = panel_data_b
                st.session_state.panel_b_key  = cache_key_b
        else:
            panel_data_b = st.session_state.get("panel_b_data")

        if panel_data_b:
            m.get_root().html.add_child(
                folium.Element(_build_panel_b_html(panel_data_b, year, cat_id))
            )
    map_data = st_folium(
        m,
        width            = "100%",
        height           = 600,
        returned_objects = ["last_object_clicked","last_object_clicked_tooltip"],
        key              = map_key,  

    )


    if map_data and map_data.get("last_object_clicked"):
        click   = map_data.get("last_object_clicked")
        tooltip = map_data.get("last_object_clicked_tooltip") or ""
        clicked_choropleth = "food_insecurity" in str(tooltip)
        lat     = click.get("lat")
        lng     = click.get("lng")


        if lat and lng:
            new_coords        = (lat, lng)
            clicked_choropleth = "food_insecurity" in str(tooltip)

            if clicked_choropleth:
                # Clic explícito en la coroplética → Panel A
                # Limpia categoría activa para no bloquear
                if st.session_state.get("clicked_coords") != new_coords:
                    st.session_state.clicked_coords              = new_coords
                    st.session_state.selected_data               = None
                    st.session_state.selected_data_key           = None
                    st.session_state.clicked_muni_coords         = None
                    st.session_state.clicked_muni_id             = None
                    st.session_state.panel_b_data                = None
                    st.session_state.panel_b_key                 = None
                    st.rerun()
            else:
                # Clic en otro elemento → Panel B
                cat_id = st.session_state.get("active_exclusive_category")
                if cat_id and cat_id != "seguridad_alimentaria":
                    if st.session_state.get("clicked_muni_coords") != new_coords:
                        muni = _get_muni_at_point(lat, lng)
                        if muni:
                            st.session_state.clicked_muni_coords = new_coords
                            st.session_state.clicked_muni_id     = muni.get("id_mun")
                            st.session_state.clicked_muni_name   = muni.get("name_mun")
                            st.session_state.panel_b_data        = None
                            st.session_state.panel_b_key         = None
                            st.session_state.clicked_coords      = None
                            st.session_state.selected_data       = None
                            st.rerun()
# ─────────────────────────────────────────────────────────────
#  LEYENDA UNIFICADA
# ─────────────────────────────────────────────────────────────

def _build_unified_legend(items: list) -> str:
    """
    Genera un panel HTML flotante colapsable con la leyenda de
    todas las capas activas en forma de lista.
    """

    def _item_html(item: dict) -> str:
        t = item.get("type")

        if t == "gradient":
            # Escala de color con rango numérico (coroplética / burbujas)
            return f"""
            <div class="leg-item">
                <div class="leg-title">{item['label']}</div>
                <div style="display:flex;gap:6px;align-items:stretch;margin-top:4px;">
                    <div style="width:12px;min-height:60px;
                                background:linear-gradient(to top,{item['color_low']},{item['color_high']});
                                border-radius:3px;flex-shrink:0;"></div>
                    <div style="display:flex;flex-direction:column;
                                justify-content:space-between;font-size:10px;color:#555;">
                        <span>{item['val_max']}</span>
                        <span>{item['val_mid']}</span>
                        <span>{item['val_min']}</span>
                    </div>
                </div>
            </div>"""

        if t == "bubble":
            # Escala de color + tamaño para BubbleLayer
            r_min = item['radius_min']
            r_max = item['radius_max']
            return f"""
            <div class="leg-item">
                <div class="leg-title">{item['label']}</div>
                <div style="display:flex;gap:6px;align-items:stretch;margin-top:4px;">
                    <div style="width:12px;min-height:60px;
                                background:linear-gradient(to top,{item['color_low']},{item['color_high']});
                                border-radius:3px;flex-shrink:0;"></div>
                    <div style="display:flex;flex-direction:column;
                                justify-content:space-between;font-size:10px;color:#555;">
                        <span>{item['val_max']}</span>
                        <span>{item['val_mid']}</span>
                        <span>{item['val_min']}</span>
                    </div>
                </div>
                <div style="display:flex;align-items:center;gap:6px;margin-top:6px;flex-wrap:wrap;">
                    <div style="width:{r_min*2}px;height:{r_min*2}px;border-radius:50%;
                                background:{item['color_low']};border:1px solid #aaa;flex-shrink:0;"></div>
                    <span style="font-size:10px;color:#666;">{item['val_min']}</span>
                    <div style="width:{r_max*2}px;height:{r_max*2}px;border-radius:50%;
                                background:{item['color_high']};border:1px solid #aaa;flex-shrink:0;"></div>
                    <span style="font-size:10px;color:#666;">{item['val_max']}</span>
                </div>
            </div>"""
        
        if t == "bars":
                    bars_legend = "".join([
                        f"""<div style="display:flex;align-items:center;gap:6px;margin-top:4px;">
                                <div style="width:12px;height:12px;border-radius:2px;
                                            background:{g['color']};flex-shrink:0;"></div>
                                <span style="font-size:11px;color:#555;">{g['label']}</span>
                            </div>"""
                        for g in item.get("groups", [])
                    ])
                    return f"""
                    <div class="leg-item">
                        <div class="leg-title">{item['label']}</div>
                        {bars_legend}
                    </div>"""
        if t == "beneficiary":
            n = item.get("n", 1)
            return f"""
            <div class="leg-item">
                <div style="display:flex;align-items:center;gap:8px;">
                    <div style="position:relative;width:24px;height:24px;flex-shrink:0;">
                        <div style="background:white;border:2px solid #1d4ed8;
                                    border-radius:50%;width:24px;height:24px;
                                    display:flex;align-items:center;
                                    justify-content:center;font-size:12px;">👤</div>
                        {"" if n < 2 else
                        f'<div style="position:absolute;top:-4px;right:-4px;'
                        f'background:#ef4444;color:white;border-radius:50%;'
                        f'width:12px;height:12px;font-size:8px;font-weight:700;'
                        f'display:flex;align-items:center;justify-content:center;">{n}</div>'}
                    </div>
                    <span class="leg-title">Beneficiarios ({n} programa{'s' if n > 1 else ''})</span>
                </div>
            </div>"""
        if t == "line":
            dash = item.get("dash_array", "")
            line_style = f"border-top: 3px {'dashed' if dash else 'solid'} {item['color']};"
            return f"""
            <div class="leg-item">
                <div style="display:flex;align-items:center;gap:8px;">
                    <div style="width:24px;height:0;{line_style}flex-shrink:0;"></div>
                    <span class="leg-title">{item['label']}</span>
                </div>
            </div>"""
        if t == "victim":
            return f"""
            <div class="leg-item">
                <div style="display:flex;align-items:center;gap:8px;">
                    <div style="background:#dc2626;border:2px solid #991b1b;
                                border-radius:50%;width:20px;height:20px;
                                display:flex;align-items:center;
                                justify-content:center;font-size:11px;
                                flex-shrink:0;">🕊️</div>
                    <span class="leg-title">{item['label']}</span>
                </div>
            </div>"""
        if t == "hatch":
            spacing = item.get("spacing", 6)
            color   = item["hatch_color"]
            return f"""
            <div class="leg-item">
                <div style="display:flex;align-items:center;gap:8px;">
                    <svg width="20" height="20" style="flex-shrink:0;border:1px solid #ddd;border-radius:3px;">
                        <defs>
                            <pattern id="leg_hatch_{color[1:]}"
                                     patternUnits="userSpaceOnUse"
                                     width="{spacing}" height="{spacing}"
                                     patternTransform="rotate(45)">
                                <line x1="0" y1="0" x2="0" y2="{spacing}"
                                      stroke="{color}" stroke-width="1"/>
                            </pattern>
                        </defs>
                        <rect width="20" height="20"
                              fill="url(#leg_hatch_{color[1:]})"
                              stroke="{item['border_color']}" stroke-width="1"/>
                    </svg>
                    <span class="leg-title">{item['label']}</span>
                </div>
            </div>"""
        
        if t == "icon_scale":
                unit     = f" {item['unit']}" if item.get("unit") else ""
                icon     = item.get("icon", "📍")
                return f"""
                <div class="leg-item">
                    <div class="leg-title">{item['label']}</div>
                    <div style="display:flex;gap:6px;
                                align-items:stretch;margin-top:6px;">
                        <div style="
                            width:20px;min-height:60px;
                            background:linear-gradient(
                                to top, {item['color_low']}, {item['color_high']}
                            );
                            border-radius:3px;flex-shrink:0;
                        "></div>
                        <div style="display:flex;flex-direction:column;
                                    justify-content:space-between;
                                    font-size:10px;color:#555;">
                            <span>{item['val_max']}{unit}</span>
                            <span>{item['val_mid']}{unit}</span>
                            <span>{item['val_min']}{unit}</span>
                        </div>
                        <div style="display:flex;align-items:center;
                                    padding-left:4px;font-size:16px;">
                            {icon}
                        </div>
                    </div>
                </div>"""   
        # Capa simple con un solo color
        return f"""
        <div class="leg-item">
            <div style="display:flex;align-items:center;gap:8px;">
                <div style="width:14px;height:14px;border-radius:3px;
                            background:{item['color']};flex-shrink:0;
                            border:1px solid rgba(0,0,0,0.2);"></div>
                <span class="leg-title">{item['label']}</span>
            </div>
        </div>"""

    items_html = "".join([_item_html(i) for i in items])
    n = len(items)

    return f"""
    <style>
        #leg-cb {{ display: none; }}

        #leg-toggle {{
            position: fixed;
            bottom: 36px;
            right: 12px;
            z-index: 1001;
            background: white;
            border: none;
            border-radius: 8px;
            padding: 7px 12px;
            font-family: sans-serif;
            font-size: 12px;
            font-weight: 700;
            color: #333;
            cursor: pointer;
            box-shadow: 0 2px 8px rgba(0,0,0,0.25);
            display: flex;
            align-items: center;
            gap: 6px;
            user-select: none;
        }}
        #leg-toggle:hover {{ background: #f5f5f5; }}

        #leg-panel {{
            position: fixed;
            bottom: 78px;
            right: 12px;
            z-index: 1000;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 12px rgba(0,0,0,0.25);
            font-family: sans-serif;
            min-width: 180px;
            max-width: 220px;
            max-height: 70vh;
            overflow-y: auto;
            display: none;
            padding: 12px;
        }}

        #leg-cb:checked ~ #leg-panel {{ display: block; }}
        #leg-cb:checked ~ #leg-toggle .leg-arrow {{ transform: rotate(180deg); }}

        .leg-item {{
            padding: 8px 0;
            border-bottom: 1px solid #f0f0f0;
        }}
        .leg-item:last-child {{ border-bottom: none; padding-bottom: 0; }}
        .leg-title {{
            font-size: 11px;
            font-weight: 700;
            color: #222;
            margin-bottom: 2px;
        }}
        .leg-arrow {{
            display: inline-block;
            transition: transform 0.2s;
        }}
    </style>

    <input type="checkbox" id="leg-cb">
    <div id="leg-panel">
        {items_html}
    </div>
    <label id="leg-toggle" for="leg-cb">
        🗂 Leyenda
        <span style="background:#1a1f2e;color:white;border-radius:10px;
                     padding:1px 6px;font-size:10px;">{n}</span>
        <span class="leg-arrow">▲</span>
    </label>
    """
# ─────────────────────────────────────────────────────────────
#  FILTRO POR PERCENTIL
# ─────────────────────────────────────────────────────────────

def _apply_percentile_filter(geojson: dict, threshold: float) -> dict:
    """
    Filtra features dejando solo los que tienen valor >= percentil indicado.
    threshold=50 → top 50% de municipios por valor.
    """
    if threshold is None:
        return geojson

    features = geojson.get("features", [])
    values   = [
        f["properties"].get("valor")
        for f in features
        if f["properties"].get("valor") is not None
    ]

    if not values:
        return geojson

    import numpy as np
    cutoff = np.percentile(values, threshold)

    filtered = [
        f for f in features
        if f["properties"].get("valor") is not None
        and f["properties"]["valor"] >= cutoff
    ]

    return {"type": "FeatureCollection", "features": filtered}

# ─────────────────────────────────────────────────────────────
#  RENDERIZADO DE CAPAS
# ─────────────────────────────────────────────────────────────

def _add_choropleth_layer(m, layer, year, dept_ids=()) -> dict | None:
    geojson  = layer.get_geojson(year=year, dept_ids=dept_ids)
    features = geojson.get("features", [])
    if not features:
        st.warning(f"Sin datos para **{layer.label}**" + (f" ({year})" if year else "") + ".")
        return None

    values   = [f["properties"]["valor"] for f in features
                if f["properties"].get("valor") is not None]
    val_min, val_max = min(values), max(values)

    colormap = cm.LinearColormap(
        #colors=[layer.color_low, "#fc8d59", layer.color_high],
        colors=[layer.color_low, layer.color_high],
        vmin=val_min, vmax=val_max,
    )
    base = layer.get_folium_style()

    def style_fn(feature):
        v = feature["properties"].get("valor")
        return {
            **base,
            "fillColor": colormap(v) if v is not None else "#ccc",
            "color":     "#555" if layer.border_visible else "transparent",
            "weight":    0.5 if layer.border_visible else 0,
        }

    folium.GeoJson(
        geojson,
        name               = layer.label,
        style_function     = style_fn,
        highlight_function = lambda f: {"weight": 3, "color": "#fff", "fillOpacity": 0.95},
        tooltip            = folium.GeoJsonTooltip(
            fields   = ["nombre", "valor"],
            aliases  = ["Municipio:", f"{layer.value_label}:"],
            sticky   = True,
            style    = ("background:white;color:#333;font-family:sans-serif;"
                        "font-size:13px;padding:8px;border-radius:4px;"),
        ),
    ).add_to(m)

    return {
        "type":       "gradient",
        "label":      layer.value_label,
        "color_low":  layer.color_low,
        "color_high": layer.color_high,
        "val_min":    f"{val_min:.1f}",
        "val_mid":    f"{(val_min+val_max)/2:.1f}",
        "val_max":    f"{val_max:.1f}",
    }


def _add_bubble_layer(m, layer, year, dept_ids=()) -> dict | None:
    geojson  = layer.get_geojson(year=year, dept_ids=dept_ids)
    geojson  = _apply_percentile_filter(geojson, layer.percentile_threshold)
    features = geojson.get("features", [])
    if not features:
        st.warning(f"Sin datos para **{layer.label}**" + (f" ({year})" if year else "") + ".")
        return None

    values  = [f["properties"]["valor"] for f in features if f["properties"].get("valor") is not None]
    if not values:
        return None

    val_min, val_max = min(values), max(values)
    rng = val_max - val_min if val_max != val_min else 1

    colormap = cm.LinearColormap(
        colors=[layer.color_low, layer.color_high],
        vmin=val_min, vmax=val_max,
    )

    for feat in features:
        props = feat["properties"]
        valor = props.get("valor")
        if valor is None:
            continue
        coords = feat["geometry"]["coordinates"]
        lat    = coords[1] + layer.offset[0]
        lng    = coords[0] + layer.offset[1]

        ratio  = (valor - val_min) / rng
        radius = layer.radius_min + ratio * (layer.radius_max - layer.radius_min)
        color  = colormap(valor)
        tip    = (f"<b>{props.get('nombre','—')}</b><br>"
                  f"{layer.value_label}: <b>{valor:,.0f}</b>")
        folium.CircleMarker(
            location=[coords[1], coords[0]],
            radius=radius, color=color,
            fill=True, fill_color=color,
            fill_opacity=0.75, weight=1,
            tooltip=folium.Tooltip(tip),
            popup=folium.Popup(tip, max_width=250),
        ).add_to(m)

    return {
        "type":       "bubble",
        "label":      layer.value_label,
        "color_low":  layer.color_low,
        "color_high": layer.color_high,
        "val_min":    f"{val_min:,.0f}",
        "val_mid":    f"{(val_min+val_max)/2:,.0f}",
        "val_max":    f"{val_max:,.0f}",
        "radius_min": layer.radius_min,
        "radius_max": layer.radius_max,
    }
def _add_bar_chart_layer(m: folium.Map, layer: BarChartLayer, year: int, dept_ids=()) -> dict | None:
    """
    Renderiza mini barras comparativas sobre cada departamento
    usando DivIcon con HTML/CSS puro.
    """
    geojson  = layer.get_geojson(year=year, dept_ids=dept_ids)
    geojson  = _apply_percentile_filter(geojson, layer.percentile_threshold)
    features = geojson.get("features", [])

    if not features:
        st.warning(f"Sin datos para **{layer.label}**" + (f" ({year})" if year else "") + ".")
        return None

    # Calcular valor máximo global para escalar todas las barras igual
    all_values = [
        g["value"]
        for f in features
        for g in f["properties"].get("groups", [])
        if g.get("value") is not None
    ]
    if not all_values:
        return None

    val_max = max(all_values)

    for feat in features:
        props  = feat["properties"]
        coords = feat["geometry"]["coordinates"]
        lat, lng = coords[1], coords[0]
        groups = props.get("groups", [])
        nombre = props.get("nombre", "—")

        # Construir las barras HTML
        bars_html = ""
        tooltip_rows = ""
        for i, g in enumerate(groups):
            val   = g.get("value") or 0
            label = g.get("label", f"Grupo {i+1}")
            color = layer.bar_colors[i] if i < len(layer.bar_colors) else "#888"
            h     = int((val / val_max) * layer.bar_max_height) if val_max else 4
            h     = max(h, 4)   # mínimo visible

            bars_html += f"""
                <div style="display:flex;flex-direction:column;
                            align-items:center;gap:2px;">
                    <div style="font-size:8px;color:#333;font-weight:700;">
                        {val:.1f}
                    </div>
                    <div style="width:{layer.bar_width}px;height:{h}px;
                                background:{color};border-radius:2px 2px 0 0;
                                border:1px solid rgba(0,0,0,0.15);">
                    </div>
                </div>"""
            tooltip_rows += f"<b style='color:{color}'>{label}:</b> {val:.1f}%<br>"

        total_w = len(groups) * (layer.bar_width + 4) + 8

        icon_html = f"""
            <div style="
                background:white;
                border:1px solid #ccc;
                border-radius:4px;
                padding:3px 4px 0 4px;
                box-shadow:0 1px 4px rgba(0,0,0,0.2);
                width:{total_w}px;
            ">
                <div style="display:flex;align-items:flex-end;
                            gap:4px;justify-content:center;">
                    {bars_html}
                </div>
                <div style="width:100%;height:1px;
                            background:#333;margin-top:2px;"></div>
            </div>"""

        tooltip_html = (
            f"<b>{nombre}</b><br>"
            f"{props.get('value_label','')}<br>"
            f"{tooltip_rows}"
            + (f"Año: {year}" if year else "")
        )

        folium.Marker(
            location=[lat, lng],
            icon=folium.DivIcon(
                html         = icon_html,
                icon_size    = (total_w, layer.bar_max_height + 30),
                icon_anchor  = (total_w // 2, layer.bar_max_height + 30),
            ),
            tooltip=folium.Tooltip(tooltip_html),
        ).add_to(m)

    # Info para la leyenda
    legend_groups = []
    if features:
        sample_groups = features[0]["properties"].get("groups", [])
        for i, g in enumerate(sample_groups):
            color = layer.bar_colors[i] if i < len(layer.bar_colors) else "#888"
            legend_groups.append({"label": g.get("label", f"Grupo {i+1}"), "color": color})

    return {
        "type":   "bars",
        "label":  layer.value_label or layer.label,
        "groups": legend_groups,
    }

_CATEGORICAL_PALETTE = [
    "#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f",
    "#edc948", "#b07aa1", "#ff9da7", "#9c755f", "#bab0ac",
    "#d37295", "#fabfd2", "#8cd17d", "#b6992d", "#499894",
    "#86bcb6", "#e49444", "#d4a6c8", "#f1ce63", "#a0cbe8",
    "#ffbe7d", "#cf4e9c", "#8fbbda", "#f4d166", "#3e9651",
    "#d4a7d4", "#ff7f0e", "#1f77b4", "#2ca02c", "#d62728",
    "#9467bd", "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22",
    "#17becf", "#aec7e8", "#ffbb78", "#98df8a", "#c5b0d5",
]

def _add_beneficiary_layers(m: folium.Map, layers: list, year: int, dept_ids=()) -> dict | None:
    """
    Consolida múltiples BeneficiaryLayer en un único icono por municipio.
    Si un municipio aparece en varios programas, muestra badge con el número
    de programas y el tooltip lista todos.
    """
    # Recopilar features de todos los programas activos
    # municipio_id → lista de properties de cada programa
    muni_data: dict = {}

    for layer in layers:
        try:
            geojson  = layer.get_geojson(year=year, dept_ids=dept_ids)
            geojson  = _apply_percentile_filter(geojson, layer.percentile_threshold)
            features = geojson.get("features", [])
            for feat in features:
                props   = feat["properties"]
                id_mun  = props.get("id_mun")
                coords  = feat["geometry"]["coordinates"]
                if id_mun not in muni_data:
                    muni_data[id_mun] = {
                        "coords":   coords,
                        "nombre":   props.get("nombre", "—"),
                        "programs": [],
                    }
                muni_data[id_mun]["programs"].append({
                    "name":  props.get("program_name", layer.label),
                    "total": props.get("total", 0),
                    **{k: v for k, v in props.items()
                       if k not in ("layer_id", "program_name", "id_mun", "nombre", "total")}
                })
        except Exception as e:
            st.error(f"Error cargando **{layer.label}**: {e}")

    if not muni_data:
        return None

    for id_mun, data in muni_data.items():
        coords   = data["coords"]
        lat, lng = coords[1], coords[0]
        programs = data["programs"]
        n        = len(programs)

        # Badge solo si hay más de un programa
        badge_html = ""
        if n > 1:
            badge_html = f"""
                <div style="
                    position:absolute; top:-6px; right:-6px;
                    background:#ef4444; color:white;
                    border-radius:50%; width:16px; height:16px;
                    font-size:9px; font-weight:700;
                    display:flex; align-items:center; justify-content:center;
                    border:1px solid white;
                ">{n}</div>"""

        icon_html = f"""
            <div style="position:relative; display:inline-block;">
                <div style="
                    background:white;
                    border:2px solid #1d4ed8;
                    border-radius:50%;
                    width:28px; height:28px;
                    display:flex; align-items:center;
                    justify-content:center;
                    box-shadow:0 2px 4px rgba(0,0,0,0.3);
                    font-size:14px;
                ">👤</div>
                {badge_html}
            </div>"""

        # Tooltip con info de todos los programas
        programs_html = "".join([
            f"<div style='margin-top:6px;padding-top:6px;"
            f"border-top:1px solid #eee;'>"
            f"<b>{p['name']}</b><br>"
            f"Beneficiarios: <b>{p['total']:,}</b>"
            f"</div>"
            for p in programs
        ])

        tooltip_html = (
            f"<b>{data['nombre']}</b>"
            f"<div style='font-size:11px;color:#555;'>"
            f"{n} programa{'s' if n > 1 else ''}</div>"
            f"{programs_html}"
            + (f"<div style='font-size:10px;color:#999;margin-top:4px;'>Año: {year}</div>" if year else "")
        )

        folium.Marker(
            location=[lat, lng],
            icon=folium.DivIcon(
                html       = icon_html,
                icon_size  = (36, 36),
                icon_anchor= (18, 18),
            ),
            tooltip=folium.Tooltip(tooltip_html),
        ).add_to(m)

    return {
        "type":  "beneficiary",
        "label": "Beneficiarios",
        "n":     len(layers),
    }

def _add_polygon_layer(m, layer, dept_ids=()):
    geojson = layer.get_geojson(dept_ids=dept_ids)
    if not geojson.get("features"):
        st.warning(f"Sin datos para **{layer.label}**.")
        return

    if layer.color_categorical:
        features = geojson["features"]
        color_map = {
            feat["properties"].get("id_dept", i): _CATEGORICAL_PALETTE[i % len(_CATEGORICAL_PALETTE)]
            for i, feat in enumerate(features)
        }

        def style_fn(feature):
            fid   = feature["properties"].get("id_dept")
            color = color_map.get(fid, layer.color)
            return {
                "color":       color,
                "fillColor":   color,
                "weight":      layer.weight,
                "opacity":     0.9,
                "fillOpacity": layer.opacity,
            }
    else:
        style = layer.get_folium_style()
        style_fn = lambda f: style

    folium.GeoJson(
        geojson,
        name               = layer.label,
        style_function     = style_fn,
        highlight_function = lambda f: {**style_fn(f), "weight": 2, "fillOpacity": min(layer.opacity + 0.2, 0.9)},
        tooltip            = folium.GeoJsonTooltip(fields=layer.properties) if layer.properties else None,
    ).add_to(m)

def _add_point_layer(m, layer):
    geojson = layer.get_geojson()
    style   = layer.get_folium_style()
    for feat in geojson.get("features", []):
        coords = feat["geometry"]["coordinates"]
        props  = feat.get("properties", {})
        tip    = "<br>".join([f"<b>{k}:</b> {v}" for k, v in props.items()
                              if k not in ("layer_id","layer_label") and v is not None])
        folium.CircleMarker(
            location=[coords[1], coords[0]],
            radius=style["radius"], color=style["color"],
            fill=True, fill_color=style["fillColor"],
            fill_opacity=style["fillOpacity"], weight=style["weight"],
            tooltip=folium.Tooltip(tip),
        ).add_to(m)

def _add_victim_layer(m: folium.Map, layer: VictimLayer, year: int, dept_ids=()) -> dict | None:
    """
    Renderiza un icono por municipio con víctimas.
    El tooltip muestra una tabla HTML con el conteo por tipo de evento.
    """
    geojson  = layer.get_geojson(year=year, dept_ids=dept_ids)
    geojson  = _apply_percentile_filter(geojson, layer.percentile_threshold)
    features = geojson.get("features", [])

    if not features:
        st.warning(f"Sin datos para **{layer.label}**" + (f" ({year})" if year else "") + ".")
        return None

    for feat in features:
        props  = feat["properties"]
        coords = feat["geometry"]["coordinates"]
        lat, lng = coords[1], coords[0]

        nombre = props.get("nombre", "—")
        total  = props.get("total", 0)
        events = props.get("events", [])

        # Tabla HTML de eventos para el tooltip
        rows_html = "".join([
            f"""<tr>
                <td style="padding:2px 8px 2px 0;font-size:11px;color:#444;">
                    {e.get('event_name','—')}
                </td>
                <td style="padding:2px 0;font-size:11px;
                           font-weight:700;text-align:right;color:#1d4ed8;">
                    {int(e.get('count', 0)):,}
                </td>
            </tr>"""
            for e in events
        ])

        tooltip_html = f"""
            <div style="font-family:sans-serif;min-width:200px;">
                <div style="font-weight:700;font-size:13px;
                            margin-bottom:4px;">{nombre}</div>
                <div style="font-size:10px;color:#666;margin-bottom:6px;">
                    Total víctimas: <b>{int(total):,}</b>
                    {"· " + str(year) if year else ""}
                </div>
                <table style="width:100%;border-collapse:collapse;">
                    <thead>
                        <tr style="border-bottom:1px solid #ddd;">
                            <th style="font-size:10px;color:#888;
                                       font-weight:600;padding-bottom:3px;
                                       text-align:left;">Tipo de evento</th>
                            <th style="font-size:10px;color:#888;
                                       font-weight:600;padding-bottom:3px;
                                       text-align:right;">Víctimas</th>
                        </tr>
                    </thead>
                    <tbody>{rows_html}</tbody>
                </table>
            </div>"""

        icon_html = """
            <div style="
                background:#dc2626;
                border:2px solid #991b1b;
                border-radius:50%;
                width:26px; height:26px;
                display:flex; align-items:center;
                justify-content:center;
                box-shadow:0 2px 4px rgba(0,0,0,0.35);
                font-size:13px;
            ">🕊️</div>"""

        folium.Marker(
            location=[lat, lng],
            icon=folium.DivIcon(
                html        = icon_html,
                icon_size   = (30, 30),
                icon_anchor = (15, 15),
            ),
            tooltip=folium.Tooltip(tooltip_html, max_width=280),
        ).add_to(m)

    return {
        "type":  "victim",
        "label": layer.label,
    }
def _add_hatch_layer(m: folium.Map, layer: HatchLayer, dept_ids=()) -> dict | None:
    """
    Renderiza polígonos con patrón SVG de líneas diagonales.
    Inyecta un <pattern> en el SVG de Leaflet y lo usa como fillColor.
    """
    geojson  = layer.get_geojson(dept_ids=dept_ids)
    features = geojson.get("features", [])

    if not features:
        st.warning(f"Sin datos para **{layer.label}**.")
        return None

    pattern_id = f"hatch_{layer.id}"
    spacing    = layer.hatch_spacing

    # SVG pattern inyectado en el mapa como elemento HTML
    pattern_svg = f"""
    <svg width="0" height="0" style="position:absolute">
        <defs>
            <pattern id="{pattern_id}"
                     patternUnits="userSpaceOnUse"
                     width="{spacing}" height="{spacing}"
                     patternTransform="rotate(45)">
                <line x1="0" y1="0" x2="0" y2="{spacing}"
                      stroke="{layer.hatch_color}"
                      stroke-width="{layer.hatch_weight}"/>
            </pattern>
        </defs>
    </svg>"""

    m.get_root().html.add_child(folium.Element(pattern_svg))

    def style_fn(feature):
        return {
            "fillColor":   f"url(#{pattern_id})",
            "fillOpacity": 1,
            "color":       layer.border_color,
            "weight":      layer.border_weight,
            "opacity":     0.9,
        }

    def highlight_fn(feature):
        return {
            "color":  layer.border_color,
            "weight": layer.border_weight + 1.5,
        }

    folium.GeoJson(
        geojson,
        name               = layer.label,
        style_function     = style_fn,
        highlight_function = highlight_fn,
        tooltip            = folium.GeoJsonTooltip(
            fields   = ["nombre"],
            aliases  = ["Municipio PDET:"],
            sticky   = False,
        ),
    ).add_to(m)

    return {
        "type":         "hatch",
        "label":        layer.label,
        "hatch_color":  layer.hatch_color,
        "border_color": layer.border_color,
        "spacing":      layer.hatch_spacing,
    }

def _add_line_layer(m, layer):
    geojson = layer.get_geojson()
    if not geojson.get("features"):
        st.warning(f"Sin datos para **{layer.label}**.")
        return
    style = layer.get_folium_style()
    folium.GeoJson(
        geojson,
        name           = layer.label,
        style_function = lambda f: style,
    ).add_to(m)
def _add_icon_scale_layer(
    m: folium.Map, layer: IconScaleLayer,
    year: int, dept_ids=()
) -> dict | None:
    """
    Renderiza un icono por municipio con color de fondo
    proporcional al valor del indicador.
    """
    geojson  = layer.get_geojson(year=year, dept_ids=dept_ids)
    features = geojson.get("features", [])

    if not features:
        st.warning(
            f"Sin datos para **{layer.label}**"
            + (f" ({year})" if year else "") + "."
        )
        return None

    values  = [
        f["properties"]["valor"]
        for f in features
        if f["properties"].get("valor") is not None
    ]
    if not values:
        return None

    val_min = min(values)
    val_max = max(values)

    colormap = cm.LinearColormap(
        colors = [layer.color_low, layer.color_high],
        vmin   = val_min,
        vmax   = val_max,
    )

    for feat in features:
        props  = feat["properties"]
        valor  = props.get("valor")
        if valor is None:
            continue

        coords   = feat["geometry"]["coordinates"]
        #lat, lng = coords[1], coords[0]
        lat      = coords[1] + layer.offset[0]
        lng      = coords[0] + layer.offset[1]   
        bg_color = colormap(valor)
        nombre   = props.get("nombre", "—")
        size     = layer.icon_size

        # Icono con fondo de color según valor
        icon_html = f"""
            <div style="
                background: {bg_color};
                border: 2px solid rgba(0,0,0,0.25);
                border-radius: 6px;
                width: {size}px;
                height: {size}px;
                display: flex;
                align-items: center;
                justify-content: center;
                font-size: {size * 0.55:.0f}px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.3);
            ">{layer.icon}</div>"""

        unit_str = f" {layer.value_unit}" if layer.value_unit else ""
        tooltip_html = f"""
            <div style="font-family:sans-serif;min-width:160px;">
                <div style="font-weight:700;font-size:13px;
                            margin-bottom:4px;">{nombre}</div>
                <div style="display:flex;justify-content:space-between;
                            align-items:center;gap:12px;">
                    <span style="font-size:11px;color:#555;">
                        {props.get('indicador', layer.label)}
                    </span>
                    <span style="font-size:13px;font-weight:700;
                                 color:{bg_color};">
                        {valor:,.1f}{unit_str}
                    </span>
                </div>
                {"<div style='font-size:10px;color:#999;margin-top:3px;'>Año: " + str(year) + "</div>" if year else ""}
            </div>"""

        folium.Marker(
            location = [lat, lng],
            icon     = folium.DivIcon(
                html        = icon_html,
                icon_size   = (size + 4, size + 4),
                icon_anchor = ((size + 4) // 2, (size + 4) // 2),
            ),
            tooltip = folium.Tooltip(tooltip_html),
        ).add_to(m)

    return {
        "type":       "icon_scale",
        "label":      layer.value_label or layer.label,
        "icon":       layer.icon,
        "color_low":  layer.color_low,
        "color_high": layer.color_high,
        "val_min":    f"{val_min:,.1f}",
        "val_mid":    f"{(val_min + val_max) / 2:,.1f}",
        "val_max":    f"{val_max:,.1f}",
        "unit":       layer.value_unit,
    }

def _build_panel_a_html(data: dict, year: int) -> str:
    nombre = data.get("name_subregion", "—")
    year_v = data.get("year", year)

    def pct_bar(label, value, color, bold=False):
        if value is None:
            return ""
        pct    = min(float(value), 100)
        weight = "700" if bold else "500"
        return f"""
        <div style="margin-bottom:6px;">
            <div style="display:flex;justify-content:space-between;
                        font-size:11px;font-weight:{weight};margin-bottom:2px;">
                <span>{label}</span>
                <span style="color:{color};">{pct:.1f}%</span>
            </div>
            <div style="background:#f0f0f0;border-radius:3px;height:5px;">
                <div style="width:{pct}%;background:{color};
                            border-radius:3px;height:5px;"></div>
            </div>
        </div>"""

    def row(label, value, color="#555", indent=False):
        if value is None:
            return ""
        pad = "padding-left:10px;" if indent else ""
        return f"""
        <div style="display:flex;justify-content:space-between;
                    font-size:11px;padding:1px 0;{pad}">
            <span style="color:#555;">{label}</span>
            <span style="color:{color};font-weight:600;">{float(value):.1f}%</span>
        </div>"""

    def section(title):
        return f"""<div style="font-size:11px;font-weight:700;color:#333;
                               margin:8px 0 4px 0;">{title}</div>"""

    content = f"""
        <div style="font-size:11px;color:#8b949e;text-transform:uppercase;
                    letter-spacing:1px;margin-bottom:2px;">Subregión</div>
        <div style="font-size:15px;font-weight:700;margin-bottom:2px;">{nombre}</div>
        <div style="font-size:11px;color:#58a6ff;margin-bottom:8px;">Año {year_v}</div>
        <hr style="margin:6px 0;border-color:#eee;">

        {section("🍽️ Seguridad Alimentaria")}
        {pct_bar("Inseguridad alimentaria", data.get("pct_u18_food_insecurity"), "#cb181d", bold=True)}
        {row("↳ Leve",     data.get("pct_u18_food_insecurity_mild"),     indent=True)}
        {row("↳ Moderada", data.get("pct_u18_food_insecurity_moderate"), indent=True)}
        {row("↳ Severa",   data.get("pct_u18_food_insecurity_severe"),   "#cb181d", indent=True)}
        {pct_bar("Seguridad alimentaria", data.get("pct_u18_food_security"), "#2ca25f")}

        <hr style="margin:6px 0;border-color:#eee;">
        {section("👶 Estado nutricional &lt;5 años")}
        {row("Desnutrición aguda severa",   data.get("pct_u5_wasting_severe"),   "#cb181d")}
        {row("Desnutrición aguda moderada", data.get("pct_u5_wasting_moderate"), "#fd8d3c")}
        {row("Bajo peso",                   data.get("pct_u5_underweight"),      "#fd8d3c")}
        {row("Retraso en talla",            data.get("pct_u5_stunting"),         "#fd8d3c")}
        {row("Sobrepeso",                   data.get("pct_u5_overweight"),       "#6baed6")}
        {row("Obesidad",                    data.get("pct_u5_obesity"),          "#6baed6")}

        <hr style="margin:6px 0;border-color:#eee;">
        {section("🧒 Estado nutricional 5-18 años")}
        {row("Retraso en talla",  data.get("pct_5_18_stunting"),   "#fd8d3c")}
        {row("IMC normal",        data.get("pct_5_18_bmi_normal"), "#2ca25f")}
        {row("Sobrepeso",         data.get("pct_5_18_overweight"), "#6baed6")}
        {row("Obesidad",          data.get("pct_5_18_obesity"),    "#6baed6")}
    """

    return f"""
    <style>
        #panel-a-cb  {{ display: none; }}
        #panel-a-box {{
            position:   fixed;
            top:        80px;
            right:      12px;
            z-index:    1002;
            background: white;
            border-radius: 10px;
            box-shadow: 0 4px 16px rgba(0,0,0,0.2);
            font-family: sans-serif;
            width:      240px;
            max-height: 75vh;
            overflow-y: auto;
            padding:    14px;
            display:    block;
        }}
        #panel-a-cb:checked ~ #panel-a-box {{
            display: none;
        }}
        #panel-a-toggle {{
            position:  fixed;
            top:       80px;
            right:     12px;
            z-index:   1003;
            display:   none;
        }}
        #panel-a-cb:checked ~ #panel-a-toggle {{
            display: block;
        }}
        #panel-a-toggle label {{
            background:    white;
            border:        1px solid #ddd;
            border-radius: 6px;
            padding:       4px 10px;
            font-size:     11px;
            font-weight:   700;
            color:         #333;
            cursor:        pointer;
            box-shadow:    0 2px 6px rgba(0,0,0,0.15);
        }}
    </style>

    <input type="checkbox" id="panel-a-cb">

    <div id="panel-a-box">
        <div style="display:flex;justify-content:space-between;
                    align-items:center;margin-bottom:8px;">
            <span style="font-size:10px;color:#aaa;font-weight:700;">
                Seguridad Alimentaria
            </span>
            <label for="panel-a-cb" style="
                cursor:pointer;font-size:11px;color:#666;
                background:#f5f5f5;border:1px solid #ddd;
                border-radius:4px;padding:2px 8px;">
                ✕ Ocultar
            </label>
        </div>
        {content}
    </div>

    <div id="panel-a-toggle">
        <label for="panel-a-cb" style="
            background:white;border:1px solid #ddd;
            border-radius:6px;padding:4px 10px;
            font-size:11px;font-weight:700;color:#333;
            cursor:pointer;box-shadow:0 2px 6px rgba(0,0,0,0.15);">
            📋 Seguridad Alimentaria
        </label>
    </div>
    """
def _build_panel_b_html(data: dict, year: int, cat_id: str) -> str:
    from config.layers_config import CATEGORIES
    cat_cfg   = CATEGORIES.get(cat_id, {})
    muni_name = st.session_state.get("clicked_muni_name", "—")

    def section(title):
        return f"""<div style="font-size:11px;font-weight:700;color:#333;
                               margin:8px 0 4px 0;">{title}</div>"""

    def kv(label, value, unit=""):
        if value is None:
            return ""
        try:
            formatted = f"{float(value):,.1f}"
        except Exception:
            formatted = str(value)
        val_str = f"{formatted} {unit}".strip() if unit else formatted
        return f"""
        <div style="display:flex;justify-content:space-between;
                    font-size:11px;padding:1px 0;
                    border-bottom:1px solid #f5f5f5;">
            <span style="color:#555;">{label}</span>
            <span style="font-weight:600;color:#222;">{val_str}</span>
        </div>"""

    content = f"""
        <div style="font-size:11px;color:#8b949e;text-transform:uppercase;
                    letter-spacing:1px;margin-bottom:2px;">Municipio</div>
        <div style="font-size:15px;font-weight:700;margin-bottom:2px;">{muni_name}</div>
        <div style="font-size:11px;color:#58a6ff;margin-bottom:8px;">
            {cat_cfg.get('icon','')} {cat_cfg.get('label','')} · {year or ''}
        </div>
        <hr style="margin:6px 0;border-color:#eee;">
    """

    if cat_id == "salud":
        content += section("🏥 Salud")
        content += kv("Desnutrición aguda <5",   data.get("desnutricion_aguda"),      "casos")
        content += kv("Mortalidad malnutrición", data.get("mortalidad_malnutricion"),  "casos")
        content += kv("Bajo peso al nacer",      data.get("bajo_peso_nacer"),          "casos")

    elif cat_id == "socioeconomico":
        content += section("📊 Socioeconómico")
        content += kv("Pobreza monetaria",  data.get("pobreza_monetaria"),  "%")
        content += kv("Población empleada", data.get("poblacion_empleada"), "personas")
        content += kv("Cobertura escolar",  data.get("cobertura_escolar"),  "estudiantes")
        edu = data.get("educacion_superior", {})
        if edu and any(v for v in edu.values() if v):
            content += section("📚 Educación Superior")
            content += kv("Técnico",         edu.get("prof_technician"), "")
            content += kv("Tecnólogo",       edu.get("technologist"),    "")
            content += kv("Universitario",   edu.get("university"),      "")
            content += kv("Especialización", edu.get("specialization"),  "")
            content += kv("Maestría",        edu.get("master"),          "")
            content += kv("Doctorado",       edu.get("doctorate"),       "")

    elif cat_id == "ambiente":
        content += section("💧 Calidad del Agua")
        content += kv("IRCA", data.get("irca", {}).get("irca_value"), "%")
        content += section("🛢️ Hidrocarburos")
        content += kv("Petróleo",  data.get("petroleo", {}).get("produccion"), "bls")
        content += kv("Gas",       data.get("gas",      {}).get("produccion"), "")
        content += kv("Regalías",  data.get("regalias", {}).get("total"),      "COP")
        for mineral in data.get("minerales", []):
            content += kv(mineral.get("mineral_resource", "—"), mineral.get("total"), "COP")
        cultivos = data.get("cultivos", [])
        if cultivos:
            content += section("🌿 Cultivos ilícitos")
            for c in cultivos:
                content += kv(c.get("id_illicit_crop", "—"), c.get("total"), "ha")
        clima = data.get("clima", [])
        if clima:
            content += section("🌦️ Clima")
            for c in clima:
                content += kv(
                    f"{c.get('variable','')} ({c.get('annual_aggregation','')})",
                    c.get("value"), ""
                )

    elif cat_id == "agropecuario":
        pecuario = data.get("pecuario", [])
        if pecuario:
            content += section("🐄 Censo Pecuario")
            for p in pecuario:
                try:
                    val = f"{float(p.get('total_animals', 0)):,.0f} animales"
                except Exception:
                    val = "—"
                content += kv(p.get("type", "—"), val, "")
        mercados = data.get("mercados", [])
        if mercados:
            content += section("🛒 Mercados Campesinos")
            for m in mercados:
                content += f"<div style='font-size:11px;padding:1px 0;'>📍 {m.get('name','—')}</div>"

    elif cat_id == "conflicto":
        victimas = data.get("victimas", [])
        if victimas:
            content += section("🕊️ Víctimas")
            for v in victimas:
                content += kv(v.get("event_name", "—"), v.get("total"), "personas")
        iraca = data.get("iraca", [])
        if iraca:
            content += section("👥 IRACA")
            for i in iraca:
                content += kv(
                    f"{i.get('type','—')} · {i.get('status','—')}",
                    i.get("total"), "personas"
                )

    return f"""
    <style>
        #panel-b-cb  {{ display: none; }}
        #panel-b-box {{
            position:   fixed;
            top:        80px;
            right:      12px;
            z-index:    1002;
            background: white;
            border-radius: 10px;
            box-shadow: 0 4px 16px rgba(0,0,0,0.2);
            font-family: sans-serif;
            width:      240px;
            max-height: 75vh;
            overflow-y: auto;
            padding:    14px;
            display:    block;
        }}
        #panel-b-cb:checked ~ #panel-b-box {{
            display: none;
        }}
        #panel-b-toggle {{
            position: fixed;
            top:      80px;
            right:    12px;
            z-index:  1003;
            display:  none;
        }}
        #panel-b-cb:checked ~ #panel-b-toggle {{
            display: block;
        }}
        #panel-b-toggle label {{
            background:    white;
            border:        1px solid #ddd;
            border-radius: 6px;
            padding:       4px 10px;
            font-size:     11px;
            font-weight:   700;
            color:         #333;
            cursor:        pointer;
            box-shadow:    0 2px 6px rgba(0,0,0,0.15);
        }}
    </style>

    <input type="checkbox" id="panel-b-cb">

    <div id="panel-b-box">
        <div style="display:flex;justify-content:space-between;
                    align-items:center;margin-bottom:8px;">
            <span style="font-size:10px;color:#aaa;font-weight:700;">
                Detalle municipio
            </span>
            <label for="panel-b-cb" style="
                cursor:pointer;font-size:11px;color:#666;
                background:#f5f5f5;border:1px solid #ddd;
                border-radius:4px;padding:2px 8px;">
                ✕ Ocultar
            </label>
        </div>
        {content}
    </div>

    <div id="panel-b-toggle">
        <label for="panel-b-cb" style="
            background:white;border:1px solid #ddd;
            border-radius:6px;padding:4px 10px;
            font-size:11px;font-weight:700;color:#333;
            cursor:pointer;box-shadow:0 2px 6px rgba(0,0,0,0.15);">
            📋 Detalle municipio
        </label>
    </div>
    """
def _add_basemap(m, name):
    url = MAP_CONFIG["basemaps"].get(name, "OpenStreetMap")
    if url == "OpenStreetMap":
        folium.TileLayer("OpenStreetMap").add_to(m)
    else:
        folium.TileLayer(tiles=url, name=name, attr=name, max_zoom=20).add_to(m)