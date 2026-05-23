"""
core/layer.py — Árbol de clases para capas geográficas

Jerarquía:
    GeoLayer (abstracta)
    ├── PolygonLayer      → polígonos simples desde PostGIS
    ├── PointLayer        → puntos desde PostGIS
    └── ChoroplethLayer   → polígonos coloreados por valor numérico
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import streamlit as st
from core.db import query_geojson, query_rows


# ─────────────────────────────────────────────────────────────
#  CLASE BASE
# ─────────────────────────────────────────────────────────────

@dataclass
class GeoLayer(ABC):
    id: str
    label: str
    visible: bool = True
    color: str = "#3b82f6"
    opacity: float = 0.5
    description: str = ""
    z_index: int = 10 
    filterable_by_dept: bool = True
    category: str  = ""
    percentile_threshold: float = None
    source_name:          str  = ""  
    source_url:           str  = ""   
    @abstractmethod
    def get_geojson(self, **kwargs) -> dict:
        pass

    @abstractmethod
    def get_folium_style(self) -> dict:
        pass

    def __repr__(self):
        return f"{self.__class__.__name__}(id='{self.id}', label='{self.label}')"


# ─────────────────────────────────────────────────────────────
#  CAPA COROPLÉTICA
# ─────────────────────────────────────────────────────────────

@dataclass
class ChoroplethLayer(GeoLayer):
    """
    Polígonos coloreados según el valor de una variable numérica.
    Hace JOIN entre tabla geométrica y tabla de datos.
    """
    geo_table:    str = ""
    geo_id_col:   str = "id_subregion"
    geo_name_col: str = "name_subregion"
    geo_geom_col: str = "geometry"
    data_table:   str = ""
    data_id_col:  str = "id_subregion"
    value_col:    str = ""
    value_label:  str = ""
    extra_cols:   list = field(default_factory=list)
    color_low:    str = "#fff5f0"
    color_high:   str = "#cb181d"
    year_col:     str = "year"
    filter_sql:   str = "" 
    border_visible: bool = True

    def get_geojson(self, year: int = None, **kwargs) -> dict:
        return _fetch_choropleth_geojson(
            layer_id     = self.id,
            layer_label  = self.label,
            geo_table    = self.geo_table,
            geo_id_col   = self.geo_id_col,
            geo_name_col = self.geo_name_col,
            geo_geom_col = self.geo_geom_col,
            data_table   = self.data_table,
            data_id_col  = self.data_id_col,
            value_col    = self.value_col,
            value_label  = self.value_label,
            year_col     = self.year_col,
            extra_cols   = tuple(self.extra_cols),
            year         = year,
            dept_ids     = kwargs.get("dept_ids", ()),
            filter_sql   = self.filter_sql,
        )

    def get_folium_style(self) -> dict:
        return {"weight": 1.2, "opacity": 0.8, "fillOpacity": 0.75}


@st.cache_data(ttl=300, show_spinner=False)
@st.cache_data(ttl=300, show_spinner=False)
def _fetch_choropleth_geojson(
    layer_id, layer_label, geo_table, geo_id_col, geo_name_col,
    geo_geom_col, data_table, data_id_col, value_col, value_label,
    year_col, extra_cols, year, dept_ids=(),
    filter_sql="" 
) -> dict:
    extra_json  = ", ".join([f"'{c}', p.\"{c}\"" for c in extra_cols if c != year_col])
    year_filter = f"AND p.\"{year_col}\" = {year}" if year else ""
    dept_filter = ""
    if dept_ids:
        ids_str     = ", ".join([f"'{d}'" for d in dept_ids])
        dept_filter = f"AND s.id_dept IN ({ids_str})"
    extra_filter = f"AND p.{filter_sql}" if filter_sql else ""  

    query = f"""
        SELECT json_build_object(
            'type',     'Feature',
            'geometry', ST_AsGeoJSON(s."{geo_geom_col}")::json,
            'properties', json_build_object(
                'layer_id',    '{layer_id}',
                'layer_label', '{layer_label}',
                'id',          s."{geo_id_col}",
                'nombre',      s."{geo_name_col}",
                'valor',       AVG(p."{value_col}"),
                'indicador',   '{value_label}',
                '{year_col}',  p."{year_col}"
                {', ' + extra_json if extra_json else ''}
            )
        ) AS feature
        FROM "{geo_table}" s
        JOIN "{data_table}" p ON s."{geo_id_col}" = p."{data_id_col}"
        WHERE p."{value_col}" IS NOT NULL
        {year_filter}
        {dept_filter}
        {extra_filter}
        GROUP BY s."{geo_id_col}", s."{geo_name_col}", s."{geo_geom_col}", p."{year_col}"
        ORDER BY AVG(p."{value_col}") DESC
    """
    
    features = query_geojson(query)
    return {"type": "FeatureCollection", "features": features}

# ─────────────────────────────────────────────────────────────
#  CAPA DE POLÍGONOS SIMPLE
# ─────────────────────────────────────────────────────────────
@dataclass
class PolygonLayer(GeoLayer):
    table:        str = ""
    geom_col:     str = "geom"
    properties:   list = field(default_factory=list)
    filter_sql:   str = ""
    max_features: int = 5000
    weight:       float = 1.5
    color_categorical: bool  = False

    def get_geojson(self, **kwargs) -> dict:
        return _fetch_polygon_geojson(
            layer_id     = self.id,
            layer_label  = self.label,
            table        = self.table,
            geom_col     = self.geom_col,
            properties   = tuple(self.properties),
            filter_sql   = self.filter_sql,
            max_features = self.max_features,
            dept_ids     = kwargs.get("dept_ids", ()),
        )

    def get_folium_style(self) -> dict:
        return {"color": self.color, "fillColor": self.color,
                "weight": self.weight, "opacity": 0.9, "fillOpacity": self.opacity}


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_polygon_geojson(
    layer_id, layer_label, table, geom_col,
    properties, filter_sql, max_features, dept_ids=()
) -> dict:
    props_json  = ", ".join([f"'{p}', \"{p}\"" for p in properties])
    where_parts = []
    if filter_sql:
        where_parts.append(filter_sql)
    if dept_ids:
        ids_str = ", ".join([f"'{d}'" for d in dept_ids])
        where_parts.append(f"id_dept IN ({ids_str})")
    where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
    query = f"""
        SELECT json_build_object(
            'type', 'Feature',
            'geometry', ST_AsGeoJSON(ST_Transform("{geom_col}", 4326))::json,
            'properties', json_build_object(
                'layer_id',    '{layer_id}',
                'layer_label', '{layer_label}'
                {', ' + props_json if props_json else ''}
            )
        ) AS feature
        FROM "{table}"
        {where}
        {"AND" if where_parts else "WHERE"} "{geom_col}" IS NOT NULL
        LIMIT {max_features}
    """
   
    features = query_geojson(query)
    return {"type": "FeatureCollection", "features": features}
# ─────────────────────────────────────────────────────────────
#  CAPA DE PUNTOS
# ─────────────────────────────────────────────────────────────

@dataclass
class PointLayer(GeoLayer):
    table:        str = ""
    geom_col:     str = "geom"
    properties:   list = field(default_factory=list)
    filter_sql:   str = ""
    max_features: int = 10000
    radius:       int = 6

    @st.cache_data(ttl=300, show_spinner=False)
    def get_geojson(_self, **kwargs) -> dict:
        props_json = ", ".join([f"'{p}', \"{p}\"" for p in _self.properties])
        where = f"WHERE {_self.filter_sql}" if _self.filter_sql else ""
        query = f"""
            SELECT json_build_object(
                'type', 'Feature',
                'geometry', ST_AsGeoJSON(ST_Transform("{_self.geom_col}", 4326))::json,
                'properties', json_build_object(
                    'layer_id', '{_self.id}',
                    'layer_label', '{_self.label}'
                    {', ' + props_json if props_json else ''}
                )
            ) AS feature
            FROM "{_self.table}"
            {where}
            WHERE "{_self.geom_col}" IS NOT NULL
            LIMIT {_self.max_features}
        """
        features = query_geojson(query)
        return {"type": "FeatureCollection", "features": features}

    def get_folium_style(self) -> dict:
        return {"color": self.color, "fillColor": self.color,
                "radius": self.radius, "weight": 1,
                "opacity": 1, "fillOpacity": self.opacity}


# ─────────────────────────────────────────────────────────────
#  CAPA DE BURBUJAS  — puntos con tamaño y color por indicador
# ─────────────────────────────────────────────────────────────

@dataclass
class BubbleLayer(GeoLayer):
    """
    Puntos cuyo tamaño Y color varían según el valor de una variable numérica.
    Hace JOIN entre una tabla de datos y dim_divipola para obtener la geometría.

    Uso en layers_config.py:
        BubbleLayer(
            id           = "mortalidad_malnutricion",
            label        = "Mortalidad por malnutrición",
            data_table   = "mortality_malnutrition",
            data_id_col  = "id_mun",
            geo_table    = "dim_divipola",
            geo_id_col   = "id_mun",
            geo_geom_col = "geometry",
            value_col    = "total_cases",
            value_label  = "Casos totales",
            agg_func     = "SUM",       # SUM, AVG, MAX — cómo agregar si hay varios años
            year_col     = "year",
            color_low    = "#fef0d9",
            color_high   = "#b30000",
            radius_min   = 4,
            radius_max   = 20,
        )
    """
    data_table:   str = ""
    data_id_col:  str = "id_mun"
    geo_table:    str = "dim_divipola"
    geo_id_col:   str = "id_mun"
    geo_geom_col: str = "geometry"
    value_col:    str = "total_cases"
    value_label:  str = ""
    agg_func:     str = "SUM"
    year_col:     str = "year"
    extra_cols:   list = field(default_factory=list)

    # Escala de color
    color_low:    str = "#feedd9"
    color_high:   str = "#b30000"

    # Escala de tamaño en píxeles
    radius_min:   int = 4
    radius_max:   int = 20

    # Desplazamiento del centroide en grados (lat, lng)
    # Útil para separar visualmente capas que comparten la misma geometría
    offset:       tuple = (0.0, 0.0)

    def get_geojson(self, year: int = None, **kwargs) -> dict:
        return _fetch_bubble_geojson(
            layer_id     = self.id,
            layer_label  = self.label,
            data_table   = self.data_table,
            data_id_col  = self.data_id_col,
            geo_table    = self.geo_table,
            geo_id_col   = self.geo_id_col,
            geo_geom_col = self.geo_geom_col,
            value_col    = self.value_col,
            value_label  = self.value_label,
            agg_func     = self.agg_func,
            year_col     = self.year_col,
            extra_cols   = tuple(self.extra_cols),
            year         = year,
            dept_ids     = kwargs.get("dept_ids", ()),
        )

    def get_folium_style(self) -> dict:
        return {
            "weight":      1,
            "opacity":     0.9,
            "fillOpacity": 0.75,
        }


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_bubble_geojson(
    layer_id, layer_label, data_table, data_id_col,
    geo_table, geo_id_col, geo_geom_col,
    value_col, value_label, agg_func, year_col,
    extra_cols, year, dept_ids=()
) -> dict:
    extra_json  = ", ".join([f"'{c}', d.\"{c}\"" for c in extra_cols])
    year_filter = f"AND d.\"{year_col}\" = {year}" if year else ""
    dept_filter = ""
    if dept_ids:
        ids_str     = ", ".join([f"'{d}'" for d in dept_ids])
        dept_filter = f"AND g.id_dept IN ({ids_str})"

    query = f"""
        SELECT json_build_object(
            'type',     'Feature',
            'geometry', ST_AsGeoJSON(ST_Centroid(g."{geo_geom_col}"))::json,
            'properties', json_build_object(
                'layer_id',    '{layer_id}',
                'layer_label', '{layer_label}',
                'id_muni',     g."{geo_id_col}",
                'nombre',      g.name_mun,
                'valor',       {agg_func}(d."{value_col}"),
                'indicador',   '{value_label}'
                {', ' + extra_json if extra_json else ''}
            )
        ) AS feature
        FROM "{geo_table}" g
        JOIN "{data_table}" d ON g."{geo_id_col}" = d."{data_id_col}"
        WHERE d."{value_col}" IS NOT NULL
        {year_filter}
        {dept_filter}
        GROUP BY g."{geo_id_col}", g.name_mun, g."{geo_geom_col}"
        ORDER BY {agg_func}(d."{value_col}") DESC
    """
    features = query_geojson(query)
    return {"type": "FeatureCollection", "features": features}

# ─────────────────────────────────────────────────────────────
#  CAPA DE BARRAS COMPARATIVAS — mini gráfico por departamento
# ─────────────────────────────────────────────────────────────

@dataclass
class BarChartLayer(GeoLayer):
    """
    Renderiza mini barras comparativas sobre cada departamento.
    Hace JOIN entre mp_sex_head_hh, dim_departament y dim_gender.
    Cada barra representa el valor de un grupo (hombre/mujer).
    """
    data_table:   str = "mp_sex_head_hh"
    geo_table:    str = "dim_departament"
    geo_id_col:   str = "id_dept"
    geo_geom_col: str = "geometry"
    value_col:    str = "mp_idx_val"
    year_col:     str = "year"
    group_col:    str = "id_gender"
    group_table:  str = "dim_gender"
    group_id_col: str = "id_gender"
    group_label_col: str = "gender"
    value_label:  str = ""

    # Colores por grupo — orden debe coincidir con valores de dim_gender
    bar_colors:   tuple = ("#3b82f6", "#ec4899")   # azul=hombre, rosa=mujer
    bar_width:    int = 12    # ancho de cada barra en píxeles
    bar_max_height: int = 40  # altura máxima de barra en píxeles

    def get_geojson(self, year: int = None, **kwargs) -> dict:
        return _fetch_bar_chart_geojson(
            data_table      = self.data_table,
            geo_table       = self.geo_table,
            geo_id_col      = self.geo_id_col,
            geo_geom_col    = self.geo_geom_col,
            value_col       = self.value_col,
            year_col        = self.year_col,
            group_col       = self.group_col,
            group_table     = self.group_table,
            group_id_col    = self.group_id_col,
            group_label_col = self.group_label_col,
            layer_id        = self.id,
            layer_label     = self.label,
            value_label     = self.value_label,
            year            = year,
            dept_ids        = kwargs.get("dept_ids", ()),
        )

    def get_folium_style(self) -> dict:
        return {}


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_bar_chart_geojson(
    data_table, geo_table, geo_id_col, geo_geom_col,
    value_col, year_col, group_col, group_table,
    group_id_col, group_label_col,
    layer_id, layer_label, value_label, year, dept_ids=()
) -> dict:
    year_filter = f"AND d.\"{year_col}\" = {year}" if year else ""
    dept_filter = ""
    if dept_ids:
        ids_str     = ", ".join([f"'{d}'" for d in dept_ids])
        dept_filter = f"AND g.\"{geo_id_col}\" IN ({ids_str})"

    query = f"""
        SELECT
            g."{geo_id_col}"                                    AS id_dept,
            g.name_dept,
            ST_AsGeoJSON(ST_Centroid(g."{geo_geom_col}"))::json AS geom,
            json_agg(
                json_build_object(
                    'label', gr."{group_label_col}",
                    'value', d."{value_col}"
                )
                ORDER BY gr."{group_id_col}"
            ) AS groups
        FROM "{geo_table}" g
        JOIN "{data_table}" d  ON g."{geo_id_col}" = d."{geo_id_col}"
        JOIN "{group_table}" gr ON d."{group_col}" = gr."{group_id_col}"
        WHERE d."{value_col}" IS NOT NULL
        {year_filter}
        {dept_filter}
        GROUP BY g."{geo_id_col}", g.name_dept, g."{geo_geom_col}"
        ORDER BY g.name_dept
    """
    rows = query_rows(query)
    features = []
    for row in rows:
        features.append({
            "type": "Feature",
            "geometry": row["geom"],
            "properties": {
                "layer_id":    layer_id,
                "layer_label": layer_label,
                "id_dept":     row["id_dept"],
                "nombre":      row["name_dept"],
                "groups":      row["groups"],
                "value_label": value_label,
            }
        })
    return {"type": "FeatureCollection", "features": features}

# ─────────────────────────────────────────────────────────────
#  CAPA DE BENEFICIARIOS — icono por municipio, consolidable
# ─────────────────────────────────────────────────────────────

@dataclass
class BeneficiaryLayer(GeoLayer):
    """
    Muestra un icono en cada municipio donde hay beneficiarios.
    Varias capas BeneficiaryLayer se consolidan en un solo icono
    por municipio con badge numérico si hay más de un programa.
    """
    data_table:        str = ""
    geo_table:         str = "dim_divipola"
    geo_id_col:        str = "id_mun"
    geo_geom_col:      str = "geometry"
    muni_id_col:       str = "id_mun"
    year_col:          str = "year"
    count_col:         str = "beneficiaries_count"  # columna numérica a sumar
    program_name:      str = ""   # nombre para el tooltip
    extra_tooltip_cols: tuple = ()  # columnas adicionales a mostrar en tooltip

    def get_geojson(self, year: int = None, **kwargs) -> dict:
        return _fetch_beneficiary_geojson(
            layer_id          = self.id,
            program_name      = self.program_name or self.label,
            data_table        = self.data_table,
            geo_table         = self.geo_table,
            geo_id_col        = self.geo_id_col,
            geo_geom_col      = self.geo_geom_col,
            muni_id_col       = self.muni_id_col,
            year_col          = self.year_col,
            count_col         = self.count_col,
            extra_tooltip_cols= self.extra_tooltip_cols,
            year              = year,
            dept_ids           = kwargs.get("dept_ids", ()),
        )

    def get_folium_style(self) -> dict:
        return {}


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_beneficiary_geojson(
    layer_id, program_name, data_table, geo_table,
    geo_id_col, geo_geom_col, muni_id_col, year_col,
    count_col, extra_tooltip_cols, year, dept_ids=()
) -> dict:
    year_filter  = f"AND d.\"{year_col}\" = {year}" if year else ""
    extra_json   = "".join([f", '{c}', MAX(d.\"{c}\")" for c in extra_tooltip_cols])
    dept_filter  = ""
    if dept_ids:
        ids_str     = ", ".join([f"'{d}'" for d in dept_ids])
        dept_filter = f"AND g.id_dept IN ({ids_str})"

    query = f"""
        SELECT json_build_object(
            'type',     'Feature',
            'geometry', ST_AsGeoJSON(ST_Centroid(g."{geo_geom_col}"))::json,
            'properties', json_build_object(
                'layer_id',     '{layer_id}',
                'program_name', '{program_name}',
                'id_mun',       g."{geo_id_col}",
                'nombre',       g.name_mun,
                'total',        SUM(d."{count_col}")
                {extra_json}
            )
        ) AS feature
        FROM "{geo_table}" g
        JOIN "{data_table}" d ON g."{geo_id_col}" = d."{muni_id_col}"
        WHERE d."{count_col}" IS NOT NULL
        {year_filter}
        {dept_filter}
        GROUP BY g."{geo_id_col}", g.name_mun, g."{geo_geom_col}"
        ORDER BY g.name_mun
    """
    features = query_geojson(query)
    return {"type": "FeatureCollection", "features": features}

# ─────────────────────────────────────────────────────────────
#  CAPA DE LÍNEAS — red vial u otras geometrías lineales
# ─────────────────────────────────────────────────────────────

@dataclass
class LineLayer(GeoLayer):
    """
    Renderiza geometrías lineales (carreteras, ríos, etc.) desde PostGIS.
    """
    table:        str   = ""
    geom_col:     str   = "geometry"
    properties:   tuple = ()
    filter_sql:   str   = ""
    max_features: int   = 10000
    weight:       float = 1.5    # grosor de la línea
    dash_array:   str   = ""     # ej: "5 5" para línea punteada, "" para sólida

    def get_geojson(self, **kwargs) -> dict:
        return _fetch_line_geojson(
            layer_id     = self.id,
            layer_label  = self.label,
            table        = self.table,
            geom_col     = self.geom_col,
            properties   = self.properties,
            filter_sql   = self.filter_sql,
            max_features = self.max_features,
            dept_ids     = (),
        )

    def get_folium_style(self) -> dict:
        return {
            "color":     self.color,
            "weight":    self.weight,
            "opacity":   self.opacity,
            "dashArray": self.dash_array,
        }


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_line_geojson(
    layer_id, layer_label, table, geom_col,
    properties, filter_sql, max_features
) -> dict:
    props_json = ", ".join([f"'{p}', \"{p}\"" for p in properties])
    where = f"WHERE {filter_sql}" if filter_sql else ""
    query = f"""
        SELECT json_build_object(
            'type',     'Feature',
            'geometry', ST_AsGeoJSON("{geom_col}")::json,
            'properties', json_build_object(
                'layer_id',    '{layer_id}',
                'layer_label', '{layer_label}'
                {', ' + props_json if props_json else ''}
            )
        ) AS feature
        FROM "{table}"
        {where}
        WHERE "{geom_col}" IS NOT NULL
        LIMIT {max_features}
    """
    features = query_geojson(query)
    return {"type": "FeatureCollection", "features": features}

# ─────────────────────────────────────────────────────────────
#  CAPA DE VÍCTIMAS — icono por municipio, tabla en tooltip
# ─────────────────────────────────────────────────────────────

@dataclass
class VictimLayer(GeoLayer):
    """
    Muestra un icono por municipio con víctimas registradas.
    El tooltip despliega una tabla con el conteo por tipo de evento.
    """
    data_table:       str   = ""
    event_table:      str   = "dim_victim_event"
    event_id_col:     str   = "id_victim_event"
    event_name_col:   str   = "event_name"
    geo_table:        str   = "dim_divipola"
    geo_id_col:       str   = "id_mun"
    geo_geom_col:     str   = "geometry"
    muni_id_col:      str   = "id_mun"
    year_col:         str   = "year"
    count_col:        str   = "victim_count"

    def get_geojson(self, year: int = None, **kwargs) -> dict:
        return _fetch_victim_geojson(
            layer_id        = self.id,
            layer_label     = self.label,
            data_table      = self.data_table,
            event_table     = self.event_table,
            event_id_col    = self.event_id_col,
            event_name_col  = self.event_name_col,
            geo_table       = self.geo_table,
            geo_id_col      = self.geo_id_col,
            geo_geom_col    = self.geo_geom_col,
            muni_id_col     = self.muni_id_col,
            year_col        = self.year_col,
            count_col       = self.count_col,
            year            = year,
            dept_ids        = kwargs.get("dept_ids", ()),
        )

    def get_folium_style(self) -> dict:
        return {}


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_victim_geojson(
    layer_id, layer_label, data_table, event_table,
    event_id_col, event_name_col, geo_table, geo_id_col,
    geo_geom_col, muni_id_col, year_col, count_col,
    year, dept_ids=()
) -> dict:
    year_filter      = f'AND v."{year_col}" = {year}' if year else ""
    year_filter_subq = f'AND "{year_col}" = {year}' if year else ""
    year_join        = f'AND v."{year_col}" = {year}' if year else ""
    dept_filter      = ""
    if dept_ids:
        ids_str     = ", ".join([f"'{d}'" for d in dept_ids])
        dept_filter = f"AND g.id_dept IN ({ids_str})"

    query = f"""
        SELECT json_build_object(
            'type',     'Feature',
            'geometry', ST_AsGeoJSON(ST_Centroid(g."{geo_geom_col}"))::json,
            'properties', json_build_object(
                'layer_id',    '{layer_id}',
                'layer_label', '{layer_label}',
                'id_mun',      g."{geo_id_col}",
                'nombre',      g.name_mun,
                'total',       SUM(v."{count_col}"),
                'events',      json_agg(
                    json_build_object(
                        'event_name', e."{event_name_col}",
                        'count',      subq.event_total
                    )
                    ORDER BY subq.event_total DESC
                )
            )
        ) AS feature
        FROM "{geo_table}" g
        JOIN (
            SELECT "{muni_id_col}", "{event_id_col}",
                   SUM("{count_col}") AS event_total
            FROM "{data_table}"
            WHERE "{count_col}" IS NOT NULL
            {year_filter_subq}
            GROUP BY "{muni_id_col}", "{event_id_col}"
        ) subq ON g."{geo_id_col}" = subq."{muni_id_col}"
        JOIN "{event_table}" e ON subq."{event_id_col}" = e."{event_id_col}"
        JOIN "{data_table}" v
          ON g."{geo_id_col}" = v."{muni_id_col}"
         AND v."{event_id_col}" = subq."{event_id_col}"
         {year_join}
        WHERE TRUE
        {dept_filter}
        GROUP BY g."{geo_id_col}", g.name_mun, g."{geo_geom_col}"
        ORDER BY g.name_mun
    """
    features = query_geojson(query)
    return {"type": "FeatureCollection", "features": features}

# ─────────────────────────────────────────────────────────────
#  CAPA HATCH — polígonos con patrón de líneas diagonales
# ─────────────────────────────────────────────────────────────

@dataclass
class HatchLayer(GeoLayer):
    """
    Renderiza polígonos con patrón de líneas diagonales (hatch).
    Útil para zonas especiales como municipios PDET.
    """
    data_table:   str   = ""
    geo_table:    str   = "dim_divipola"
    geo_id_col:   str   = "id_mun"
    geo_geom_col: str   = "geometry"
    join_col:     str   = "id_mun"        # columna en data_table que referencia geo
    has_own_geometry:  bool  = False
    name_col:     str   = ""      
    hatch_color:  str   = "#e11d48"       # color de las líneas diagonales
    hatch_weight: float = 1.0             # grosor de las líneas
    hatch_spacing: int  = 6              # separación entre líneas en píxeles
    border_color: str   = "#e11d48"       # color del contorno
    border_weight: float = 1.5

    def get_geojson(self, **kwargs) -> dict:
        return _fetch_hatch_geojson(
            layer_id         = self.id,
            layer_label      = self.label,
            data_table       = self.data_table,
            geo_table        = self.geo_table,
            geo_id_col       = self.geo_id_col,
            geo_geom_col     = self.geo_geom_col,
            join_col         = self.join_col,
            dept_ids         = kwargs.get("dept_ids", ()),
            has_own_geometry = self.has_own_geometry,
            name_col         = self.name_col,
        )

    def get_folium_style(self) -> dict:
        return {}


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_hatch_geojson(
    layer_id, layer_label, data_table,
    geo_table, geo_id_col, geo_geom_col,
    join_col, dept_ids=(), has_own_geometry=False,
    name_col=""
) -> dict:

    if has_own_geometry:
        dept_filter  = ""
        if dept_ids:
            ids_str     = ", ".join([f"'{d}'" for d in dept_ids])
            dept_filter = f"""
                AND ST_Within(
                    ST_SetSRID("{geo_geom_col}", 4326),
                    (SELECT ST_Union(geometry)
                     FROM dim_departament
                     WHERE id_dept IN ({ids_str}))
                )
            """
        # Si tiene columna de nombre la usa, si no usa el label de la capa
        nombre_sql = f'"{name_col}"' if name_col else f"'{layer_label}'"

        query = f"""
            SELECT json_build_object(
                'type',     'Feature',
                'geometry', ST_AsGeoJSON(
                    ST_SetSRID("{geo_geom_col}", 4326)
                )::json,
                'properties', json_build_object(
                    'layer_id',    '{layer_id}',
                    'layer_label', '{layer_label}',
                    'nombre',      {nombre_sql}
                )
            ) AS feature
            FROM "{data_table}"
            WHERE "{geo_geom_col}" IS NOT NULL
            {dept_filter}
        """
    else:
        dept_filter = ""
        if dept_ids:
            ids_str     = ", ".join([f"'{d}'" for d in dept_ids])
            dept_filter = f"AND g.id_dept IN ({ids_str})"
        query = f"""
            SELECT json_build_object(
                'type',     'Feature',
                'geometry', ST_AsGeoJSON(g."{geo_geom_col}")::json,
                'properties', json_build_object(
                    'layer_id',    '{layer_id}',
                    'layer_label', '{layer_label}',
                    'id_mun',      g."{geo_id_col}",
                    'nombre',      g.name_mun
                )
            ) AS feature
            FROM "{geo_table}" g
            JOIN "{data_table}" d ON g."{geo_id_col}" = d."{join_col}"
            WHERE g."{geo_geom_col}" IS NOT NULL
            {dept_filter}
            ORDER BY g.name_mun
        """

    features = query_geojson(query)
    return {"type": "FeatureCollection", "features": features}

# ─────────────────────────────────────────────────────────────
#  CAPA DE ICONO CON ESCALA DE COLOR
#  Icono fijo por municipio, color de fondo según el valor
# ─────────────────────────────────────────────────────────────

@dataclass
class IconScaleLayer(GeoLayer):
    """
    Muestra un icono por municipio cuyo color de fondo varía
    según el valor de un indicador numérico.
    Útil para producción de petróleo, gas, minerales, etc.
    """
    data_table:   str   = ""
    geo_table:    str   = "dim_divipola"
    geo_id_col:   str   = "id_mun"
    geo_geom_col: str   = "geometry"
    muni_id_col:  str   = "id_mun"
    value_col:    str   = ""
    value_label:  str   = ""
    value_unit:   str   = ""        # ej: "barriles/día", "MMPCD"
    agg_func:     str   = "SUM"
    year_col:     str   = "year"
    extra_cols:   tuple = ()

    # Icono visible — emoji o texto corto
    icon:         str   = "🛢️"
    icon_size:    int   = 28        # tamaño del contenedor en píxeles

    # Escala de color del fondo del icono
    color_low:    str   = "#fef9c3"   # amarillo suave → poca producción
    color_high:   str   = "#78350f"   # café oscuro    → mucha producción
    has_own_geometry: bool = False  
    offset:           tuple = (0.0, 0.0) 

    def get_geojson(self, year: int = None, **kwargs) -> dict:
        return _fetch_icon_scale_geojson(
            layer_id         = self.id,
            layer_label      = self.label,
            data_table       = self.data_table,
            geo_table        = self.geo_table,
            geo_id_col       = self.geo_id_col,
            geo_geom_col     = self.geo_geom_col,
            muni_id_col      = self.muni_id_col,
            value_col        = self.value_col,
            value_label      = self.value_label,
            agg_func         = self.agg_func,
            year_col         = self.year_col,
            extra_cols       = self.extra_cols,
            year             = year,
            dept_ids         = kwargs.get("dept_ids", ()),
            has_own_geometry = self.has_own_geometry,
            
        )

    def get_folium_style(self) -> dict:
        return {}
@st.cache_data(ttl=300, show_spinner=False)
def _fetch_icon_scale_geojson(
    layer_id, layer_label, data_table, geo_table,
    geo_id_col, geo_geom_col, muni_id_col,
    value_col, value_label, agg_func, year_col,
    extra_cols, year, dept_ids=(),
    has_own_geometry=False
) -> dict:

    if has_own_geometry:
        dept_filter = ""
        if dept_ids:
                ids_str     = ", ".join([f"'{d}'" for d in dept_ids])
                dept_filter = f"""
                    AND ST_Within(
                        ST_SetSRID("{geo_geom_col}", 4326),
                        (SELECT ST_Union(geometry)
                        FROM dim_departament
                        WHERE id_dept IN ({ids_str}))
                    )
                """


        year_filter = f"AND year = {year}" if year else ""
        extra_json  = "".join([f", '{c}', \"{c}\"" for c in extra_cols])

        query = f"""


            SELECT json_build_object(
                'type',     'Feature',
                'geometry', ST_AsGeoJSON(
                    ST_Centroid(ST_SetSRID("{geo_geom_col}", 4326))
                )::json,
                'properties', json_build_object(
                    'layer_id',    '{layer_id}',
                    'layer_label', '{layer_label}',
                    'nombre',      '{layer_label}',
                    'valor',       "{value_col}",
                    'indicador',   '{value_label}'
                    {extra_json}
                )
            ) AS feature
            FROM "{data_table}"
            WHERE "{value_col}" IS NOT NULL
            AND "{geo_geom_col}" IS NOT NULL
            {year_filter}
            {dept_filter}
            ORDER BY "{value_col}" DESC
        """

    else:
        year_filter = f"AND d.\"{year_col}\" = {year}" if year else ""
        extra_json  = "".join([f", '{c}', {agg_func}(d.\"{c}\")" for c in extra_cols])
        dept_filter = ""
        if dept_ids:
            ids_str     = ", ".join([f"'{d}'" for d in dept_ids])
            dept_filter = f"AND g.id_dept IN ({ids_str})"

        query = f"""
            SELECT json_build_object(
                'type',     'Feature',
                'geometry', ST_AsGeoJSON(ST_Centroid(g."{geo_geom_col}"))::json,
                'properties', json_build_object(
                    'layer_id',    '{layer_id}',
                    'layer_label', '{layer_label}',
                    'id_mun',      g."{geo_id_col}",
                    'nombre',      g.name_mun,
                    'valor',       {agg_func}(d."{value_col}"),
                    'indicador',   '{value_label}'
                    {extra_json}
                )
            ) AS feature
            FROM "{geo_table}" g
            JOIN "{data_table}" d ON g."{geo_id_col}" = d."{muni_id_col}"
            WHERE d."{value_col}" IS NOT NULL
            {year_filter}
            {dept_filter}
            GROUP BY g."{geo_id_col}", g.name_mun, g."{geo_geom_col}"
            ORDER BY {agg_func}(d."{value_col}") DESC
        """


    features = query_geojson(query)

    return {"type": "FeatureCollection", "features": features}