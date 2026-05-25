"""
config/layers_config.py — ÁRBOL DE CAPAS
Observatorio de Seguridad Alimentaria de Antioquia
══════════════════════════════════════════════════════════════
Este es el archivo que editas para agregar, quitar o reorganizar
las capas. No necesitas tocar ningún otro archivo.
══════════════════════════════════════════════════════════════
"""

from core.layer import (ChoroplethLayer, PolygonLayer, PointLayer, BubbleLayer,
                        BarChartLayer, BeneficiaryLayer, LineLayer, VictimLayer,
                        HatchLayer, IconScaleLayer)
from core.layer_group import LayerGroup


# ─────────────────────────────────────────────────────────────
#  CAPAS
# ─────────────────────────────────────────────────────────────

municipios = PolygonLayer(
    id          = "municipios",
    label       = "Municipios",
    description = "División municipal de Colombia \n\n FUENTE:https://www.colombiaenmapas.gov.co/",
    table       = "dim_divipola",
    geom_col    = "geometry",
    properties  = ["name_mun", "name_dept", "id_mun"],
    color       = "#7D7F83",  
    opacity     = 0,       
    weight      = 0.3,  
    z_index     = 1,
    category = "contexto",
    source_name = "Colombia en Mapas",
    source_url  = "https://www.colombiaenmapas.gov.co/",
)
departamentos = PolygonLayer(
    id          = "departamentos",
    label       = "Departamentos",
    description = "División departamental de Colombia",
    table       = "dim_departament",
    geom_col    = "geometry",
    properties  = ["id_dept", "name_dept"],
    color       = "#CC95C9",  
    opacity     = 0.2,       
    weight      = 0.3,  
    z_index     = 1,
    color_categorical = True,
    category = "contexto",
    source_name = "Colombia en Mapas",
    source_url  = "https://www.colombiaenmapas.gov.co/",
)

municipios_pdet = HatchLayer(
    id           = "municipios_pdet",
    label        = "Municipios PDET",
    description  = "Municipios con Programas de Desarrollo con Enfoque Territorial",
    data_table   = "dim_mun_pdet",
    geo_table    = "dim_divipola",
    geo_id_col   = "id_mun",
    join_col     = "id_mun",
    hatch_color  = "#e11d48",    # rojo rosado
    hatch_weight = 1.0,
    hatch_spacing= 6,
    border_color = "#9f1239",
    border_weight= 1.2,
    z_index      = 3,
    category = "contexto",
    source_name = "Agencia de Renovación del Territorio",
    source_url  = "https://centralpdet.renovacionterritorio.gov.co/"
)

resguardos_indigenas = HatchLayer(
    id               = "resguardos_indigenas",
    label            = "Resguardos indígenas",
    description      = "Resguardos indígenas de Colombia",
    data_table       = "dim_indigenous_reserve",
    geo_table        = "dim_divipola",
    geo_geom_col     = "geometry",
    has_own_geometry = True,
    name_col         = "indigenous",   # ← columna de nombre
    hatch_color      = "#16a34a",
    hatch_weight     = 1.0,
    hatch_spacing    = 6,
    border_color     = "#15803d",
    border_weight    = 1.2,
    z_index          = 3,
    category = "contexto",
    source_name = "Datos Abiertos",
    source_url  = "https://www.datos.gov.co",
)

red_vial = LineLayer(
    id          = "red_vial",
    label       = "Red vial",
    description = "Red vial de Colombia",
    table       = "dim_road_network",
    geom_col    = "geometry",
    properties  = ("id_road",),
    color       = "#f5260b",   
    opacity     = 0.7,
    weight      = 1.2,
    dash_array  = "",          # "" = sólida, "5 5" = punteada
    z_index     = 8,
    filterable_by_dept = False, 
    category = "contexto"
)


desnutricion_aguda_5 = BubbleLayer(
    id          = "desnutricion_aguda_5",
    label       = "Desnutrición aguda <5 años",
    description = "Casos de desnutrición aguda en menores de 5 años per capita por 100.000 habitantes",
    data_table  = "v_acute_malnutrition_5_pc",
    data_id_col = "id_mun",
    geo_table   = "dim_divipola",
    geo_id_col  = "id_mun",
    geo_geom_col= "geometry",
    value_col   = "total_cases_per_capita",
    value_label = "Tasa de desnutrición aguda x 100.000 habitantes",
    agg_func    = "SUM",
    year_col    = "year",
    color_low   = "#5aa7f0",   # rosa suave  → pocos casos
    color_high  = "#7a0177",   # morado      → muchos casos
    radius_min  = 4,
    radius_max  = 20,
    offset      = (0.0, -0.05),
    category = "salud",
    source_name = "SIVIGILA",
    source_url  = "https://portalsivigila.ins.gov.co/Paginas/Buscador.aspx",
)

mortalidad_malnutricion = BubbleLayer(
    id          = "mortalidad_malnutricion",
    label       = "Mortalidad por malnutrición",
    description = "Casos de mortalidad asociada a malnutrición por municipio",
    data_table  = "v_mortality_malnutrition_pc",
    data_id_col = "id_mun",
    geo_table   = "dim_divipola",
    geo_id_col  = "id_mun",
    geo_geom_col= "geometry",
    value_col   = "total_cases_per_capita",
    value_label = "Tasa de mortalidad x 100.000 habitantes",
    agg_func    = "SUM",
    year_col    = "year",
    color_low   = "#fef0d9",   # amarillo suave → pocos casos
    color_high  = "#b30000",   # rojo oscuro    → muchos casos
    radius_min  = 4,
    radius_max  = 20,
    offset      = (0.0,  0.05),
    category = "salud",
    source_name = "SIVIGILA",
    source_url  = "https://portalsivigila.ins.gov.co/Paginas/Buscador.aspx",
)

bajo_peso_al_nacer = BubbleLayer(
    id          = "bajo_peso_al_nacer",
    label       = "Bajo peso al nacer",
    description = "Proporción de bebés con bajo peso al nacer por municipio",
    data_table  = "v_low_birth_weight_pc",
    data_id_col = "id_mun",
    geo_table   = "dim_divipola",
    geo_id_col  = "id_mun",
    geo_geom_col= "geometry",
    value_col   = "total_cases_per_capita",
    value_label = "Tasa de bebés con bajo peso al nacer x 100.000 habitantes",
    agg_func    = "SUM",
    year_col    = "year",
    color_low   = "#d4ffde",   
    color_high  = "#09470DCC",   
    radius_min  = 4,
    radius_max  = 20,
    offset      = (0.0,  0.05),
    category = "salud",
    source_name = "SIVIGILA",
    source_url  = "https://portalsivigila.ins.gov.co/Paginas/Buscador.aspx",
)

inseguridad_alimentaria = ChoroplethLayer(
    id           = "food_insecurity",
    label        = "Inseguridad alimentaria (<18 años)",
    description  = "% de menores de 18 años en hogares con inseguridad alimentaria",

    # Tabla de geometría
    geo_table    = "subregion",
    geo_id_col   = "id_subregion",
    geo_name_col = "name_subregion",
    geo_geom_col = "geometry",

    # Tabla de datos (JOIN)
    data_table   = "perfil_antioquia",
    data_id_col  = "id_subregion",
    value_col    = "pct_u18_food_insecurity",
    value_label  = "% inseguridad alimentaria (<18 años)",
    year_col     = "year",

    # Escala de color: blanco rosado → rojo oscuro
    color_low    = "#fff5f0",
    color_high   = "#cb181d",
    opacity      = 0.80,

    filterable_by_dept = False, 
    category = "seguridad_alimentaria",
    source_name = "Universidad de Antioquia",
    source_url  = "Encuesta"
)

pobreza_monetaria_jefe_hogar = BarChartLayer(
    id          = "pobreza_monetaria_jefe_hogar",
    label       = "Pobreza monetaria por genero del jefe de hogar",
    description = "Índice de pobreza monetaria según genero del jefe de hogar por departamento",
    data_table  = "mp_sex_head_hh",
    geo_table   = "dim_departament",
    geo_id_col  = "id_dept",
    value_col   = "mp_idx_val",
    value_label = "Índice pobreza monetaria jefe de hogar (%)",
    year_col    = "year",
    group_col   = "id_gender",
    group_table = "dim_gender",
    bar_colors  = ("#3b82f6", "#ec4899"),   # azul=hombre, rosa=mujer
    bar_width   = 12,
    bar_max_height = 40,
    z_index     = 5,
    category = "socioeconomico",
)

pobreza_monetaria_por_genero = BarChartLayer(
    id          = "pobreza_monetaria_por_genero",
    label       = "Pobreza monetaria por genero",
    description = "Índice de pobreza monetaria según genero por departamento",
    data_table  = "mp_gender",
    geo_table   = "dim_departament",
    geo_id_col  = "id_dept",
    value_col   = "mp_idx_val",
    value_label = "Índice pobreza monetaria por genero (%)",
    year_col    = "year",
    group_col   = "id_gender",
    group_table = "dim_gender",
    bar_colors  = ("#3b82f6", "#ec4899"),   # azul=hombre, rosa=mujer
    bar_width   = 12,
    bar_max_height = 40,
    z_index     = 5,
)

iraca = BeneficiaryLayer(
    id           = "iraca",
    label        = "Beneficiarios IRACA",
    description  = "Municipios con beneficiarios del programa IRACA",
    data_table   = "iraca_beneficiaries",
    geo_table    = "dim_divipola",
    geo_id_col   = "id_mun",
    muni_id_col  = "id_mun",
    count_col    = "beneficiaries_count",
    year_col     = "year",
    program_name = "IRACA",
    extra_tooltip_cols = ("status", "type"),
    z_index      = 20,   # encima de todo
    category = "conflicto",
    source_name = "Datos Abiertos",
    source_url  = "https://www.datos.gov.co",
)

victimas = VictimLayer(
    id          = "victimas",
    label       = "Víctimas del conflicto",
    description = "Víctimas registradas por municipio y tipo de evento",
    data_table  = "victims",        # ← reemplaza con el nombre exacto de tu tabla
    event_table = "dim_victim_event",
    geo_table   = "dim_divipola",
    geo_id_col  = "id_mun",
    muni_id_col = "id_mun",
    count_col   = "victim_count",
    year_col    = "year",
    z_index     = 15,
    category = "conflicto",
    source_name = "Datos Abiertos",
    source_url  = "https://www.datos.gov.co",
)


produccion_petroleo = IconScaleLayer(
    id          = "produccion_petroleo",
    label       = "Producción de petróleo",
    description = "Producción de petróleo por campo",
    data_table  = "oil_production",  
    geo_table   = "dim_divipola",
    geo_id_col  = "id_mun",
    geo_geom_col= "geometry", 
    muni_id_col = "id_mun",
    value_col   = "produc_bls", 
    value_label = "Producción de petróleo",
    value_unit  = "bls",
    agg_func    = "SUM",
    year_col    = "year",
    icon        = "🛢️",
    icon_size   = 28,
    color_low   = "#f3e22b",   # amarillo suave → poca producción
    color_high  = "#78350f",   # café oscuro    → mucha producción
    has_own_geometry = True,
    z_index     = 10,
    offset = (0.0, 0.05),
    category = "ambiente",
    source_name = "Datos Abiertos",
    source_url  = "https://www.datos.gov.co",
)

produccion_gas = IconScaleLayer(
    id          = "produccion_gas",
    label       = "Producción de gas",
    description = "Producción de gas por campo",
    data_table  = "gas_production",  
    geo_table   = "dim_divipola",
    geo_id_col  = "id_mun",
    geo_geom_col= "geometry", 
    muni_id_col = "id_mun",
    value_col   = "produc_kpc", 
    value_label = "Producción de gas",
    value_unit  = "kpc",
    agg_func    = "SUM",
    year_col    = "year",
    icon        = "🔥",
    icon_size   = 28,
    color_low   = "#fef9c3",   # amarillo suave → poca producción
    color_high  = "#78350f",   # café oscuro    → mucha producción
    has_own_geometry = True,
    z_index     = 10,
    offset = (0.0, -0.05),
    category = "ambiente",
    source_name = "Datos Abiertos",
    source_url  = "https://www.datos.gov.co",
)

regalias = IconScaleLayer(
    id          = "regalias_oil_gas",
    label       = "Regalias por producción de petróleo y gas",
    description = "Regalias por producción de petróleo y gas",
    data_table  = "royalties",  
    geo_table   = "dim_divipola",
    geo_id_col  = "id_mun",
    geo_geom_col= "geometry", 
    muni_id_col = "id_mun",
    value_col   = "royalties_cop", 
    value_label = "Regalías",
    value_unit  = "cop",
    agg_func    = "SUM",
    year_col    = "year",
    icon        = "💰",
    icon_size   = 28,
    color_low   = "#fef9c3",   
    color_high  = "#78350f",   
    has_own_geometry = True,
    z_index     = 10,
    offset = (0.0, 0),
    category = "ambiente",
    source_name = "Datos Abiertos",
    source_url  = "https://www.datos.gov.co",
)
regalias_minerales = IconScaleLayer(
    id          = "regalias_minerals",
    label       = "Regalias por minerales",
    description = "Regalias por minerales",
    data_table  = "mineral_royalties",  
    geo_table   = "dim_divipola",
    geo_id_col  = "id_mun",
    geo_geom_col= "geometry", 
    muni_id_col = "id_mun",
    value_col   = "royalties_cop", 
    value_label = "Regalías",
    value_unit  = "cop",
    agg_func    = "SUM",
    year_col    = "year",
    icon        = "💰💎",
    icon_size   = 28,
    color_low   = "#fef9c3",   # amarillo suave → poca producción
    color_high  = "#78350f",   # café oscuro    → mucha producción
    has_own_geometry = False,
    z_index     = 10,
    offset = (0.0, 0),
    category = "ambiente",
    source_name = "Datos Abiertos",
    source_url  = "https://www.datos.gov.co",
)

clima_ppt = ChoroplethLayer(
    id           = "clima_ppt",
    label        = "Precipitación",
    geo_table    = "dim_divipola",
    geo_id_col   = "id_mun",
    geo_name_col = "name_mun",
    geo_geom_col = "geometry",
    data_table   = "terraclimate",
    data_id_col  = "id_mun",
    value_col    = "value",
    value_label  = "Precipitación (mm)",
    year_col     = "year",
    color_low    = "#f0f9ff",
    color_high   = "#0369a1",
    category     = "ambiente",
    filter_sql   = "variable = 'ppt'",
    source_name = "Terraclimate",
    source_url  = "https://climate.northwestknowledge.net/TERRACLIMATE-DATA/",
    border_visible = False,
)

clima_tmin = ChoroplethLayer(
    id           = "clima_tmin",
    label        = "Temperatura mínima",
    geo_table    = "dim_divipola",
    geo_id_col   = "id_mun",
    geo_name_col = "name_mun",
    geo_geom_col = "geometry",
    data_table   = "terraclimate",
    data_id_col  = "id_mun",
    value_col    = "value",
    value_label  = "Temperatura mínima (°C)",
    year_col     = "year",
    color_low    = "#f5e18b",
    color_high   = "#a10303",
    category     = "ambiente",
    filter_sql   = "variable = 'tmin'",
    source_name = "Terraclimate",
    source_url  = "https://climate.northwestknowledge.net/TERRACLIMATE-DATA/",
    border_visible = False,
)

clima_tmax = ChoroplethLayer(
    id           = "clima_tmax",
    label        = "Temperatura máxima",
    geo_table    = "dim_divipola",
    geo_id_col   = "id_mun",
    geo_name_col = "name_mun",
    geo_geom_col = "geometry",
    data_table   = "terraclimate",
    data_id_col  = "id_mun",
    value_col    = "value",
    value_label  = "Temperatura máxima (°C)",
    year_col     = "year",
    color_low    = "#f5e18b",
    color_high   = "#a10303",
    category     = "ambiente",
    filter_sql   = "variable = 'tmax'",
    source_name = "Terraclimate",
    source_url  = "https://climate.northwestknowledge.net/TERRACLIMATE-DATA/",
    border_visible = False,
)

edu_escolar = BubbleLayer(
    id          = "edu_escolar",
    label       = "Educación escolar",
    description = "Proporción de niños con acceso a educación escolar por municipio",
    data_table  = "v_school_education_pc",
    data_id_col = "id_mun",
    geo_table   = "dim_divipola",
    geo_id_col  = "id_mun",
    geo_geom_col= "geometry",
    value_col   = "total_cases_per_capita",
    value_label = "Tasa de niños con acceso a educación escolarx 100 habitantes",
    agg_func    = "SUM",
    year_col    = "year",
    color_low   = "#ffd4f9",   
    color_high  = "#D1189A",   
    radius_min  = 4,
    radius_max  = 20,
    offset      = (0.0,  0.05),
    category    = "socioeconomico",
    source_name = "Datos Abiertos",
    source_url  = "https://www.datos.gov.co",
)

edu_superior = BubbleLayer(
    id          = "edu_superior",
    label       = "Educación superior",
    description = "Proporción de personas con acceso a educación superior por municipio",
    data_table  = "v_higher_education_pc",
    data_id_col = "id_mun",
    geo_table   = "dim_divipola",
    geo_id_col  = "id_mun",
    geo_geom_col= "geometry",
    value_col   = "total_cases_per_capita",
    value_label = "Tasa de personas con acceso a educación superior x 100 habitantes",
    agg_func    = "SUM",
    year_col    = "year",
    color_low   = "#ecf59f",   
    color_high  = "#1CF340",   
    radius_min  = 4,
    radius_max  = 20,
    offset      = (0.0,  0.05),
    category    = "socioeconomico",
    source_name = "Datos Abiertos",
    source_url  = "https://www.datos.gov.co",
)
# ── Producción agrícola — base compartida ─────────────────────
_agro_base = dict(
    data_table   = "agricultural_production",
    geo_table    = "dim_divipola",
    geo_id_col   = "id_mun",
    geo_geom_col = "geometry",
    value_col    = "yield",
    value_label  = "Rendimiento agrícola",
    agg_func     = "AVG",
    year_col     = "year",

    category     = "agropecuario",
    source_name  = "UPRA",
    source_url   = "https://upra.gov.co/es-co/eva",
    z_index      = 10,
)

agro_cereales = BubbleLayer(
    id         = "agro_cereales",
    label      = "Cereales",
    color_low  = "#fef9c3",
    color_high = "#854d0e",
    row_filter = "type = 'cereales'",
    offset     = (0.00,  0.00),
    **_agro_base,
)

agro_condimentos = BubbleLayer(
    id         = "agro_condimentos",
    label      = "Condimentos y aromáticas",
    color_low  = "#f0fdf4",
    color_high = "#14532d",
    row_filter = "type = 'cultivos para condimentos y bebidas medicinales y aromaticas'",
    offset     = (0.04,  0.04),
    **_agro_base,
)

agro_tropicales = BubbleLayer(
    id         = "agro_tropicales",
    label      = "Cultivos tropicales",
    color_low  = "#fefce8",
    color_high = "#713f12",
    row_filter = "type = 'cultivos tropicales tradicionales'",
    offset     = (-0.04,  0.04),
    **_agro_base,
)

agro_frutales = BubbleLayer(
    id         = "agro_frutales",
    label      = "Frutales",
    color_low  = "#fff1f2",
    color_high = "#9f1239",
    row_filter = "type = 'frutales'",
    offset     = (0.04, -0.04),
    **_agro_base,
)

agro_hortalizas = BubbleLayer(
    id         = "agro_hortalizas",
    label      = "Hortalizas",
    color_low  = "#f0fdf4",
    color_high = "#166534",
    row_filter = "type = 'hortalizas'",
    offset     = (-0.04, -0.04),
    **_agro_base,
)

agro_leguminosas = BubbleLayer(
    id         = "agro_leguminosas",
    label      = "Leguminosas",
    color_low  = "#fdf4ff",
    color_high = "#581c87",
    row_filter = "type = 'leguminosas'",
    offset     = (0.00,  0.08),
    **_agro_base,
)

agro_oleaginosas = BubbleLayer(
    id         = "agro_oleaginosas",
    label      = "Oleaginosas",
    color_low  = "#fffbeb",
    color_high = "#92400e",
    row_filter = "type = 'oleaginosas'",
    offset     = (0.00, -0.08),
    **_agro_base,
)

agro_raices = BubbleLayer(
    id         = "agro_raices",
    label      = "Raíces y tubérculos",
    color_low  = "#fff7ed",
    color_high = "#7c2d12",
    row_filter = "type = 'raices y tuberculos'",
    offset     = (0.08,  0.00),
    **_agro_base,
)

# ── Producción agrícola — base compartida ─────────────────────
_pecu_base = dict(
    data_table   = "census_livestock",
    geo_table    = "dim_divipola",
    geo_id_col   = "id_mun",
    geo_geom_col = "geometry",
    value_col    = "total_animals",
    value_label  = "produccion pecuaria",
    agg_func     = "SUM",
    year_col     = "year",
    category     = "agropecuario",
    source_name  = "ICA",
    source_url   = "https://www.ica.gov.co/areas/pecuaria/servicios/epidemiologia-veterinaria/censos-2016",
    z_index      = 10,
)

pecu_aves = BubbleLayer(
    id         = "pecu_aves",
    label      = "Aves",
    color_low  = "#fef9c3",
    color_high = "#854d0e",
    row_filter = "type = 'aves'",
    offset     = (0.00,  0.00),
    **_pecu_base,
)

pecu_bovino = BubbleLayer(
    id         = "pecu_bovinos",
    label      = "Bovinos",
    color_low  = "#f0fdf4",
    color_high = "#14532d",
    row_filter = "type = 'bovinos'",
    offset     = (0.04,  0.04),
    **_agro_base,
)

pecu_bufalino = BubbleLayer(
    id         = "pecu_bufalino",
    label      = "Bufalino",
    color_low  = "#fefce8",
    color_high = "#713f12",
    row_filter = "type = 'bufalino'",
    offset     = (-0.04,  0.04),
    **_agro_base,
)

pecu_caprino = BubbleLayer(
    id         = "pecu_caprino",
    label      = "Caprino",
    color_low  = "#fff1f2",
    color_high = "#9f1239",
    row_filter = "type = 'caprino'",
    offset     = (0.04, -0.04),
    **_agro_base,
)

pecu_equino = BubbleLayer(
    id         = "pecu_equino",
    label      = "Equino",
    color_low  = "#fff7ed",
    color_high = "#7c2d12",
    row_filter = "type = 'equino'",
    offset     = (0.04, -0.04),
    **_agro_base,
)

pecu_ovino = BubbleLayer(
    id         = "pecu_ovino",
    label      = "Ovino",
    color_low  = "#fff1f2",
    color_high = "#9f1239",
    row_filter = "type = 'ovino'",
    offset     = (0.04, -0.04),
    **_agro_base,
)

pecu_porcino = BubbleLayer(
    id         = "pecu_porcino",
    label      = "Porcino",
    color_low  = "#fff1f2",
    color_high = "#9f1239",
    row_filter = "type = 'porcino'",
    offset     = (0.04, -0.04),
    **_agro_base,
)
# ─────────────────────────────────────────────────────────────
#  ÁRBOL DE CAPAS
# ─────────────────────────────────────────────────────────────

LAYER_TREE = LayerGroup(
    id    = "root",
    label = "Capas",
    items = [
        LayerGroup(
            id       = "admin",
            label    = "División administrativa",
            icon     = "🗺️",
            expanded = True,
            items    = [municipios,departamentos, municipios_pdet, resguardos_indigenas],
        ),
        LayerGroup(
            id       = "infraestructura",
            label    = "Infraestructura",
            icon     = "🛣️",
            expanded = False,
            items    = [red_vial],
        ),

        LayerGroup(
            id       = "seg_alimentaria",
            label    = "Seguridad alimentaria",
            icon     = "🍎",
            expanded = True,
            items    = [inseguridad_alimentaria],
        ),

        LayerGroup(
            id       = "mortalidad",
            label    = "Mortalidad y desnutrición",
            icon     = "💀",
            expanded = False,
            items    = [
                desnutricion_aguda_5,
                mortalidad_malnutricion,
                bajo_peso_al_nacer,
            ],
        ),
        LayerGroup(
            id       = "pobreza",
            label    = "Pobreza",
            icon     = "📉",
            expanded = False,
            items    = [pobreza_monetaria_jefe_hogar,pobreza_monetaria_por_genero],
        ),
        LayerGroup(
            id       = "beneficiarios",
            label    = "Programas sociales",
            icon     = "👥",
            expanded = False,
            items    = [iraca],
        ),
        LayerGroup(
            id       = "conflicto",
            label    = "Conflicto",
            icon     = "🕊️",
            expanded = False,
            items    = [victimas],
        ),
        LayerGroup(
            id       = "hidrocarburos",
            label    = "Hidrocarburos",
            icon     = "⛽",
            expanded = False,
            items    = [produccion_petroleo,produccion_gas, regalias, regalias_minerales],
        ),
        LayerGroup(
            id       = "clima",
            label    = "Clima",
            icon     = "☔",
            expanded = False,
            items    = [clima_ppt, clima_tmin, clima_tmax],
            ),

        LayerGroup(
            id       = "educacion",
            label    = "Educación",
            icon     = "📚",
            expanded = False,
            items    = [edu_escolar,edu_superior],
        ),
        LayerGroup(
            id       = "produccion_agricola",
            label    = "Producción agrícola",
            icon     = "🌱",
            expanded = False,
            items    = [
                agro_cereales,
                agro_condimentos,
                agro_tropicales,
                agro_frutales,
                agro_hortalizas,
                agro_leguminosas,
                agro_oleaginosas,
                agro_raices,
            ],
        ),

LayerGroup(
            id       = "produccion_pecuaria",
            label    = "Producción pecuaria",
            icon     = "🐄",
            expanded = False,
            items    = [
                pecu_aves,
                pecu_bovino,
                pecu_caprino,
                pecu_porcino,
                pecu_equino,
                pecu_ovino,
                pecu_bufalino,
            ],
        ),
    ]
)

# ─────────────────────────────────────────────────────────────
#  DEFINICIÓN DE CATEGORÍAS
# ─────────────────────────────────────────────────────────────

CATEGORIES = {
    "seguridad_alimentaria": {
        "label": "Seguridad Alimentaria",
        "icon":  "🍽️",
        "exclusive": False,   # puede coexistir con cualquier categoría
    },
    "salud": {
        "label": "Salud",
        "icon":  "🏥",
        "exclusive": True,
    },
    "socioeconomico": {
        "label": "Condiciones Socioeconómicas",
        "icon":  "📊",
        "exclusive": True,
    },
    "ambiente": {
        "label": "Ambiente y Territorio",
        "icon":  "🌿",
        "exclusive": True,
    },
    "agropecuario": {
        "label": "Sector Agropecuario",
        "icon":  "🌾",
        "exclusive": True,
    },
    "conflicto": {
        "label": "Conflicto y Vulnerabilidad",
        "icon":  "🕊️",
        "exclusive": True,
    },
    "contexto": {
        "label": "Contexto Geográfico",
        "icon":  "🗺️",
        "exclusive": False,   # siempre activable
    },
}
# ─────────────────────────────────────────────────────────────
#  CONFIGURACIÓN DEL MAPA
# ─────────────────────────────────────────────────────────────

MAP_CONFIG = {
    # Centro aproximado de Antioquia
    "center":          [7.0, -75.5],
    "zoom":            8,
    "default_basemap": "Calles (OSM)",
    "basemaps": {
        "Calles (OSM)":     "OpenStreetMap",
        "Satélite (Esri)":  "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        "Claro (CartoDB)":  "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
        "Oscuro (CartoDB)": "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
    },
}