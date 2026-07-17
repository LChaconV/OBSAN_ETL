
UPLOAD_VARIABLES: dict = {

"divipola": {
        "hidden": True,
        "label": "División municipal de Colombia",
        "description": "División político-administrativa a nivel municipal de Colombia (DIVIPOLA).",
        "allowed_types": ["geojson", "parquet"],
        "storage_folder": "divipola/geojson",
        "format_hint": (
            "GeoJSON: geometrías Polygon o MultiPolygon.\n"
            "Columnas: MpCodigo , MpNombre , DeCodigo , geometry.\n\n"
            "Parquet: columnas geometry, id_mun, name_mun, id_dept, __index_level_0__."
        ),
        "required_columns": {
            "geojson": {
                "id_mun":   "MpCodigo",
                "name_mun": "MpNombre",
                "id_dept":  "departamentos — Departamentos_Diciembre_2025_shp/Depto.shp_DeCodigo",
                "geometry": "geometry",
            },
            "parquet": ["geometry", "id_mun", "name_mun", "id_dept", "__index_level_0__"],
        },
        "pipeline": "divipola",
        "format_image": "apps/streamlit/assets/formats/divipola.png",
        "download_url": "https://www.colombiaenmapas.gov.co/?e=-93.57552216249368,-8.477621778898875,-48.927084662505536,14.748502806916624,4686&b=igac&l=20&u=0&t=29&servicio=20",
    },

    "departamento": {
        "hidden": True,
        "label": "División departamental de Colombia",
        "description": "División político-administrativa a nivel departamental de Colombia.",
        "allowed_types": ["geojson", "parquet"],
        "storage_folder": "departamentos",
        "format_hint": (
                    "GeoJSON: Archivo GeoJSON con geometrías de tipo Polygon o MultiPolygon. "
                    "Columnas requeridas: DeCodigo, DeNombre, geometry.\n\n"
                    "Parquet: Archivo Parquet con las columnas: geometry, id_dept, name_dept, "
                    "__index_level_0__."
                ),
        "required_columns": {
            "geojson": {
                "id_dept":   "DeCodigo",
                "name_dept": "DeNombre",
                "geometry":  "geometry",
            },
            "parquet": ["geometry", "id_dept", "name_dept", "__index_level_0__"],
        },
        "pipeline": "departamento",
        "format_image": "apps/streamlit/assets/formats/departamento.png",
        "download_url": "https://www.colombiaenmapas.gov.co/?e=-93.57552216249368,-8.477621778898875,-48.927084662505536,14.748502806916624,4686&b=igac&l=20&u=0&t=29&servicio=20",
    },

    "municipios_pdet": {
        "hidden": True,
        "label":       "Municipios PDET",
        "description": "Listado de municipios con Programas de Desarrollo con Enfoque Territorial",
        "allowed_types": ["xlsx", "csv"],
        "storage_folder": "pdet",
        "format_hint": (
            "El archivo debe contener la columna: **Código DANE Municipio** "
            "con los códigos DIVIPOLA de los municipios PDET."
        ),
        "required_columns": ["Código DANE Municipio"],
        "pipeline": "mun_pdet",
        "format_image": "apps/streamlit/assets/formats/mun_pdet.png",
        "download_url": "https://centralpdet.renovacionterritorio.gov.co/wp-content/uploads/2022/01/MunicipiosPDET.xlsx",
    },

    "subregiones": {
        "hidden": True,
        "label":       "Subregiones de Antioquia",
        "description": "Listado de subregiones de Antioquia",
        "allowed_types": ["geojson", "parquet"],
        "storage_folder": "subregion_antioquia",
        "format_hint": (
            "El archivo debe contener geometrías de tipo Polygon o MultiPolygon"
           
        ),
        "required_columns": [],
        "pipeline": "subregion_antioquia",
    },

    "Perfil_antioquia": {
        "label":       "Perfil alimentario Antioquia",
        "description": "Perfil alimentario Antioquia",
        "allowed_types": ["csv","xlsx","xls"],
        "storage_folder": "perfil_antioquia",
        "format_hint": (
            "El archivo debe contener los resultados del perfil alimentario por subregiones"
           
        ),
        "required_columns": [],
        "pipeline": "perfil_antioquia",
        "format_image": "apps/streamlit/assets/formats/perfil_antioquia.png",
    },

    "poblacion": {
        "label":       "Proyección poblacional de Colombia",
        "description": "Población de Colombia ",
        "allowed_types": ["csv","xlsx","xls"],
        "storage_folder": "poblacin",
        "format_hint": (
            "El archivo debe contener Código Entidad, Año, Dato Numérico, Unidad de Medida"
           
        ),
        "required_columns": ["Código Entidad", "Año", "Dato Numérico", "Unidad de Medida"],
        "pipeline": "poblacion",
        "format_image": "apps/streamlit/assets/formats/poblacion.png",
        "download_url": "https://terridata.dnp.gov.co/index-app.html#/descargas"
    },

    "resguardo_indigena": {
        "label":       "Resguardo indígena",
        "description": "Geometrías de resguardos indígenas en Colombia",
        "allowed_types": ["geojson"],
        "storage_folder": "resguardo_indigena",
        "format_hint": (
            "Archivo GeoJSON con geometrías de tipo Polygon o MultiPolygon. "
        ),
        "required_columns": [],
        "pipeline": "resguardo_indigena",
        "format_image": "apps/streamlit/assets/formats/resguardo_indigena.png",
        "download_url": "https://www.datos.gov.co/dataset/Resguardo-Indigena-Formalizado/4hy9-7y2w/about_data"
    },

    "mercado_laboral": {
        "label":       "Mercado laboral",
        "description": "Indicadores del mercado laboral por municipio",
        "allowed_types": ["xlsx", "csv"],
        "storage_folder": "mercado_laboral",
        "format_hint": (
            "El archivo debe contener las columnas **año**, **código entidad**, **indicador**, **dato numérico**, **departamento** "
            "**entidad**, **subcategoría**"
        ),
        "required_columns": ["año"],
        "pipeline": "mercado_laboral",
        "format_image": "apps/streamlit/assets/formats/mercado_laboral.png",
        "download_url": "https://terridata.dnp.gov.co/index-app.html#/descargas"
    },

    "mortalidad_desnutricion": {
        "label":       "Mortalidad por desnutrición",
        "description": "Casos de mortalidad por desnutrición",
        "allowed_types": ["xlsx", "csv","xls"],
        "storage_folder": "mortalidad_desnutricion",
        "format_hint": (
            "El archivo debe contener las columnas: **COD_EVE**, **FEC_DEF**, **ANO**, **EDAD**, **COD_PAIS_O**, **COD_DPTO_O**, **COD_MUN_O**"
           
        ),
        "required_columns": ["COD_EVE", "FEC_DEF", "ANO", "EDAD", "COD_PAIS_O", "COD_DPTO_O", "COD_MUN_O"],
        "pipeline": "mortalidad_desnutricion",
        "format_image": "apps/streamlit/assets/formats/mortalidad_desnutricion.png",
        "download_url": "https://portalsivigila.ins.gov.co/Paginas/Buscador.aspx",
    },

    "desnutricion_aguda_5": {
        "label":       "Desnutrición aguda en menores de 5 años",
        "description": "Casos de desnutrición aguda en menores de 5 años",
        "allowed_types": ["xlsx", "csv","xls"],
        "storage_folder": "desnutricion_aguda_5",
        "format_hint": (
            "El archivo debe contener las columnas: **CONSECUTIVE**, **confirmados**, **CON_FIN**, **FEC_NOT**, **ANO**, **EDAD**, **COD_PAIS_O**, **COD_DPTO_O**, **COD_MUN_O**, **COD_EVE**"
           
        ),
        "required_columns": [ "confirmados", "CON_FIN", "FEC_NOT", "ANO", "EDAD", "COD_PAIS_O", "COD_DPTO_O", "COD_MUN_O", "COD_EVE"],
        "pipeline": "desnutricion_aguda_5",
        "format_image": "apps/streamlit/assets/formats/desnutricion_5.png",
        "download_url": "https://portalsivigila.ins.gov.co/Paginas/Buscador.aspx",
    },

    "bajo_peso_nacer": {
        "label":       "Bajo Peso Al Nacer",
        "description": "Casos de niños con bajo peso a nacer",
        "allowed_types": ["xlsx", "csv","xls"],
        "storage_folder": "bajo_peso_nacer",
        "format_hint": (
            "El archivo debe contener las columnas: **CONSECUTIVE**, **confirmados**, **FEC_NOT**, **COD_PAIS_O**, **COD_DPTO_O**, **COD_MUN_O**"
           
        ),
        "required_columns": [ "confirmados","COD_PAIS_O", "COD_DPTO_O", "COD_MUN_O"],
        "pipeline": "bajo_peso_nacer",
        "format_image": "apps/streamlit/assets/formats/bajo_peso_nacer.png",
        "download_url": "https://portalsivigila.ins.gov.co/Paginas/Buscador.aspx",
    },

    "mercados_campesinos": {
        "label":       "Mercados campesinos",
        "description": "Geometrías de mercados campesinos en Colombia",
        "allowed_types": ["kml"],
        "storage_folder": "mercados_campesinos",
        "format_hint": (
            "Archivo KML con geometrías de tipo Point, representando la ubicación de los mercados campesinos."
        ),
        "required_columns": [],
        "pipeline": "mercados_campesinos",
        "format_image": "apps/streamlit/assets/formats/mercados_campesinos.png",
        "download_url": "https://www.google.com/maps/d/u/0/viewer?mid=1vFPkcoidvWCixLQlu-3JzjVGDbQ&femb=1&ll=4.804698096274127%2C-75.58867292494415&z=11",
    },

    "censo_pecuario": {
            "label":       "Censo pecuario",
            "description": "Censo pecuario nacional por municipio y especie",
            "allowed_types": ["xlsx", "csv","xls"],
            "storage_folder": "censo_pecuario",
            "format_hint": (
                "El archivo debe contener los datos por municipio"
            ),
            "required_columns": [],
            "pipeline": "censo_pecuario",
            "format_image": "apps/streamlit/assets/formats/censo_pecuario.png",
            "download_url": "https://www.ica.gov.co/areas/pecuaria/servicios/epidemiologia-veterinaria/censos-2016/censo-2018",
            "extra_fields": [
                {
                    "id":      "year",
                    "label":   "Año del censo",
                    "type":    "number_input",
                    "min_value": 2000,
                    "max_value": 2030,    
                    "required": True,
                },
                {
                    "id":      "animal_type",
                    "label":   "Tipo de especie",
                    "type":    "selectbox",
                    "options": ["aves", "bovino", "bufalino",
                                "caprino", "ovino", "equino", "porcino"],
                    "required": True,
                },
            ],
        },
    
    "ipm_departamentos": {
        "label":       "Incidencia de Pobreza Multidimensional",
        "description": "IPM por departamento — hoja 'IPM_Departamentos' del archivo DANE",
        "allowed_types": ["xlsx", "xls"],
        "storage_folder": "ipm_departamentos",
        "format_hint": (
            "Archivo Excel del DANE con hoja **IPM_Departamentos**. "
            "Debe contener columnas de Código DANE, Departamentos y los años disponibles "
            "con sus subcolumnas (Total, Cabeceras, Centros poblados y rural disperso)."
        ),
        "required_columns": [],
        "pipeline": "ipm_departamentos",
        "format_image":"apps/streamlit/assets/formats/ipm departamental.png",
        "download_url": "https://www.dane.gov.co/index.php/estadisticas-por-tema/pobreza-y-condiciones-de-vida/pobreza-multidimensional",
    },

    "pobreza_monetaria_municipal": {
        "label":       "Pobreza monetaria municipal",
        "description": "Estimación de pobreza monetaria por municipio (DANE)",
        "allowed_types": ["xlsx", "xls"],
        "storage_folder": "pobreza_monetaria_municipal",
        "format_hint": (
            "Archivo Excel del DANE. Cada hoja debe llamarse **Monetaria YYYY** "
            "(p.ej. 'Monetaria 2022'). El año se extrae automáticamente del nombre de la hoja; "
            "un archivo puede contener varias hojas con distintos años.\n\n"
            "Columnas requeridas: **Código Municipio**, **Estimación Pobreza Monetaria**."
        ),
        "required_columns": [],
        "pipeline": "pobreza_monetaria_municipal",
        "download_url": "https://sitios.dane.gov.co/EstimacionesModeloSAE/",
    },

    "nbi_municipal": {
        "label":       "Necesidades Básicas Insatisfechas (NBI)",
        "description": "Proporción de personas con necesidades básicas insatisfechas por municipio",
        "allowed_types": ["xlsx", "xls"],
        "storage_folder": "nbi_municipal",
        "format_hint": (
            "Archivo Excel del DANE con hoja **Municipios**. "
            "Columnas: Código departamento, Nombre departamento, Código municipio, Nombre municipio, "
            "seguidas de los indicadores NBI por dominio (Total, Cabeceras, Rural disperso)."
        ),
        "required_columns": [],
        "pipeline": "nbi_municipal",
        "format_image": "apps/streamlit/assets/formats/NBI.png",
        "download_url": "https://www.dane.gov.co/index.php/estadisticas-por-tema/pobreza-y-condiciones-de-vida/necesidades-basicas-insatisfechas-nbi",
        "extra_fields": [
            {
                "id":        "year",
                "label":     "Año del censo",
                "type":      "number_input",
                "min_value": 2000,
                "max_value": 2030,
                "required":  True,
            },
        ],
    },

    "agricola": {
        "label":       "Producción Agrícola",
        "description": "Datos sobre producción agrícola por municipio",
        "allowed_types": ["xlsx", "csv","xls"],
        "storage_folder": "agricola",
        "format_hint": (
            "El archivo debe contener las columnas: **Código Dane municipio**, **Grupo cultivo**, **Año**, **Área sembrada**, **Área cosechada**, **Producción**"
           
        ),
        "required_columns": [],
        "pipeline": "agricola",
        "format_image": "apps/streamlit/assets/formats/upra_eva.png",
        "download_url": "https://upra.gov.co/es-co/eva",
    },
}
