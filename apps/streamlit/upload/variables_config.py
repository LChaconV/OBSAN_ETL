"""
upload/variables_config.py — Configuración de variables de carga
════════════════════════════════════════════════════════════════
Este es el único archivo que editas para agregar o modificar
variables de carga. No necesitas tocar ningún otro módulo.

Cada variable define:
  - label:          Nombre visible en el selectbox
  - description:    Descripción del contenido esperado
  - allowed_types:  Extensiones permitidas
  - storage_folder: Subcarpeta donde se guarda el archivo
  - format_hint:    Mensaje informativo sobre el formato esperado
  - required_columns: Columnas obligatorias (para Excel/CSV)
  - pipeline:       ID del pipeline ETL a ejecutar
════════════════════════════════════════════════════════════════
"""


UPLOAD_VARIABLES: dict = {

    "divipola": {
        "label":       "División municipal de Colombia",
        "description": "División municipal de Colombia",
        "allowed_types": ["geojson"],
        "storage_folder": "divipola/geojson",
        "format_hint": (
            "Archivo GeoJSON con geometrías de tipo Polygon o MultiPolygon."
        ),
        "required_columns": [],
        "pipeline": "divipola",
    },

    "departamento": {
        "label":       "División departamental de Colombia",
        "description": "División departamental de Colombia",
        "allowed_types": ["geojson"],
        "storage_folder": "departamentos",
        "format_hint": (
            "Archivo GeoJSON con geometrías de tipo Polygon o MultiPolygon."
        ),
        "required_columns": [],
        "pipeline": "departamento",
    },

    "municipios_pdet": {
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
    },

    "subregiones": {
        "label":       "Subregiones de Antioquia",
        "description": "Listado de subregiones de Antioquia",
        "allowed_types": ["geojson"],
        "storage_folder": "subregion_antioquia",
        "format_hint": (
            "El archivo debe contener geometrías de tipo Polygon o MultiPolygon"
           
        ),
        "required_columns": [],
        "pipeline": "subregion_antioquia",
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
    },

    "bajo_peso_nacer": {
        "label":       "Bajo Peso Al Nacer",
        "description": "Casos de niños con bajo peso a nacer",
        "allowed_types": ["xlsx", "csv","xls"],
        "storage_folder": "bajo_peso_nacer",
        "format_hint": (
            "El archivo debe contener las columnas: **CONSECUTIVE**, **confirmados**, **FECHA_NTO**, **COD_PAIS_O**, **COD_DPTO_O**, **COD_MUN_O**"
           
        ),
        "required_columns": [ "confirmados", "FECHA_NTO","COD_PAIS", "COD_DPTO_O", "COD_MUN_O"],
        "pipeline": "bajo_peso_nacer",
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
    # ── Agrega más variables aquí siguiendo el mismo patrón ──
}
