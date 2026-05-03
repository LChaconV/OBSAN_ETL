import os
from pathlib import Path
import sys
import geopandas as gpd

# Se asume que estas funciones están en tu estructura de carpetas src/etl/utils/
from src.etl.utils.config_utils import load_source_config, load_yaml

# ============================================================
# RUTAS DEL PROYECTO
# ============================================================
# Localiza la raíz del proyecto subiendo 3 niveles desde src/etl/transform/
PROJECT_ROOT = Path(__file__).resolve().parents[3]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Rutas a los archivos de configuración
SOURCES_CONFIG_PATH = PROJECT_ROOT / "config" / "sources.yaml"
GENERAL_CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"

def validate_id_subregion(gdf):
    """
    Realiza validaciones de integridad sobre la columna id_subregion.
    """
    # Asegurar que sea string para evitar problemas de formato en el Join
    gdf["id_subregion"] = gdf["id_subregion"].astype("string")

    mask_null = gdf["id_subregion"].isna()
    if mask_null.any():
        print("Advertencia: Se encontraron valores nulos en 'id_subregion'.")
        print(gdf.loc[mask_null, ["id_subregion"]])

    # El estándar de códigos de subregiones/provincias suele ser de 4 dígitos
    mask_length = gdf["id_subregion"].str.len() != 4
    if mask_length.any():
        print("Advertencia: Algunos 'id_subregion' no tienen la longitud estándar de 4 caracteres.")

    mask_digits = ~gdf["id_subregion"].str.match(r"^\d{4}$", na=False)
    if mask_digits.any():
        print("Advertencia: Algunos 'id_subregion' contienen caracteres no numéricos.")

    print("Validación de id_subregion finalizada.")
    return gdf

def run() -> None:
    # 1. CARGA DE CONFIGURACIONES
    # Carga la configuración de la fuente específica
    source_cfg = load_source_config("subregiones", SOURCES_CONFIG_PATH)
    # Carga la configuración general de rutas de salida
    general_cfg = load_yaml(GENERAL_CONFIG_PATH)

    # 2. DEFINICIÓN DE RUTAS (Entrada de Bronze y Salida de Silver)
    ruta_entrada = os.environ.get("OBSAN_INPUT_FILE")
    if not ruta_entrada:
        raise ValueError("No se definió OBSAN_INPUT_FILE")
    #ruta_entrada = PROJECT_ROOT / source_cfg["storage"]["bronze_dir"] / source_cfg["storage"]["file"]

    # Ruta de salida: data/silver/subregiones/subregiones_provincias_colombia.parquet
    output_dir = PROJECT_ROOT / general_cfg["silver"]["subregiones_dir"]
    output_name = general_cfg["silver"]["subregiones_file"]
    ruta_salida = output_dir / output_name

    # 3. LECTURA DE DATOS


    gdf = gpd.read_file(ruta_entrada)

    # 4. TRANSFORMACIÓN (Estandarización de Capa Silver)
    print("Transformando datos...")
    
    # Renombrado de columnas según el estándar definido para el Observatorio
    gdf = gdf.rename(columns={
        "COD_SUBREGION": "id_subregion",
        "COD_DEPTO": "id_dept",
        "NOM_SUBREGION": "name_subregion"
    })

    # Selección de columnas críticas (Enfoque en Determinantes Sociales)
    # Eliminamos metadatos de SIG como OBJECTID o medidas de área originales
    cols_to_keep = ["id_subregion", "id_dept", "name_subregion", "geometry"]
    gdf = gdf[cols_to_keep]

    # 5. VALIDACIÓN
    gdf = validate_id_subregion(gdf)

    # 6. GUARDADO EN FORMATO PARQUET
    # Parquet es ideal para Streamlit por su alta velocidad de lectura
    print(f"Guardando archivo Silver en: {ruta_salida}")
    
    # Asegurar que el directorio de destino exista
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Guardar GeoDataFrame como GeoParquet
    try:
        gdf.to_parquet(ruta_salida, index=False)
        print("¡Transformación exitosa!")
    except Exception as e:
        print(f"Error al guardar el archivo Parquet: {e}")

    # Resumen final
    print("\nResumen del dataset procesado:")
    print(f"Total registros: {len(gdf)}")
    print(gdf.head())
    print(gdf.info())

if __name__ == "__main__":
    run()