import pandas as pd
import geopandas as gpd
import topojson as tp
from pathlib import Path
import yaml
from shapely import wkt

# ------------------ CONFIG ------------------
with open("config/config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

column_map = config['data_silver']["columns"]["red_vial"]

# ------------------ FUNCIONES ------------------
def standardize_geography_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Renombra columnas del dataset original.
    """
    rename_map = {v: k for k, v in column_map.items()}
    return gdf.rename(columns=rename_map)


def safe_wkt_loads(value):
    """
    Convierte WKT a geometría, manejando errores.
    """
    try:
        return wkt.loads(value) if pd.notnull(value) else None
    except Exception:
        return None


def simplify_geometry(gdf):
    try:
        topo = tp.Topology(gdf, prequantize=False)
        gdf_topo = topo.toposimplify(3000).to_gdf()
        return gdf_topo
    except Exception as e:
        print(f"Error procesando topología: {e}")
        return gdf


# ------------------ LECTURA ------------------
filepath = r"C:\Users\laura\ESCUELA COLOMBIANA DE INGENIERIA JULIO GARAVITO\Proyecto OBSAN - General\Datos_observatorio-san\observatorio-san\data\bronze\red_vial\Red_Vial_20260420.csv"

df = pd.read_csv(filepath)

# ------------------ GEOMETRÍA ------------------
# Convertir WKT → geometría
df["multiline"] = df["multiline"].apply(safe_wkt_loads)

# Eliminar filas con geometría inválida
df = df[df["multiline"].notnull()]

# Crear GeoDataFrame
gdf = gpd.GeoDataFrame(df, geometry="multiline")
gdf = gdf.rename(columns={"multiline": "geometry"})
gdf = gdf.set_geometry("geometry")

# Asignar sistema de coordenadas (ajústalo si es necesario)
gdf.set_crs(epsg=4326, inplace=True)

# ------------------ COLUMNAS ------------------
gdf = gdf[list(column_map.values())]
gdf = standardize_geography_columns(gdf)

# ------------------ SIMPLIFICACIÓN (opcional) ------------------
# gdf = simplify_geometry(gdf)

# ------------------ EXPORTAR ------------------
base = Path(config["silver"]["red_vial_dir"])
file = config["silver"]["red_vial_file"]

base.mkdir(parents=True, exist_ok=True)  # asegura que exista la carpeta

path = base / file
gdf.to_parquet(path, index=False)

print(gdf.head())
print(f"\nArchivo guardado en: {path}")