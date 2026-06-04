import os
import topojson as tp
import pandas as pd
from pathlib import Path
import geopandas as gpd
from shapely.geometry import mapping, shape
from shapely.ops import transform as shapely_transform

def build_run_name(file_path: str) -> str:
    filename = Path(file_path).stem
    if "_run_" in filename:
        return f"run_{filename.split('_run_')[-1]}"
    return f"run_{filename}"


def simplify_mun_geometry(gdf):
    try:

        topo = tp.Topology(gdf, prequantize=False)
    
        gdf_topo = topo.toposimplify(6000).to_gdf()
        
        return gdf_topo
    
    except Exception as e:
        print(f"Error procesando topología: {e}")
        return None
def drop_z_coords(x, y, z=None):
    return (x, y)

def run():
    # 1. Leer el archivo Excel
    file_path = os.environ.get("OBSAN_INPUT_FILE")
    if not file_path:
        raise ValueError("No se definió OBSAN_INPUT_FILE")
    df = gpd.read_file(file_path)
    # 2. Seleccionar y renombrar la columna
    df = df[["PUEBLO","MUNICIPIO","geometry"]].rename(columns={
        "PUEBLO": "indigenous",
        "MUNICIPIO": "id_mun",
        "geometry": "geometry"
    })


    df["id_mun"] = df["id_mun"].astype(str).str.zfill(5)
    df = df.to_crs('EPSG:4326')
    df.geometry = df.geometry.apply(lambda geom: shapely_transform(drop_z_coords, geom) if geom is not None else geom)
    
    ruta = Path("data/golden/resguardo_indigena")

    # Crea la carpeta si no existe (incluye subcarpetas)
    ruta.mkdir(parents=True, exist_ok=True)
    df.to_parquet(ruta / f"resguardo_indigena_{build_run_name(file_path)}.parquet", index=False)

if __name__ == "__main__":
    run()
