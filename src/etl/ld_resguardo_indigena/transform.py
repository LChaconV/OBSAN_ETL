import pandas as pd
from pathlib import Path
import geopandas as gpd
def run():
    # 1. Leer el archivo Excel
    df = gpd.read_file(r"data\bronze\resguardo_indigena\resguardo_indigena.geojson")
    # 2. Seleccionar y renombrar la columna
    df = df[["PUEBLO","MUNICIPIO","geometry"]].rename(columns={
        "PUEBLO": "indigenous",
        "MUNICIPIO": "id_mun",
        "geometry": "geometry"
    })

    # (opcional) asegurar formato tipo string con ceros a la izquierda
    df["id_mun"] = df["id_mun"].astype(str).str.zfill(5)


    ruta = Path("data/golden/resguardo_indigena")

    # Crea la carpeta si no existe (incluye subcarpetas)
    ruta.mkdir(parents=True, exist_ok=True)
    df.to_parquet(ruta / "resguardo_indigena_run_20260421_202413.parquet", index=False)

if __name__ == "__main__":
    run()