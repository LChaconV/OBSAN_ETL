import os
import pandas as pd
import geopandas as gpd
import topojson as tp
from pathlib import Path
import yaml

with open("config/config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)
with open("config/sources.yaml", "r", encoding="utf-8") as f:
    sources_config = yaml.safe_load(f)


def standardize_geography_columns(gdf: gpd.GeoDataFrame, column_map: dict) -> gpd.GeoDataFrame:
    """
    Renombra columnas del dataset original.
    """
    rename_map = {v: k for k, v in column_map.items()}
    gdf = gdf.rename(columns=rename_map)
    return gdf

def validate_id_dept(gdf):

    gdf["id_dept"] = gdf["id_dept"].astype("string")

    mask_null = gdf["id_dept"].isna()

    if mask_null.any():
        print("Advertencia: Se encontraron valores nulos en 'id_dept'.")
        print(gdf.loc[mask_null, ["id_dept"]])

    mask_length = gdf["id_dept"].str.len() != 2

    if mask_length.any():
        print("Advertencia: Algunos 'id_dept' no tienen 2 caracteres.")
        print(gdf.loc[mask_length, ["id_dept"]])

    mask_digits = ~gdf["id_dept"].str.match(r"^\d{2}$", na=False)

    if mask_digits.any():
        print("Advertencia: Algunos 'id_dept' no son códigos DIVIPOLA válidos.")
        print(gdf.loc[mask_digits, ["id_dept"]])

    print("Validación de id_dept finalizada.")


def simplify_geometry(gdf):
    try:

        topo = tp.Topology(gdf, prequantize=False)
    
        gdf_topo = topo.toposimplify(6000).to_gdf()
        
        return gdf_topo
    
    except Exception as e:
        print(f"Error procesando topología: {e}")
        return None
def run():
    file_path = os.environ.get("OBSAN_INPUT_FILE")
    if not file_path:
        raise ValueError("No se definió OBSAN_INPUT_FILE")

    gdf = gpd.read_file(file_path)
    column_map = config['data_silver']["columns"]["departamentos"]
    gdf = gdf[list(column_map.values())]

    gdf = standardize_geography_columns(gdf,column_map)
    gdf=simplify_geometry(gdf)

    base = Path(config["silver"]["departamentos_dir"])
    file = config["silver"]["departamentos_file"]

    path = base / file
    gdf.to_parquet(path)
    print(gdf)

if __name__ == "__main__":
    run()