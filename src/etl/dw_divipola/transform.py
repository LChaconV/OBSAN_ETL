import pandas as pd
import geopandas as gpd
import topojson as tp
from pathlib import Path
import yaml

with open("config/config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

with open("config/sources.yaml", "r", encoding="utf-8") as f:
    sources_config = yaml.safe_load(f)

def standardize_geography_columns(gdf: gpd.GeoDataFrame,column_map) -> gpd.GeoDataFrame:
    """
    Renombra columnas del dataset original.
    """
    rename_map = {v: k for k, v in column_map.items()}
    gdf = gdf.rename(columns=rename_map)
    return gdf

def validate_id_mun(gdf):

    gdf["id_mun"] = gdf["id_mun"].astype("string")

   
    mask_null = gdf["id_mun"].isna()

    if mask_null.any():
        print("Advertencia: Se encontraron valores nulos en 'id_mun'.")
        print(gdf.loc[mask_null, ["id_mun"]])

    mask_length = gdf["id_mun"].str.len() != 5

    if mask_length.any():
        print("Advertencia: Algunos 'id_mun' no tienen 5 caracteres.")
        print(gdf.loc[mask_length, ["id_mun"]])

    mask_digits = ~gdf["id_mun"].str.match(r"^\d{5}$", na=False)

    if mask_digits.any():
        print("Advertencia: Algunos 'id_mun' no son códigos DIVIPOLA válidos.")
        print(gdf.loc[mask_digits, ["id_mun"]])

    print("Validación de id_mun finalizada.")
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

def fill_dept_from_mun(gdf, mun_col="id_mun", dept_col="id_dept"):

    gdf[mun_col] = gdf[mun_col].astype("string")
    gdf[dept_col] = gdf[dept_col].astype("string")

    mask_null = gdf[dept_col].isna()

    if mask_null.any():
        n = mask_null.sum()
        print(f"Se completarán {n} valores de '{dept_col}' usando '{mun_col}'.")

        completed = gdf.loc[mask_null, [mun_col, dept_col]].copy()
        completed["id_dept"] = completed[mun_col].str[:2]

        print("Registros afectados:")
        print(completed)

    # llenar usando los dos primeros caracteres
    gdf.loc[mask_null, dept_col] = gdf.loc[mask_null, mun_col].str[:2]

    return gdf
def simplify_mun_geometry(gdf):
    try:

        topo = tp.Topology(gdf, prequantize=False)
    
        gdf_topo = topo.toposimplify(6000).to_gdf()
        
        return gdf_topo
    
    except Exception as e:
        print(f"Error procesando topología: {e}")
        return None
def run() -> None:
    filepath = sources_config['divipola']['storage']['bronze_dir'] + '/' + sources_config['divipola']['storage']['file']
    gdf = gpd.read_file(filepath)  
    column_map = config['data_silver']["columns"]["divipola"]     
    gdf = gdf[list(column_map.values())]

    gdf = standardize_geography_columns(gdf,column_map)
    gdf = fill_dept_from_mun(gdf)
    gdf=simplify_mun_geometry(gdf)

    base = Path(config["silver"]["divipola_dir"])
    file = config["silver"]["divipola_file"]

    path = base / file
    gdf.to_parquet(path)
    print(gdf.head())

if __name__ == "__main__":
    run()