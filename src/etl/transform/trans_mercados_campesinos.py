from pathlib import Path
import pandas as pd
import logging
import fiona
fiona.drvsupport.supported_drivers['KML'] = 'rw'
import geopandas as gpd
from src.etl.utils.transform_utils import load_transform_config,get_latest_bronze_run,load_latest_bronze_run,clean_columns,extract_run_name,save_fact_table
PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "mercados_campesinos_transform.yaml"
def load_latest_kml_run(run_dir: Path):
    files = list(run_dir.glob("*.kml")) + list(run_dir.glob("*.KML"))

    if not files:
        raise ValueError(f"No se encontraron archivos KML en {run_dir}")

    logging.info("Archivos KML encontrados: %s", len(files))

    dfs = [gpd.read_file(file, driver="KML") for file in files]

    df = gpd.GeoDataFrame(pd.concat(dfs, ignore_index=True))

    logging.info("Filas cargadas desde bronze: %s", len(df))

    return df
def main():

    config = load_transform_config("mercados_campesinos_transform", CONFIG_PATH)

    bronze_dir = PROJECT_ROOT / config["source"]["bronze_dir"]
    fact_dir = PROJECT_ROOT / config["source"]["silver_fact_dir"]
    fact_dir_golden = PROJECT_ROOT / config["source"]["golden_fact_dir"]

    run_dir = get_latest_bronze_run(bronze_dir)
    run_name = extract_run_name(run_dir)
    df = load_latest_kml_run(run_dir)
    df=df[["Name","geometry"]]
    df = df.rename(columns={"Name": "name"})
    save_fact_table(df, run_name, fact_dir_golden, config, "mercados_campesinos")

    

    print(df.head())

if __name__ == "__main__":
    main()
