import os
from pathlib import Path
import pandas as pd
import logging
import fiona
fiona.drvsupport.supported_drivers['KML'] = 'rw'
import geopandas as gpd
from src.etl.utils.transform_utils import load_transform_config,get_latest_bronze_run,load_latest_bronze_run,clean_columns,extract_run_name,save_fact_table
PROJECT_ROOT = Path(__file__).resolve().parents[3]

CONFIG_PATH = (
    PROJECT_ROOT
    / "config"
    / "transform"
    / "mercados_campesinos_transform.yaml"
)

def load_kml_file(file_path: Path):

    if not file_path.exists():
        raise FileNotFoundError(
            f"No existe el archivo: {file_path}"
        )

    ext = file_path.suffix.lower()

    if ext != ".kml":
        raise ValueError(
            f"El archivo debe ser KML. Recibido: {ext}"
        )

    logging.info(
        "Leyendo archivo KML: %s",
        file_path
    )

    df = gpd.read_file(
        file_path,
        driver="KML"
    )

    logging.info(
        "Filas cargadas: %s",
        len(df)
    )

    return df

def run():
    config = load_transform_config(
        "mercados_campesinos_transform",
        CONFIG_PATH
    )

    fact_dir = (
        PROJECT_ROOT
        / config["source"]["silver_fact_dir"]
    )

    fact_dir_golden = (
        PROJECT_ROOT
        / config["source"]["golden_fact_dir"]
    )

    # =====================================================
    # INPUT FILE
    # =====================================================
    file_path = os.environ.get("OBSAN_INPUT_FILE")

    if not file_path:
        raise ValueError( "No se definió OBSAN_INPUT_FILE")

    input_path = Path(file_path)

    logging.info(
        "Archivo recibido: %s",
        input_path
    )

    filename = input_path.stem

    run_name = filename.split("_run_")[-1]

    run_name = f"run_{run_name}"

    logging.info(
        "Run name: %s",
        run_name
    )

    df = load_kml_file(input_path)

    df = df[["Name", "geometry"]]

    df = df.rename(
        columns={
            "Name": "name"
        }
    )

    save_fact_table(
        df,
        run_name,
        fact_dir_golden,
        config,
        "mercados_campesinos"
    )

    logging.info(
        "Transformación finalizada correctamente"
    )

    print(df.head())

if __name__ == "__main__":
    run()