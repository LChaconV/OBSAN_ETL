from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd
from shapely import Point

from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.transform_utils import (
    load_transform_config,
    clean_columns,
    normalize_types,
    deduplicate_by_id,
    save_fact_table,
    get_latest_bronze_run,
    load_latest_bronze_run,
    extract_run_name,
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "regalias_transform.yaml"
LOG_DIR = PROJECT_ROOT / "logs"


def get_config() -> dict:
    return load_transform_config("regalias_transform", CONFIG_PATH)


def build_table(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    fact_cfg = config["fact_table"]
    grain = fact_cfg["grain"]
    metric_column = fact_cfg["metric_column"]
    rename_columns = fact_cfg["rename_columns"]

    df = clean_columns(df)
    df = normalize_types(df, config)
    df = deduplicate_by_id(df, config)

    table_fact = (
        df.groupby(grain, dropna=False, as_index=False)[metric_column]
        .sum()
        .rename(columns=rename_columns)
    )

    table_fact = table_fact[table_fact["royalties_cop"] != 0]
    table_fact = table_fact.sort_values(
        ["year", "month", "latitud", "longitud"]
    ).reset_index(drop=True)

    table_fact["date_event"] = pd.to_datetime(
        dict(year=table_fact["year"].astype(int), month=table_fact["month"].astype(int), day=1)
    )

    logging.info("Filas tras transformar: %s", len(table_fact))
    return table_fact


def build_golden(table_fact: pd.DataFrame) -> pd.DataFrame:
    df_golden = (
        table_fact
        .groupby(["latitud", "longitud", "year"], dropna=False, as_index=False)["royalties_cop"]
        .sum()
    )
    df_golden["geometry"] = df_golden.apply(
        lambda r: Point(float(r["longitud"]), float(r["latitud"])), axis=1
    )
    df_golden["geometry"] = df_golden["geometry"].astype(str)
    df_golden = df_golden[df_golden["geometry"] != "POINT (0 0)"]
    df_golden = df_golden.drop(columns=["latitud", "longitud"])
    return df_golden[["year", "royalties_cop", "geometry"]]


def run() -> None:
    setup_logging(LOG_DIR, "regalias.log")
    logging.info("Iniciando transformación de regalías")

    config = get_config()

    bronze_dir = PROJECT_ROOT / config["source"]["bronze_dir"]
    fact_dir = PROJECT_ROOT / config["source"]["silver_fact_dir"]
    fact_dir_golden = PROJECT_ROOT / config["source"]["golden_fact_dir"]

    run_dir = get_latest_bronze_run(bronze_dir)
    run_name = extract_run_name(run_dir)

    df = load_latest_bronze_run(run_dir)

    table_fact = build_table(df, config)
    df_golden = build_golden(table_fact)

    save_fact_table(table_fact, run_name, fact_dir, config, "regalias")
    save_fact_table(df_golden, run_name, fact_dir_golden, config, "regalias")
    logging.info("Transformación finalizada correctamente")


if __name__ == "__main__":
    run()
