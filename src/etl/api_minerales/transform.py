from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.transform_utils import (
    load_transform_config,
    clean_columns,
    clean_text_data,
    normalize_types,
    deduplicate_by_id,
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "minerales_transform.yaml"
LOG_DIR = PROJECT_ROOT / "logs"


def build_table(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Recibe el DataFrame crudo de un año, aplica limpieza, deduplicación
    por :id (fuente Socrata) y agrega al grano año + municipio + mineral.
    """
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

    table_fact = clean_text_data(table_fact)
    table_fact = table_fact[table_fact["royalties_cop"] != 0]
    table_fact = table_fact.sort_values(
        ["year", "month", "id_mun", "mineral_resource"]
    ).reset_index(drop=True)

    logging.info("Filas aggregadas tras transformar: %s", len(table_fact))
    return table_fact


def build_golden(table_fact: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega la fact table al grano final de carga (año + municipio + mineral + unidad).
    """
    return (
        table_fact
        .groupby(["id_mun", "year", "unit_measure", "mineral_resource"], dropna=False, as_index=False)["royalties_cop"]
        .sum()
    )


def get_config() -> dict:
    return load_transform_config("minerales_transform", CONFIG_PATH)
