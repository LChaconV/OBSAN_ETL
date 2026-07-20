from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

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

CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "oro_aluvion_transform.yaml"


def build_table(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Recibe el DataFrame crudo de un año, aplica limpieza, deduplicación
    por :id (fuente Socrata) y agrega al grano año + municipio.
    """
    fact_cfg       = config["fact_table"]
    grain          = fact_cfg["grain"]
    metric_columns = fact_cfg["metric_columns"]
    rename_columns = fact_cfg["rename_columns"]

    df = clean_columns(df)
    df = normalize_types(df, config)
    df = deduplicate_by_id(df, config)

    table_fact = (
        df.groupby(grain, dropna=False, as_index=False)[metric_columns]
        .sum()
        .rename(columns=rename_columns)
    )

    table_fact = clean_text_data(table_fact)

    # Zero-pad id_mun a 5 dígitos (la API entrega "5031" en lugar de "05031")
    table_fact["id_mun"] = table_fact["id_mun"].apply(
        lambda v: str(int(float(v))).zfill(5) if pd.notna(v) and str(v).strip() != "" else None
    )
    table_fact = table_fact[table_fact["id_mun"].notna()]

    # Año como entero
    table_fact["year"] = table_fact["year"].apply(
        lambda v: int(float(v)) if pd.notna(v) else None
    )

    table_fact = table_fact.sort_values(["year", "id_mun"]).reset_index(drop=True)

    logging.info("Filas aggregadas tras transformar: %s", len(table_fact))
    return table_fact


def build_golden(table_fact: pd.DataFrame) -> pd.DataFrame:
    """Agrega al grano final de carga (municipio + año)."""
    return (
        table_fact
        .groupby(["id_mun", "year"], dropna=False, as_index=False)[["illicit_hectares", "total_evidence"]]
        .sum()
    )


def get_config() -> dict:
    return load_transform_config("oro_aluvion_transform", CONFIG_PATH)


if __name__ == "__main__":
    import logging
    from src.etl.api_oro_aluvion.extract import fetch_year_data, find_all_years_in_api

    logging.basicConfig(level=logging.INFO, format="%(levelname)s — %(message)s")

    config = get_config()
    print(f"Config cargada: grain={config['fact_table']['grain']}, "
          f"metrics={config['fact_table']['metric_columns']}")

    years = find_all_years_in_api()
    if not years:
        print("No se encontraron años en la API. Abortando.")
    else:
        for year in years:
            print(f"\n{'='*50}")
            print(f"Año: {year}")

            print("  [1] Descargando datos crudos...")
            df_raw = fetch_year_data(year)
            print(f"      Filas crudas     : {len(df_raw)}")
            print(f"      Columnas         : {list(df_raw.columns)}")

            if df_raw.empty:
                print("      Sin datos. Se omite.")
                continue

            print("  [2] Aplicando build_table...")
            table_fact = build_table(df_raw, config)
            print(f"      Filas agregadas  : {len(table_fact)}")
            print(f"      id_mun únicos    : {table_fact['id_mun'].nunique()}")
            print(f"      Muestra:")
            print(table_fact.head(5).to_string(index=False))

            print("  [3] Aplicando build_golden...")
            df_golden = build_golden(table_fact)
            print(f"      Filas golden     : {len(df_golden)}")
            print(f"      Columnas         : {list(df_golden.columns)}")
            print(f"      Muestra:")
            print(df_golden.head(5).to_string(index=False))

            print(f"\n  Rango illicit_hectares : "
                  f"min={df_golden['illicit_hectares'].min():.2f}, "
                  f"max={df_golden['illicit_hectares'].max():.2f}")
            print(f"  Rango total_evidence    : "
                  f"min={df_golden['total_evidence'].min():.2f}, "
                  f"max={df_golden['total_evidence'].max():.2f}")
            pct = (df_golden['illicit_hectares'] / df_golden['total_evidence'].replace(0, float('nan')) * 100)
            print(f"  % ilícito (media)        : {pct.mean():.1f}%")
