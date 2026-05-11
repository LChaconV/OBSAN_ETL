from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.config_utils import load_yaml, save_yaml
from src.etl.utils.db_utils import get_engine


PROJECT_ROOT = Path(__file__).resolve().parents[3]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

CONFIG_PATH = PROJECT_ROOT / "config" /"transform"/ "terraclimate_transform.yaml"
STATE_DB_PATH = PROJECT_ROOT / "state" / "state_db.yaml"
LOG_DIR = PROJECT_ROOT / "logs"


def load_config() -> dict:
    return load_yaml(CONFIG_PATH)


def load_db_state() -> dict:
    if not STATE_DB_PATH.exists():
        return {}

    return load_yaml(STATE_DB_PATH)


def save_db_state(state: dict) -> None:
    STATE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    save_yaml(STATE_DB_PATH, state)


def get_golden_files(config: dict) -> list[Path]:
    golden_dir = PROJECT_ROOT / config["terraclimate_transform"]["output"]["golden_dir"]
    file_prefix = config["terraclimate_transform"]["output"]["file_prefix"]

    files = sorted(golden_dir.glob(f"{file_prefix}_*_annual_*.parquet"))

    if not files:
        raise FileNotFoundError(f"No se encontraron archivos golden en {golden_dir}")

    return files


def is_file_loaded(state: dict, file_path: Path) -> bool:
    file_name = file_path.name

    return (
        state
        .get("terraclimate_load", {})
        .get("loaded_files", {})
        .get(file_name, {})
        .get("status") == "loaded"
    )


def update_loaded_file_state(state: dict, file_path: Path, df: pd.DataFrame) -> dict:
    file_name = file_path.name

    state.setdefault("terraclimate_load", {})
    state["terraclimate_load"].setdefault("loaded_files", {})

    variables = df["variable"].dropna().unique().tolist()
    years = df["year"].dropna().unique().tolist()

    state["terraclimate_load"]["loaded_files"][file_name] = {
        "variable": variables[0] if len(variables) == 1 else variables,
        "year": int(years[0]) if len(years) == 1 else [int(y) for y in years],
        "row_count": int(len(df)),
        "loaded_at": pd.Timestamp.utcnow().isoformat(),
        "status": "loaded",
    }

    return state


def create_table(engine, table_name: str) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id_mun TEXT NOT NULL,
        year INTEGER NOT NULL,
        variable TEXT NOT NULL,
        annual_aggregation TEXT,
        value NUMERIC,
        PRIMARY KEY (id_mun, year, variable)
    );
    """

    with engine.begin() as conn:
        conn.execute(text(sql))

    logging.info("Tabla verificada/creada: %s", table_name)


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df["id_mun"] = df["id_mun"].astype(str).str.strip()
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["variable"] = df["variable"].astype(str).str.strip()
    df["annual_aggregation"] = df["annual_aggregation"].astype(str).str.strip()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    return df[
        [
            "id_mun",
            "year",
            "variable",
            "annual_aggregation",
            "value",
        ]
    ]


def upsert_dataframe(engine, df: pd.DataFrame, table_name: str) -> None:
    temp_table = f"{table_name}_tmp"

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table};"))

    df.to_sql(
        name=temp_table,
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=5000,
    )

    sql = f"""
    INSERT INTO {table_name} (
        id_mun,
        year,
        variable,
        annual_aggregation,
        value
    )
    SELECT
        id_mun,
        year,
        variable,
        annual_aggregation,
        value
    FROM {temp_table}
    ON CONFLICT (id_mun, year, variable)
    DO UPDATE SET
        annual_aggregation = EXCLUDED.annual_aggregation,
        value = EXCLUDED.value;
    """

    with engine.begin() as conn:
        conn.execute(text(sql))
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table};"))

    logging.info("UPSERT completado en tabla: %s", table_name)


def main() -> None:
    setup_logging(LOG_DIR, "terraclimate_load.log")
    logging.info("Iniciando carga TerraClimate golden a PostgreSQL")

    config = load_config()
    state = load_db_state()

    engine = get_engine()
    table_name = "terraclimate"

    create_table(engine, table_name)

    golden_files = get_golden_files(config)

    loaded_count = 0
    skipped_count = 0

    for file_path in golden_files:
        if is_file_loaded(state, file_path):
            logging.info(
                "Archivo ya cargado según state_db.yaml. Se omite: %s",
                file_path.name,
            )
            skipped_count += 1
            continue

        logging.info("Cargando archivo golden: %s", file_path.name)

        df = pd.read_parquet(file_path)
        df = normalize_dataframe(df)

        upsert_dataframe(engine, df, table_name)

        state = update_loaded_file_state(state, file_path, df)
        save_db_state(state)

        loaded_count += 1

    logging.info("Archivos cargados: %s", loaded_count)
    logging.info("Archivos omitidos: %s", skipped_count)
    logging.info("Carga TerraClimate finalizada correctamente.")


if __name__ == "__main__":
    main()