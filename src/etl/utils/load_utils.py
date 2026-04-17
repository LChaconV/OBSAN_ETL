from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Sequence

import pandas as pd
from sqlalchemy import text

from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.config_utils import load_yaml, load_state, update_state
from src.etl.utils.db_utils import get_engine


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

STATE_DB_PATH = PROJECT_ROOT / "config" / "state_db.yaml"
LOG_DIR = PROJECT_ROOT / "logs"


def get_latest_parquet_file(source_dir: Path, file_prefix: str) -> Path | None:
    files = list(source_dir.glob(f"{file_prefix}_run_*.parquet"))
    if not files:
        return None
    return sorted(files)[-1]


def build_upsert_query(
    table_name: str,
    temp_table: str,
    insert_columns: Sequence[str],
    conflict_columns: Sequence[str],
    update_columns: Sequence[str],
) -> str:
    insert_cols_sql = ", ".join(insert_columns)
    select_cols_sql = ", ".join(insert_columns)
    conflict_cols_sql = ", ".join(conflict_columns)

    if update_columns:
        update_sql = ", ".join(
            f"{col} = EXCLUDED.{col}" for col in update_columns
        )
        conflict_action = f"DO UPDATE SET {update_sql}"
    else:
        conflict_action = "DO NOTHING"

    return f"""
    INSERT INTO {table_name} ({insert_cols_sql})
    SELECT {select_cols_sql}
    FROM {temp_table}
    ON CONFLICT ({conflict_cols_sql})
    {conflict_action};
    """


def load_parquet_to_postgres(
    *,
    transform_config_path: Path,
    config_key: str,
    table_name: str,
    state_key: str,
    log_file_name: str,
    create_table_sql: str,
    create_index_sql: str | None = None,
    load_mode: str = "append",   # "append" | "upsert"
    conflict_columns: Sequence[str] | None = None,
    update_columns: Sequence[str] | None = None,
    state_field_name: str = "last_incremental_value",
) -> None:
    setup_logging(LOG_DIR, log_file_name)
    logging.info("Iniciando carga para tabla %s", table_name)

    try:
        engine = get_engine()
        config = load_yaml(transform_config_path)[config_key]

        source_dir = PROJECT_ROOT / config["source"]["golden_fact_dir"]
        file_prefix = config["fact_table"]["file_prefix"]
        temp_table = f"temp_{table_name}"

        latest_file = get_latest_parquet_file(source_dir, file_prefix)
        if not latest_file:
            logging.warning("No se encontraron archivos en la ruta: %s", source_dir)
            return

        db_state = load_state(state_key, STATE_DB_PATH)
        if db_state.get(state_field_name) == latest_file.name:
            logging.info("El archivo %s ya fue cargado. Omitiendo.", latest_file.name)
            return

        df = pd.read_parquet(latest_file)

        with engine.begin() as conn:
            logging.info("Verificando/creando tabla %s", table_name)
            conn.execute(text(create_table_sql))

            if create_index_sql:
                logging.info("Creando/verificando índice único")
                conn.execute(text(create_index_sql))

            if load_mode == "append":
                logging.info("Insertando %d registros en modo APPEND", len(df))
                df.to_sql(
                    name=table_name,
                    con=conn,
                    if_exists="append",
                    index=False,
                )

            elif load_mode == "upsert":
                if not conflict_columns:
                    raise ValueError("Para load_mode='upsert' debes definir conflict_columns")

                insert_columns = list(df.columns)
                update_columns = list(update_columns or [])

                logging.info("Subiendo datos a tabla temporal %s", temp_table)
                df.to_sql(
                    name=temp_table,
                    con=conn,
                    if_exists="replace",
                    index=False,
                )

                upsert_sql = build_upsert_query(
                    table_name=table_name,
                    temp_table=temp_table,
                    insert_columns=insert_columns,
                    conflict_columns=conflict_columns,
                    update_columns=update_columns,
                )

                logging.info("Ejecutando UPSERT sobre %s", table_name)
                conn.execute(text(upsert_sql))

                logging.info("Eliminando tabla temporal %s", temp_table)
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table};"))

            else:
                raise ValueError(f"Modo de carga no soportado: {load_mode}")

            update_state(
                key=state_key,
                incremental_value=latest_file.name,
                incremental_column="file_name",
                row_count=len(df),
                extraction_mode="golden_load",
                path_state=STATE_DB_PATH,
            )

        logging.info("Carga finalizada correctamente para %s", latest_file.name)

    except Exception as e:
        logging.critical("Fallo en la carga de %s: %s", table_name, e, exc_info=True)
        sys.exit(1)