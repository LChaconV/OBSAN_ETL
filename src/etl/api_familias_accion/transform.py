from __future__ import annotations

import logging
import sqlite3
import sys
from tempfile import TemporaryDirectory
from pathlib import Path

import pandas as pd

from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.transform_utils import load_transform_config, get_latest_bronze_run,extract_run_name,clean_columns,clean_text_data,validate_required_columns,normalize_types,save_fact_table

# ============================================================
# RUTAS DEL PROYECTO
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parents[3]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "familias_accion_transform.yaml"
LOG_DIR = PROJECT_ROOT / "logs"


def build_familias_benefit_fact(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    fact_cfg = config["fact_table"]
    
    
    grain = fact_cfg["grain"] 
    metric_column = fact_cfg["metric_column"] 
    rename_columns = fact_cfg["rename_columns"]

    
    df_temp = df.copy()
    df_temp['fechaultimobeneficioasignado'] = pd.to_datetime(df_temp['fechaultimobeneficioasignado']).dt.to_period('M').dt.to_timestamp()

    familias_benefit_fact = (
        df_temp.groupby(grain, dropna=False, as_index=False)[metric_column]
        .sum()
        .rename(columns=rename_columns)
    )
    familias_benefit_fact = clean_text_data(familias_benefit_fact )

    familias_benefit_fact = familias_benefit_fact.sort_values(
        ["date_event", "id_dept", "id_mun"]
    ).reset_index(drop=True)

    logging.info("Filas finales de familias_benefit_fact (mensual): %s", len(familias_benefit_fact))

    return familias_benefit_fact


def read_bronze_page(file_path: Path, config: dict) -> pd.DataFrame:
    required_columns = config["validation"]["required_columns"]

    try:
        df = pd.read_parquet(file_path, columns=required_columns)
    except TypeError:
        df = pd.read_parquet(file_path)

    df = clean_columns(df)
    validate_required_columns(df, required_columns)
    df = normalize_types(df, config)

    return df


def prepare_page_for_sqlite(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    dedup_cfg = config["deduplication"]
    fact_cfg = config["fact_table"]

    id_column = dedup_cfg["id_column"]
    order_column = dedup_cfg["order_column"]
    date_column = "fechaultimobeneficioasignado"
    metric_column = fact_cfg["metric_column"]

    columns = [
        id_column,
        order_column,
        date_column,
        "codigodepartamentoatencion",
        "codigomunicipioatencion",
        "tipobeneficio",
        "estadobeneficiario",
        metric_column,
    ]

    df_page = df[columns].copy()
    df_page[order_column] = pd.to_datetime(df_page[order_column], errors="coerce", utc=True)
    df_page = (
        df_page
        .dropna(subset=[id_column, order_column])
        .sort_values([id_column, order_column])
        .drop_duplicates(subset=[id_column], keep="last")
    )

    benefit_date = pd.to_datetime(df_page[date_column], errors="coerce", utc=True)
    df_page[date_column] = (
        benefit_date
        .dt.tz_convert(None)
        .dt.to_period("M")
        .dt.to_timestamp()
        .dt.strftime("%Y-%m-%d")
    )
    df_page[order_column] = df_page[order_column].astype("int64")
    df_page[metric_column] = pd.to_numeric(df_page[metric_column], errors="coerce")

    for column in [
        id_column,
        "codigodepartamentoatencion",
        "codigomunicipioatencion",
        "tipobeneficio",
        "estadobeneficiario",
    ]:
        df_page[column] = df_page[column].astype("string").str.strip()

    return df_page.where(pd.notna(df_page), None)


def initialize_latest_rows_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE latest_familias_accion (
            source_id TEXT PRIMARY KEY,
            updated_at_ns INTEGER NOT NULL,
            date_event TEXT,
            id_dept TEXT,
            id_mun TEXT,
            benefit_type TEXT,
            status TEXT,
            beneficiary_count REAL
        );
    """)


def upsert_latest_rows(conn: sqlite3.Connection, df_page: pd.DataFrame) -> None:
    rows = [
        (
            row[":id"],
            int(row[":updated_at"]),
            row["fechaultimobeneficioasignado"],
            row["codigodepartamentoatencion"],
            row["codigomunicipioatencion"],
            row["tipobeneficio"],
            row["estadobeneficiario"],
            row["cantidaddebeneficiarios"],
        )
        for row in df_page.to_dict("records")
    ]

    if not rows:
        return

    conn.executemany(
        """
        INSERT INTO latest_familias_accion (
            source_id,
            updated_at_ns,
            date_event,
            id_dept,
            id_mun,
            benefit_type,
            status,
            beneficiary_count
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_id) DO UPDATE SET
            updated_at_ns = excluded.updated_at_ns,
            date_event = excluded.date_event,
            id_dept = excluded.id_dept,
            id_mun = excluded.id_mun,
            benefit_type = excluded.benefit_type,
            status = excluded.status,
            beneficiary_count = excluded.beneficiary_count
        WHERE excluded.updated_at_ns >= latest_familias_accion.updated_at_ns;
        """,
        rows,
    )


def build_familias_benefit_fact_from_sqlite(conn: sqlite3.Connection) -> pd.DataFrame:
    familias_benefit_fact = pd.read_sql_query(
        """
        SELECT
            date_event,
            id_dept,
            id_mun,
            benefit_type,
            status AS estadobeneficiario,
            SUM(beneficiary_count) AS beneficiary_count
        FROM latest_familias_accion
        GROUP BY
            date_event,
            id_dept,
            id_mun,
            benefit_type,
            status
        """,
        conn,
        parse_dates=["date_event"],
    )

    familias_benefit_fact = clean_text_data(familias_benefit_fact)
    familias_benefit_fact = familias_benefit_fact.sort_values(
        ["date_event", "id_dept", "id_mun"]
    ).reset_index(drop=True)

    logging.info(
        "Filas finales de familias_benefit_fact (mensual): %s",
        len(familias_benefit_fact),
    )

    return familias_benefit_fact


def build_familias_benefit_fact_from_run(run_dir: Path, config: dict) -> pd.DataFrame:
    files = sorted(run_dir.glob("*.parquet"))

    if not files:
        raise ValueError(f"No se encontraron archivos parquet en {run_dir}")

    logging.info("Archivos parquet encontrados en la corrida: %s", len(files))

    with TemporaryDirectory(prefix="familias_accion_") as tmp_dir:
        db_path = Path(tmp_dir) / "familias_accion.sqlite"
        conn = sqlite3.connect(db_path)

        try:
            initialize_latest_rows_table(conn)

            total_rows = 0
            for index, file_path in enumerate(files, start=1):
                df_page = read_bronze_page(file_path, config)
                total_rows += len(df_page)
                prepared_page = prepare_page_for_sqlite(df_page, config)
                upsert_latest_rows(conn, prepared_page)

                if index % 10 == 0 or index == len(files):
                    conn.commit()
                    logging.info(
                        "Transformadas %s/%s páginas bronze (%s filas leídas).",
                        index,
                        len(files),
                        total_rows,
                    )

            conn.commit()
            logging.info("Filas cargadas desde bronze: %s", total_rows)

            return build_familias_benefit_fact_from_sqlite(conn)
        finally:
            conn.close()

# ============================================================
# RESUMEN
# ============================================================
def log_summary(df: pd.DataFrame, familias_benefit_fact: pd.DataFrame) -> None:
    logging.info("Resumen de transformación:")
    logging.info("Filas finales en familias_benefit_fact: %s", len(familias_benefit_fact))

    if not familias_benefit_fact.empty:
        logging.info("Fecha mínimo en familias_benefit_fact: %s", familias_benefit_fact["date_event"].min())
        logging.info("Fecha máximo en familias_benefit_fact: %s", familias_benefit_fact["date_event"].max())


# ============================================================
# MAIN
# ============================================================
def run() -> None:
    setup_logging(LOG_DIR, "familias_accion_transform.log")
    logging.info("Iniciando transformación de beneficiarios de Familias en Acción")

    config = load_transform_config("familias_accion_transform", CONFIG_PATH)

    bronze_dir = PROJECT_ROOT / config["source"]["bronze_dir"]
    fact_dir = PROJECT_ROOT / config["source"]["silver_fact_dir"]

    run_dir = get_latest_bronze_run(bronze_dir)
    run_name = extract_run_name(run_dir)

    familias_benefit_fact = build_familias_benefit_fact_from_run(run_dir, config)

    save_fact_table(familias_benefit_fact, run_name, fact_dir, config, "familias_accion")
    log_summary(pd.DataFrame(), familias_benefit_fact)

    logging.info("Transformación finalizada correctamente")


if __name__ == "__main__":
    run()
