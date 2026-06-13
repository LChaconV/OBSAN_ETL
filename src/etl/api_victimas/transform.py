from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.config_utils import load_yaml


# ============================================================
# RUTAS DEL PROYECTO
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parents[3]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "victimas_transform.yaml"
LOG_DIR = PROJECT_ROOT / "logs"


# ============================================================
# CONFIG
# ============================================================
def load_transform_config() -> dict:
    config = load_yaml(CONFIG_PATH)

    if "victimas_transform" not in config:
        raise KeyError("No existe la clave 'victimas_transform' en el YAML de transformación.")

    return config["victimas_transform"]


# ============================================================
# BRONZE
# ============================================================
def get_latest_bronze_run(bronze_dir: Path) -> Path:
    run_dirs = [
        path for path in bronze_dir.iterdir()
        if path.is_dir() and path.name.startswith("run_")
    ]

    if not run_dirs:
        raise ValueError(f"No se encontraron carpetas run_ en {bronze_dir}")

    latest_run = sorted(run_dirs)[-1]
    logging.info("Carpeta bronze más reciente: %s", latest_run.name)

    return latest_run


def load_latest_bronze_run(run_dir: Path) -> pd.DataFrame:
    files = sorted(run_dir.glob("*.parquet"))

    if not files:
        raise ValueError(f"No se encontraron archivos parquet en {run_dir}")

    logging.info("Archivos parquet encontrados en la corrida: %s", len(files))

    dfs = [pd.read_parquet(file) for file in files]
    df = pd.concat(dfs, ignore_index=True)

    logging.info("Filas cargadas desde bronze: %s", len(df))
    return df


def extract_run_name(run_dir: Path) -> str:
    return run_dir.name


# ============================================================
# LIMPIEZA
# ============================================================
def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(col).strip().lower() for col in df.columns]
    return df


def validate_required_columns(df: pd.DataFrame, required_columns: list[str]) -> None:
    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        raise ValueError(f"Faltan columnas requeridas en bronze: {missing}")


def normalize_types(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    typing_cfg = config["typing"]

    for col in typing_cfg.get("numeric_columns", []):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in typing_cfg.get("datetime_columns", []):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    for col in typing_cfg.get("text_columns", []):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    return df


def parse_fecha_corte(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    date_cfg = config["date_parsing"]

    source_col = date_cfg["source_column"]
    raw_col = date_cfg["raw_column"]
    parsed_col = date_cfg["parsed_column"]
    invalid_col = date_cfg["invalid_flag_column"]
    formats = date_cfg["formats"]

    df[raw_col] = df[source_col]

    parsed_series = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")

    for fmt in formats:
        parsed_try = pd.to_datetime(
            df[raw_col],
            format=fmt,
            errors="coerce",
           # dayfirst=True if fmt == "%d/%m/%Y" else False,
        )
        parsed_series = parsed_series.fillna(parsed_try)

    df[parsed_col] = parsed_series
    df[invalid_col] = df[parsed_col].isna()

    return df




    
# ============================================================
# DEDUPLICACIÓN Y REGLAS DE NEGOCIO
# ============================================================
def deduplicate_by_id(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    dedup_cfg = config["deduplication"]

    id_column = dedup_cfg["id_column"]
    order_column = dedup_cfg["order_column"]
    keep = dedup_cfg["keep"]

    before = len(df)

    df = df.sort_values([id_column, order_column])
    df = df.drop_duplicates(subset=[id_column], keep=keep)

    after = len(df)
    logging.info("Duplicados técnicos eliminados por %s: %s", id_column, before - after)

    return df


def apply_business_rules(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    rules_cfg = config["business_rules"]
    metric_column = rules_cfg["metric_column"]

    if rules_cfg.get("drop_null_in_metric", False):
        before = len(df)
        df = df.dropna(subset=[metric_column])
        after = len(df)
        logging.info("Filas excluidas por %s nulo: %s", metric_column, before - after)

    return df


# ============================================================
# DIMENSIÓN ESTÁTICA
# ============================================================
def build_current_event_dimension(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    dim_cfg = config["dimensions"]["victim_event_type"]

    source_id_col = dim_cfg["source_id_column"]
    source_name_col = dim_cfg["source_name_column"]
    output_id_col = dim_cfg["output_id_column"]
    output_name_col = dim_cfg["output_name_column"]

    event_df = (
        df[[source_id_col, source_name_col]]
        .dropna(subset=[source_id_col])
        .drop_duplicates()
        .rename(
            columns={
                source_id_col: output_id_col,
                source_name_col: output_name_col,
            }
        )
        .sort_values(output_id_col)
        .reset_index(drop=True)
    )

    event_df[output_id_col] = pd.to_numeric(event_df[output_id_col], errors="coerce")
    event_df = event_df.dropna(subset=[output_id_col])
    event_df[output_id_col] = event_df[output_id_col].astype(int)

    return event_df


def upsert_victim_event_type(df: pd.DataFrame, dimension_dir: Path, config: dict) -> pd.DataFrame:
    """
    Crea la dimensión si no existe.
    Si ya existe, agrega solo nuevos eventos.
    """
    dim_cfg = config["dimensions"]["victim_event_type"]
    file_name = dim_cfg["file_name"]
    output_id_col = dim_cfg["output_id_column"]
    output_name_col = dim_cfg["output_name_column"]

    dimension_dir.mkdir(parents=True, exist_ok=True)
    dim_path = dimension_dir / file_name

    current_dim = build_current_event_dimension(df, config)

    if not dim_path.exists():
        current_dim.to_parquet(dim_path, index=False)
        logging.info("Dimensión victim_event_type creada en: %s", dim_path)
        logging.info("Eventos guardados: %s", len(current_dim))
        return current_dim

    existing_dim = pd.read_parquet(dim_path)

    combined_dim = (
        pd.concat([existing_dim, current_dim], ignore_index=True)
        .drop_duplicates(subset=[output_id_col], keep="last")
        .sort_values(output_id_col)
        .reset_index(drop=True)
    )

    combined_dim.to_parquet(dim_path, index=False)

    new_events = len(combined_dim) - len(existing_dim)
    logging.info("Dimensión victim_event_type actualizada en: %s", dim_path)
    logging.info("Nuevos eventos agregados: %s", new_events)
    logging.info("Total eventos en dimensión: %s", len(combined_dim))

    return combined_dim


def read_bronze_page(file_path: Path, config: dict) -> pd.DataFrame:
    required_columns = config["validation"]["required_columns"]

    try:
        df = pd.read_parquet(file_path, columns=required_columns)
    except TypeError:
        df = pd.read_parquet(file_path)

    df = clean_columns(df)
    validate_required_columns(df, required_columns)
    df = normalize_types(df, config)
    df = parse_fecha_corte(df, config)

    return df


def prepare_page_for_sqlite(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    dedup_cfg = config["deduplication"]
    fact_cfg = config["fact_table"]
    date_cfg = config["date_parsing"]

    id_column = dedup_cfg["id_column"]
    order_column = dedup_cfg["order_column"]
    parsed_date_column = date_cfg["parsed_column"]
    invalid_date_column = date_cfg["invalid_flag_column"]
    metric_column = fact_cfg["metric_column"]

    columns = [
        id_column,
        order_column,
        parsed_date_column,
        invalid_date_column,
        "cod_ciudad_muni",
        "param_hecho",
        "hecho",
        "sexo",
        metric_column,
    ]

    df_page = df[columns].copy()
    df_page[order_column] = pd.to_datetime(df_page[order_column], errors="coerce", utc=True)
    df_page = (
        df_page
        .dropna(subset=[id_column, order_column])
        .sort_values([id_column, order_column])
        .drop_duplicates(subset=[id_column], keep=dedup_cfg["keep"])
    )

    df_page[parsed_date_column] = (
        pd.to_datetime(df_page[parsed_date_column], errors="coerce")
        .dt.strftime("%Y-%m-%d")
    )
    df_page[invalid_date_column] = df_page[invalid_date_column].fillna(True).astype(int)
    df_page[order_column] = df_page[order_column].astype("int64")
    df_page["param_hecho"] = pd.to_numeric(df_page["param_hecho"], errors="coerce")
    df_page[metric_column] = pd.to_numeric(df_page[metric_column], errors="coerce")

    for column in [id_column, "cod_ciudad_muni", "hecho", "sexo"]:
        df_page[column] = df_page[column].astype("string").str.strip()

    return df_page.astype(object).where(pd.notna(df_page), None)


def initialize_latest_rows_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE latest_victimas (
            source_id TEXT PRIMARY KEY,
            updated_at_ns INTEGER NOT NULL,
            date_victim TEXT,
            invalid_date INTEGER,
            id_mun TEXT,
            id_victim_event REAL,
            event_name TEXT,
            sexo TEXT,
            victim_count REAL
        );
    """)


def upsert_latest_rows(conn: sqlite3.Connection, df_page: pd.DataFrame) -> None:
    rows = [
        (
            row[":id"],
            int(row[":updated_at"]),
            row["date_victim"],
            row["invalid_date"],
            row["cod_ciudad_muni"],
            row["param_hecho"],
            row["hecho"],
            row["sexo"],
            row["per_ocu"],
        )
        for row in df_page.to_dict("records")
    ]

    if not rows:
        return

    conn.executemany(
        """
        INSERT INTO latest_victimas (
            source_id,
            updated_at_ns,
            date_victim,
            invalid_date,
            id_mun,
            id_victim_event,
            event_name,
            sexo,
            victim_count
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_id) DO UPDATE SET
            updated_at_ns = excluded.updated_at_ns,
            date_victim = excluded.date_victim,
            invalid_date = excluded.invalid_date,
            id_mun = excluded.id_mun,
            id_victim_event = excluded.id_victim_event,
            event_name = excluded.event_name,
            sexo = excluded.sexo,
            victim_count = excluded.victim_count
        WHERE excluded.updated_at_ns >= latest_victimas.updated_at_ns;
        """,
        rows,
    )


def upsert_victim_event_type_from_sqlite(
    conn: sqlite3.Connection,
    dimension_dir: Path,
    config: dict,
) -> pd.DataFrame:
    dim_cfg = config["dimensions"]["victim_event_type"]
    output_id_col = dim_cfg["output_id_column"]
    output_name_col = dim_cfg["output_name_column"]
    file_name = dim_cfg["file_name"]

    dimension_dir.mkdir(parents=True, exist_ok=True)
    dim_path = dimension_dir / file_name

    current_dim = pd.read_sql_query(
        """
        SELECT DISTINCT
            id_victim_event,
            event_name
        FROM latest_victimas
        WHERE id_victim_event IS NOT NULL
        """,
        conn,
    ).rename(
        columns={
            "id_victim_event": output_id_col,
            "event_name": output_name_col,
        }
    )

    current_dim[output_id_col] = pd.to_numeric(current_dim[output_id_col], errors="coerce")
    current_dim = (
        current_dim
        .dropna(subset=[output_id_col])
        .drop_duplicates(subset=[output_id_col], keep="last")
        .sort_values(output_id_col)
        .reset_index(drop=True)
    )
    current_dim[output_id_col] = current_dim[output_id_col].astype(int)

    if not dim_path.exists():
        current_dim.to_parquet(dim_path, index=False)
        logging.info("Dimensión victim_event_type creada en: %s", dim_path)
        logging.info("Eventos guardados: %s", len(current_dim))
        return current_dim

    existing_dim = pd.read_parquet(dim_path)
    combined_dim = (
        pd.concat([existing_dim, current_dim], ignore_index=True)
        .drop_duplicates(subset=[output_id_col], keep="last")
        .sort_values(output_id_col)
        .reset_index(drop=True)
    )
    combined_dim.to_parquet(dim_path, index=False)

    logging.info("Dimensión victim_event_type actualizada en: %s", dim_path)
    logging.info("Nuevos eventos agregados: %s", len(combined_dim) - len(existing_dim))
    logging.info("Total eventos en dimensión: %s", len(combined_dim))

    return combined_dim


def build_victim_unit_from_sqlite(conn: sqlite3.Connection, config: dict) -> pd.DataFrame:
    output_columns = config["fact_table"]["output_columns"]

    victim_unit = pd.read_sql_query(
        """
        SELECT
            date_victim,
            id_mun,
            id_victim_event,
            sexo,
            SUM(victim_count) AS victim_count
        FROM latest_victimas
        WHERE victim_count IS NOT NULL
        GROUP BY
            date_victim,
            id_mun,
            id_victim_event,
            sexo
        """,
        conn,
        parse_dates=["date_victim"],
    )

    victim_unit["id_victim_event"] = pd.to_numeric(
        victim_unit["id_victim_event"],
        errors="coerce",
    )
    victim_unit = victim_unit.sort_values(
        ["date_victim", "id_mun", "id_victim_event", "sexo"]
    ).reset_index(drop=True)
    victim_unit = victim_unit[output_columns]

    logging.info("Filas finales de victim_unit: %s", len(victim_unit))

    return victim_unit


def build_victim_unit_golden_from_sqlite(conn: sqlite3.Connection) -> pd.DataFrame:
    victim_unit_golden = pd.read_sql_query(
        """
        SELECT
            CAST(strftime('%Y', date_victim) AS INTEGER) AS year,
            id_mun,
            id_victim_event,
            sexo,
            SUM(victim_count) AS victim_count
        FROM latest_victimas
        WHERE victim_count IS NOT NULL
        GROUP BY
            year,
            id_mun,
            id_victim_event,
            sexo
        """,
        conn,
    )

    victim_unit_golden["id_victim_event"] = pd.to_numeric(
        victim_unit_golden["id_victim_event"],
        errors="coerce",
    )

    logging.info("Filas finales de victim_unit_golden: %s", len(victim_unit_golden))

    return victim_unit_golden


def build_outputs_from_run(
    run_dir: Path,
    dimension_dir: Path,
    config: dict,
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    files = sorted(run_dir.glob("*.parquet"))

    if not files:
        raise ValueError(f"No se encontraron archivos parquet en {run_dir}")

    logging.info("Archivos parquet encontrados en la corrida: %s", len(files))

    with TemporaryDirectory(prefix="victimas_") as tmp_dir:
        db_path = Path(tmp_dir) / "victimas.sqlite"
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

            upsert_victim_event_type_from_sqlite(conn, dimension_dir, config)
            victim_unit = build_victim_unit_from_sqlite(conn, config)
            victim_unit_golden = build_victim_unit_golden_from_sqlite(conn)
            invalid_dates = int(
                conn.execute(
                    "SELECT COUNT(*) FROM latest_victimas WHERE invalid_date = 1"
                ).fetchone()[0]
            )

            return victim_unit, victim_unit_golden, invalid_dates
        finally:
            conn.close()


# ============================================================
# TABLA DE HECHOS SILVER
# ============================================================
def build_victim_unit(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    fact_cfg = config["fact_table"]

    grain = fact_cfg["grain"]
    metric_column = fact_cfg["metric_column"]
    rename_columns = fact_cfg["rename_columns"]
    output_columns = fact_cfg["output_columns"]

    victim_unit = (
        df.groupby(grain, dropna=False, as_index=False)[metric_column]
        .sum()
        .rename(columns=rename_columns)
    )

    victim_unit = victim_unit.sort_values(
        ["date_victim", "id_mun", "id_victim_event", "sexo"]
    ).reset_index(drop=True)


    if "id_victim_event" in victim_unit.columns:
        victim_unit["id_victim_event"] = pd.to_numeric(
            victim_unit["id_victim_event"],
            errors="coerce"
        )

    victim_unit = victim_unit[output_columns]

    logging.info("Filas finales de victim_unit: %s", len(victim_unit))

    return victim_unit

# ============================================================
# TABLA DE HECHOS GOLDEN
# ============================================================
def build_victim_unit_golden(df: pd.DataFrame, config: dict) -> pd.DataFrame:

    fact_cfg = config["fact_table"]
    grain = fact_cfg["grain"]
    metric_column = fact_cfg["metric_column"]
    rename_columns = fact_cfg["rename_columns"]


    df_golden = df.copy()
    df_golden["year"] = df_golden["date_victim"].dt.year

    golden_grain = [col for col in grain if col != "date_victim"]
    
    if "year" not in golden_grain:
        golden_grain.append("year")
    
    logging.info("Agrupando en Capa Golden por: %s", golden_grain)

    victim_unit_golden = (
        df_golden.groupby(golden_grain, dropna=False, as_index=False)[metric_column]
        .sum()
        .rename(columns=rename_columns)
    )



    logging.info("Filas finales de victim_unit_golden: %s", len(victim_unit_golden))

    return victim_unit_golden



# ============================================================
# GUARDADO
# ============================================================
def save_victim_unit(
    victim_unit: pd.DataFrame,
    run_name: str,
    fact_dir: Path,
    config: dict,
) -> None:
    fact_dir.mkdir(parents=True, exist_ok=True)

    fact_prefix = config["fact_table"]["file_prefix"]
    victim_unit_path = fact_dir / f"{fact_prefix}_{run_name}.parquet"

    victim_unit.to_parquet(victim_unit_path, index=False)

    logging.info("Archivo silver victim_unit guardado en: %s", victim_unit_path)


# ============================================================
# RESUMEN
# ============================================================
def log_summary(victim_unit: pd.DataFrame, invalid_dates: int) -> None:
    logging.info("Resumen de transformación:")
    logging.info("Fechas no parseadas: %s", invalid_dates)

    if not victim_unit.empty:
        logging.info("Fecha mínima en victim_unit: %s", victim_unit["date_victim"].min())
        logging.info("Fecha máxima en victim_unit: %s", victim_unit["date_victim"].max())

    logging.info("Filas finales en victim_unit: %s", len(victim_unit))


# ============================================================
# MAIN
# ============================================================
def run() -> None:
    setup_logging(LOG_DIR, "victimas_transform.log")
    logging.info("Iniciando transformación de víctimas")

    config = load_transform_config()

    bronze_dir = PROJECT_ROOT / config["source"]["bronze_dir"]
    fact_dir = PROJECT_ROOT / config["source"]["silver_fact_dir"]
    fact_dir_golden = PROJECT_ROOT / config["source"]["golden_fact_dir"]
    dimension_dir = PROJECT_ROOT / config["source"]["silver_dimension_dir"]

    run_dir = get_latest_bronze_run(bronze_dir)
    run_name = extract_run_name(run_dir)

    victim_unit, victim_unit_golden, invalid_dates = build_outputs_from_run(
        run_dir,
        dimension_dir,
        config,
    )

    save_victim_unit(victim_unit, run_name, fact_dir, config)
    save_victim_unit(victim_unit_golden, run_name, fact_dir_golden, config)
    log_summary(victim_unit, invalid_dates)

    logging.info("Transformación finalizada correctamente")


if __name__ == "__main__":
    run()
