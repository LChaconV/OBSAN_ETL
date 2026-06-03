from __future__ import annotations
import os
import logging
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

# Importación de utilidades modulares
from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.config_utils import load_yaml, load_state, update_state
from src.etl.utils.db_utils import get_engine

# ============================================================
# CONFIGURACIÓN DE RUTAS (ESTÁNDAR ACTUALIZADO)
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Nuevas rutas centralizadas en /config/
STATE_DB_PATH = PROJECT_ROOT / "config" / "state_db.yaml"
SOURCES_CONFIG_PATH = PROJECT_ROOT / "config" / "sources.yaml"
LOG_DIR = PROJECT_ROOT / "logs"
PROFILE_COLUMNS = [
    "year",
    "id_subregion",
    "name_subregion",
    "pct_u5_wasting_severe",
    "pct_u5_wasting_moderate",
    "pct_u5_wasting_risk",
    "pct_u5_underweight",
    "pct_u5_underweight_risk",
    "pct_u5_underweight_normal",
    "pct_u5_stunting",
    "pct_u5_stunting_risk",
    "pct_u5_stunting_normal",
    "pct_u5_wasting_normal",
    "pct_u5_overweight_risk",
    "pct_u5_overweight",
    "pct_u5_obesity",
    "pct_5_10_thinness_risk",
    "pct_5_10_bmi_normal",
    "pct_5_18_stunting",
    "pct_5_18_stunting_risk",
    "pct_5_18_stunting_normal",
    "pct_5_18_thinness_risk",
    "pct_5_18_bmi_normal",
    "pct_5_18_overweight",
    "pct_5_18_obesity",
    "pct_u18_food_security",
    "pct_u18_food_insecurity",
    "pct_u18_food_insecurity_mild",
    "pct_u18_food_insecurity_moderate",
    "pct_u18_food_insecurity_severe",
]
REQUIRED_PROFILE_COLUMNS = {"year", "id_subregion", "name_subregion"}

# ============================================================
# UTILIDADES DE BÚSQUEDA
# ============================================================
def get_latest_run_file(directory: Path) -> Path | None:
    """Identifica el CSV de la corrida más reciente para el perfil Antioquia."""
    files = list(directory.glob("perfil_antioquia_run_*.csv"))
    if not files:
        return None
    files.sort(reverse=True)
    return files[0]

def read_profile_file(file_path: Path) -> pd.DataFrame:
    """Lee el perfil desde CSV o Excel según la extensión cargada."""
    suffix = file_path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(file_path, dtype={"id_subregion": str})
    return pd.read_csv(file_path, dtype={"id_subregion": str})

def normalize_profile_dataframe(df_source: pd.DataFrame) -> pd.DataFrame:
    """Normaliza columnas y tipos antes de cargar perfil_antioquia."""
    missing_required = REQUIRED_PROFILE_COLUMNS - set(df_source.columns)
    if missing_required:
        raise ValueError(
            "Faltan columnas obligatorias en perfil_antioquia: "
            + ", ".join(sorted(missing_required))
        )

    for col in PROFILE_COLUMNS:
        if col not in df_source.columns:
            df_source[col] = pd.NA

    df_source = df_source[PROFILE_COLUMNS].copy()
    df_source["id_subregion"] = df_source["id_subregion"].str.replace('"', '').str.strip()
    df_source = df_source.replace(['na', 'nan', ' ', ''], pd.NA)

    cols_pct = [c for c in df_source.columns if c.startswith("pct_")]
    for col in cols_pct:
        df_source[col] = pd.to_numeric(df_source[col], errors="coerce")

    df_source["year"] = pd.to_numeric(df_source["year"], errors="coerce")
    if df_source["year"].isna().any():
        raise ValueError("La columna year contiene valores vacíos o no numéricos.")
    df_source["year"] = df_source["year"].astype(int)

    return df_source

def ensure_subregion_infrastructure(conn) -> None:
    """Garantiza la dimensión subregion requerida por perfil_antioquia."""
    conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS subregion (
            id_subregion VARCHAR(20) PRIMARY KEY,
            id_dept VARCHAR(10),
            name_subregion VARCHAR(150),
            geometry GEOMETRY(MultiPolygon, 4326)
        );
    """))
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_subregion_geom
        ON subregion USING GIST (geometry);
    """))

def upsert_subregion_placeholders(conn, df_source: pd.DataFrame) -> None:
    """
    Inserta subregiones mínimas desde el perfil para permitir cargar indicadores
    antes de cargar la capa geográfica. El pipeline geográfico actualiza luego
    nombre, departamento y geometría mediante upsert.
    """
    df_subregion = (
        df_source[["id_subregion", "name_subregion"]]
        .dropna(subset=["id_subregion"])
        .drop_duplicates(subset=["id_subregion"])
    )

    if df_subregion.empty:
        logging.warning("No se encontraron id_subregion para validar la dimensión subregion.")
        return

    rows = df_subregion.where(pd.notna(df_subregion), None).to_dict("records")
    conn.execute(
        text("""
            INSERT INTO subregion (id_subregion, name_subregion)
            VALUES (:id_subregion, :name_subregion)
            ON CONFLICT (id_subregion) DO UPDATE
            SET name_subregion = COALESCE(subregion.name_subregion, EXCLUDED.name_subregion);
        """),
        rows,
    )
    logging.info("Dimensión subregion validada con %d códigos del perfil.", len(rows))

# ============================================================
# PROCESO DE CARGA
# ============================================================
def run() -> None:
    setup_logging(LOG_DIR, "load_perfil_antioquia.log")
    logging.info("Iniciando carga de indicadores: fact_perfil_antioquia")

    try:
        engine = get_engine()
        sources_config = load_yaml(SOURCES_CONFIG_PATH)
        
        # 1. Localización en Capa Gold
        gold_rel_path = sources_config["perfil_antioquia"]["path_gold"]
        gold_dir = PROJECT_ROOT / gold_rel_path
        
        #latest_file = get_latest_run_file(gold_dir)
        file_path = os.environ.get("OBSAN_INPUT_FILE")
        if not file_path:
            raise ValueError("No se definió OBSAN_INPUT_FILE")

        latest_file = Path(file_path)

        if not latest_file:
            logging.warning("No se detectaron archivos de corrida en %s", gold_dir)
            return

        # 2. Control de Sincronización
        db_state = load_state("perfil_antioquia", STATE_DB_PATH)
        if db_state.get("last_loaded_file") == latest_file.name:
            logging.info("Archivo %s ya procesado. Omitiendo.", latest_file.name)
            return
        
        # 3. Lectura y Tipado (id_subregion como string para evitar pérdida de ceros)
        df_source = normalize_profile_dataframe(read_profile_file(latest_file))
        years_to_clean = df_source['year'].unique().tolist()

        with engine.begin() as conn:
            # 4. Infraestructura de dimensiones y tabla de hechos
            ensure_subregion_infrastructure(conn)
            upsert_subregion_placeholders(conn, df_source)

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS perfil_antioquia (
                    year INTEGER,
                    id_subregion VARCHAR(20),
                    name_subregion VARCHAR(150),
                    pct_u5_wasting_severe FLOAT,
                    pct_u5_wasting_moderate FLOAT,
                    pct_u5_wasting_risk FLOAT,
                    pct_u5_underweight FLOAT,
                    pct_u5_underweight_risk FLOAT,
                    pct_u5_underweight_normal FLOAT,
                    pct_u5_stunting FLOAT,
                    pct_u5_stunting_risk FLOAT,
                    pct_u5_stunting_normal FLOAT,
                    pct_u5_wasting_normal FLOAT,
                    pct_u5_overweight_risk FLOAT,
                    pct_u5_overweight FLOAT,
                    pct_u5_obesity FLOAT,
                    pct_5_10_thinness_risk FLOAT,
                    pct_5_10_bmi_normal FLOAT,
                    pct_5_18_stunting FLOAT,
                    pct_5_18_stunting_risk FLOAT,
                    pct_5_18_stunting_normal FLOAT,
                    pct_5_18_thinness_risk FLOAT,
                    pct_5_18_bmi_normal FLOAT,
                    pct_5_18_overweight FLOAT,
                    pct_5_18_obesity FLOAT,
                    pct_u18_food_security FLOAT,
                    pct_u18_food_insecurity FLOAT,
                    pct_u18_food_insecurity_mild FLOAT,
                    pct_u18_food_insecurity_moderate FLOAT,
                    pct_u18_food_insecurity_severe FLOAT,
                    CONSTRAINT fk_subregion_perfil 
                        FOREIGN KEY(id_subregion) 
                        REFERENCES subregion(id_subregion)
                );
            """))

            # 5. Idempotencia: Borrado por Años presentes en el CSV
            logging.info("Limpiando registros previos para los años: %s", years_to_clean)
            conn.execute(
                text("DELETE FROM perfil_antioquia WHERE year IN :years"),
                {"years": tuple(years_to_clean)}
            )

            # 6. Inserción
            logging.info("Insertando %d registros desde %s", len(df_source), latest_file.name)
            df_source.to_sql("perfil_antioquia", conn, if_exists="append", index=False)
            
            # 7. Actualización de Estado (en config/state_db.yaml)
            update_state(
                key="perfil_antioquia",
                incremental_value=latest_file.name,
                incremental_column="run_filename",
                row_count=len(df_source),
                extraction_mode="db_load_gold_run",
                path_state=STATE_DB_PATH
            )
            logging.info("Carga de Perfil Antioquia finalizada exitosamente.")

    except Exception as e:
        logging.critical("Fallo en la carga de Perfil Antioquia: %s", str(e), exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    run()
