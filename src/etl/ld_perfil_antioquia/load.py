from __future__ import annotations

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
        
        latest_file = get_latest_run_file(gold_dir)
        if not latest_file:
            logging.warning("No se detectaron archivos de corrida en %s", gold_dir)
            return

        # 2. Control de Sincronización
        db_state = load_state("perfil_antioquia", STATE_DB_PATH)
        if db_state.get("last_loaded_file") == latest_file.name:
            logging.info("Archivo %s ya procesado. Omitiendo.", latest_file.name)
            return
        
        # 3. Lectura y Tipado (id_subregion como string para evitar pérdida de ceros)
        df_source = pd.read_csv(latest_file, dtype={'id_subregion': str})
        years_to_clean = df_source['year'].unique().tolist()

        df_source['id_subregion'] = df_source['id_subregion'].str.replace('"', '').str.strip()
        df_source = df_source.replace(['na', 'nan', ' ', ''], pd.NA)
        # CONVERSIÓN MASIVA A NUMÉRICO (FLOAT)
        # Seleccionamos todas las columnas que empiezan con 'pct_'
        cols_pct = [c for c in df_source.columns if c.startswith('pct_')]

        for col in cols_pct:
            # errors='coerce' convierte cualquier texto restante en NaN
            df_source[col] = pd.to_numeric(df_source[col], errors='coerce')
            # Opcional: Llenar nulos con 0.0 si prefieres no tener NULLs en la DB
            # df_source[col] = df_source[col].fillna(0.0)

        # Asegurar tipos enteros para llaves y fechas
        df_source['year'] = pd.to_numeric(df_source['year'], errors='coerce').astype(int)



        with engine.begin() as conn:
            # 4. Infraestructura de Tabla (Fact Table)
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