"""
scripts/audit/data_audit.py

Funciones reutilizables para auditar la completitud y calidad de
cualquier tabla/vista del DW antes de incorporarla a un índice
compuesto (p. ej. el índice de vulnerabilidad).

Todas las funciones reciben el nombre de la tabla/vista y los nombres
de columna (año, geografía, valor) como parámetros, para poder
reutilizarlas con otras fuentes sin reescribir SQL.

Por defecto usa la conexión ya existente en apps/streamlit/core/db.py
(lee credenciales de .env). Para auditar una base de datos distinta
(p. ej. una BD de pruebas), construye otra función de consulta con
`make_query_fn(env_file=...)` y pásala como `query_fn` a cada función.
"""

import sys
from pathlib import Path
from typing import Callable

import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import dotenv_values

PROJECT_ROOT = Path(__file__).resolve().parents[2]
STREAMLIT_APP_DIR = PROJECT_ROOT / "apps" / "streamlit"
if str(STREAMLIT_APP_DIR) not in sys.path:
    sys.path.insert(0, str(STREAMLIT_APP_DIR))

from core.db import query_rows  # noqa: E402  (conexión por defecto, vía .env)


def make_query_fn(env_file: Path | str) -> Callable:
    """
    Construye una función de consulta equivalente a query_rows pero
    apuntando a las credenciales de otro archivo .env (p. ej. una BD
    de pruebas), sin tocar la conexión por defecto ni el entorno global.
    """
    env_file = Path(env_file)
    if not env_file.exists():
        raise FileNotFoundError(
            f"No se encontró {env_file}. Crea ese archivo (no se versiona en git) "
            f"con DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD."
        )
    env_values = dotenv_values(env_file)
    db_config = {
        "host": env_values.get("DB_HOST", "localhost"),
        "port": int(env_values.get("DB_PORT", 5432)),
        "dbname": env_values.get("DB_NAME", env_values.get("POSTGRES_DB", "postgres")),
        "user": env_values.get("DB_USER", env_values.get("POSTGRES_USER", "postgres")),
        "password": env_values.get("DB_PASSWORD", env_values.get("POSTGRES_PASSWORD", "")),
        "connect_timeout": 10,
    }

    def _query_rows(sql: str, params: tuple | dict = None) -> list[dict]:
        conn = psycopg2.connect(**db_config)
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(sql, params)
            rows = cur.fetchall()
            cur.close()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    return _query_rows


TEST_DB_ENV_FILE = PROJECT_ROOT / ".env.audit"


# ---------------------------------------------------------------------
# 1. Esquema real de la tabla/vista
# ---------------------------------------------------------------------
def get_table_schema(table_name: str, query_fn: Callable = query_rows) -> pd.DataFrame:
    """Devuelve columnas y tipo de dato de una tabla/vista, en su orden real."""
    rows = query_fn(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %(table_name)s
        ORDER BY ordinal_position
        """,
        {"table_name": table_name},
    )
    return pd.DataFrame(rows)


def print_schema(table_name: str, query_fn: Callable = query_rows) -> pd.DataFrame:
    schema = get_table_schema(table_name, query_fn=query_fn)
    print(f"\n=== Esquema real de {table_name} ===")
    if schema.empty:
        print(f"  No se encontraron columnas. ¿Existe la tabla/vista '{table_name}'?")
    else:
        print(schema.to_string(index=False))
    return schema


# ---------------------------------------------------------------------
# 2. Cobertura general
# ---------------------------------------------------------------------
def get_coverage_overview(
    table_name: str,
    year_col: str,
    geo_col: str,
    geo_universe_table: str = "dim_divipola",
    geo_universe_col: str = "id_mun",
    dept_col: str = "id_dept",
    dept_value: str = "05",
    query_fn: Callable = query_rows,
) -> dict:
    """
    Rango de años, número de años distintos, número de municipios del
    departamento con al menos un dato, total de filas y tamaño del
    universo de municipios del departamento (p. ej. Antioquia).
    """
    stats = query_fn(
        f"""
        SELECT
            MIN({year_col})              AS min_year,
            MAX({year_col})              AS max_year,
            COUNT(DISTINCT {year_col})   AS n_years,
            COUNT(*)                     AS total_rows
        FROM {table_name}
        """
    )[0]

    n_geo_with_data = query_fn(
        f"""
        SELECT COUNT(DISTINCT t.{geo_col}) AS n
        FROM {table_name} t
        JOIN {geo_universe_table} d ON d.{geo_universe_col} = t.{geo_col}
        WHERE d.{dept_col} = %(dept_value)s
        """,
        {"dept_value": dept_value},
    )[0]["n"]

    universe_size = query_fn(
        f"""
        SELECT COUNT(*) AS n
        FROM {geo_universe_table}
        WHERE {dept_col} = %(dept_value)s
        """,
        {"dept_value": dept_value},
    )[0]["n"]

    return {
        "min_year": stats["min_year"],
        "max_year": stats["max_year"],
        "n_years": stats["n_years"],
        "total_rows": stats["total_rows"],
        "n_municipios_con_dato": n_geo_with_data,
        "universo_municipios": universe_size,
        "pct_municipios_con_dato": round(
            100.0 * n_geo_with_data / universe_size, 1
        ) if universe_size else None,
    }


# ---------------------------------------------------------------------
# 3. Cobertura por municipio
# ---------------------------------------------------------------------
def get_coverage_by_municipio(
    table_name: str,
    year_col: str,
    geo_col: str,
    geo_universe_table: str | None = None,
    geo_universe_col: str = "id_mun",
    dept_col: str = "id_dept",
    dept_value: str | None = None,
    query_fn: Callable = query_rows,
) -> pd.DataFrame:
    """
    Número de años con dato por municipio, ordenado descendente.

    Si se pasa geo_universe_table/dept_value, restringe el resultado a
    los municipios de ese departamento (p. ej. Antioquia), en lugar de
    todos los códigos DIVIPOLA presentes en la tabla.
    """
    if geo_universe_table and dept_value is not None:
        sql = f"""
            SELECT t.{geo_col} AS id_geo, COUNT(DISTINCT t.{year_col}) AS n_years
            FROM {table_name} t
            JOIN {geo_universe_table} d ON d.{geo_universe_col} = t.{geo_col}
            WHERE d.{dept_col} = %(dept_value)s
            GROUP BY t.{geo_col}
            ORDER BY n_years DESC
        """
        rows = query_fn(sql, {"dept_value": dept_value})
    else:
        rows = query_fn(
            f"""
            SELECT {geo_col} AS id_geo, COUNT(DISTINCT {year_col}) AS n_years
            FROM {table_name}
            GROUP BY {geo_col}
            ORDER BY n_years DESC
            """
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------
# 4. Clasificación de completitud
# ---------------------------------------------------------------------
def classify_completeness(
    coverage_df: pd.DataFrame,
    n_years_total: int,
    complete_ratio: float = 0.8,
    punctual_years: int = 1,
) -> pd.DataFrame:
    """
    Clasifica cada municipio en:
      - 'serie_completa'  : n_years >= complete_ratio * n_years_total
      - 'dato_puntual'     : n_years <= punctual_years
      - 'serie_parcial'    : el resto
    """
    df = coverage_df.copy()
    complete_cutoff = complete_ratio * n_years_total

    def _classify(n):
        if n >= complete_cutoff:
            return "serie_completa"
        if n <= punctual_years:
            return "dato_puntual"
        return "serie_parcial"

    df["categoria"] = df["n_years"].apply(_classify)
    return df


def summarize_completeness(classified_df: pd.DataFrame) -> pd.DataFrame:
    """Conteo de municipios por categoría de completitud."""
    return (
        classified_df["categoria"]
        .value_counts()
        .rename_axis("categoria")
        .reset_index(name="n_municipios")
    )


# ---------------------------------------------------------------------
# 5. Cobertura año por año
# ---------------------------------------------------------------------
def get_coverage_by_year(
    table_name: str,
    year_col: str,
    geo_col: str,
    geo_universe_table: str | None = None,
    geo_universe_col: str = "id_mun",
    dept_col: str = "id_dept",
    dept_value: str | None = None,
    query_fn: Callable = query_rows,
) -> pd.DataFrame:
    """
    Número de municipios distintos con dato en cada año. Si se pasa
    geo_universe_table/dept_value, restringe a los municipios de ese
    departamento.
    """
    if geo_universe_table and dept_value is not None:
        sql = f"""
            SELECT t.{year_col} AS year, COUNT(DISTINCT t.{geo_col}) AS n_municipios
            FROM {table_name} t
            JOIN {geo_universe_table} d ON d.{geo_universe_col} = t.{geo_col}
            WHERE d.{dept_col} = %(dept_value)s
            GROUP BY t.{year_col}
            ORDER BY t.{year_col}
        """
        rows = query_fn(sql, {"dept_value": dept_value})
    else:
        rows = query_fn(
            f"""
            SELECT {year_col} AS year, COUNT(DISTINCT {geo_col}) AS n_municipios
            FROM {table_name}
            GROUP BY {year_col}
            ORDER BY {year_col}
            """
        )
    return pd.DataFrame(rows)


MIN_YEARS_FOR_DROP_DETECTION = 3


def detect_coverage_drops(
    year_coverage_df: pd.DataFrame,
    drop_ratio: float = 0.6,
    min_years: int = MIN_YEARS_FOR_DROP_DETECTION,
) -> dict:
    """
    Marca años cuya cobertura cae por debajo de drop_ratio * mediana de
    cobertura de los demás años. Útil para detectar caídas abruptas.
    """
    n_years = year_coverage_df["year"].nunique() if not year_coverage_df.empty else 0

    if n_years < min_years:
        return {
            "flagged_years": [],
            "reliable": False,
            "message": (
                f"No se puede establecer un comportamiento normal: solo hay "
                f"{n_years} año(s) registrado(s), por debajo del mínimo de "
                f"{min_years} años requerido para detectar caídas abruptas."
            ),
        }

    median_coverage = year_coverage_df["n_municipios"].median()
    threshold = drop_ratio * median_coverage
    flagged = year_coverage_df[year_coverage_df["n_municipios"] < threshold]
    return {
        "flagged_years": flagged["year"].tolist(),
        "reliable": True,
        "message": "",
    }


# ---------------------------------------------------------------------
# 6. Calidad del dato
# ---------------------------------------------------------------------
def quality_checks(
    table_name: str,
    value_col: str,
    plausible_min: float = 0,
    plausible_max: float | None = None,
    query_fn: Callable = query_rows,
) -> dict:
    """
    Revisa nulos disfrazados, valores negativos y estadísticos
    descriptivos del indicador para verificar que estén dentro de un
    rango lógico.
    """
    stats = query_fn(
        f"""
        SELECT
            COUNT(*)                                   AS total_rows,
            COUNT(*) FILTER (WHERE {value_col} IS NULL) AS n_nulls,
            COUNT(*) FILTER (WHERE {value_col} < 0)      AS n_negativos,
            AVG({value_col})                            AS avg_value,
            MIN({value_col})                            AS min_value,
            MAX({value_col})                            AS max_value,
            STDDEV({value_col})                         AS stddev_value
        FROM {table_name}
        """
    )[0]

    flags = []
    if stats["n_nulls"]:
        flags.append(f"{stats['n_nulls']} filas con {value_col} NULL (dato disfrazado)")
    if stats["n_negativos"]:
        flags.append(f"{stats['n_negativos']} filas con {value_col} negativo")
    if stats["min_value"] is not None and stats["min_value"] < plausible_min:
        flags.append(f"valor mínimo {stats['min_value']} por debajo del rango lógico ({plausible_min})")
    if plausible_max is not None and stats["max_value"] is not None and stats["max_value"] > plausible_max:
        flags.append(f"valor máximo {stats['max_value']} por encima del rango lógico ({plausible_max})")

    stats["flags"] = flags
    return stats


# ---------------------------------------------------------------------
# Resumen final en una sola fila
# ---------------------------------------------------------------------
def build_summary_row(
    table_name: str,
    overview: dict,
    completeness_summary: pd.DataFrame,
    coverage_drops: dict,
    quality: dict,
) -> pd.DataFrame:
    """
    Combina todos los hallazgos en una sola fila apta para reporte/CSV.

    Los nombres de columna van en inglés y en snake_case, igual que las
    columnas de las tablas/vistas del DW (year, id_mun, total_cases...).
    """
    completeness_dict = dict(
        zip(completeness_summary["categoria"], completeness_summary["n_municipios"])
    )

    if coverage_drops["reliable"]:
        drop_years = ", ".join(str(y) for y in coverage_drops["flagged_years"]) or "none"
    else:
        drop_years = "not assessable (" + coverage_drops["message"] + ")"

    row = {
        "table_name": table_name,
        "year_range": f"{overview['min_year']}-{overview['max_year']}",
        "n_years_available": overview["n_years"],
        "total_rows": overview["total_rows"],
        "pct_municipios_with_data": overview["pct_municipios_con_dato"],
        "n_complete_series": completeness_dict.get("serie_completa", 0),
        "n_partial_series": completeness_dict.get("serie_parcial", 0),
        "n_single_point": completeness_dict.get("dato_puntual", 0),
        "years_with_coverage_drop": drop_years,
        "quality_flags": "; ".join(quality["flags"]) or "no findings",
    }
    return pd.DataFrame([row])
