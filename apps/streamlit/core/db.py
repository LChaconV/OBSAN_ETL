"""
core/db.py — Conexión a PostgreSQL / PostGIS
"""

import os
from pathlib import Path
import psycopg2
import psycopg2.extras
import streamlit as st
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(dotenv_path=PROJECT_ROOT / ".env", override=False)

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME",     "postgres"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", "admin123"),
    "connect_timeout": 10,
}


def _get_connection():
    return psycopg2.connect(**DB_CONFIG)


def query_geojson(sql: str, params: tuple = None) -> list[dict]:
    try:
        conn = _get_connection()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [row["feature"] for row in rows]
    except Exception:
        return []


def query_rows(sql: str, params: tuple = None) -> list[dict]:
    try:
        conn = _get_connection()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def get_subregion_at_point(lat: float, lng: float, year: int) -> dict | None:
    """
    Dado un punto (lat, lng) y un año, retorna todos los indicadores
    de la subregión que contiene ese punto.
    Usa ST_Contains para la consulta espacial.
    """
    sql = """
        SELECT
            s.id_subregion,
            s.name_subregion,
            p.year,
            p.pct_u18_food_security,
            p.pct_u18_food_insecurity,
            p.pct_u18_food_insecurity_mild,
            p.pct_u18_food_insecurity_moderate,
            p.pct_u18_food_insecurity_severe,
            p.pct_u5_wasting_severe,
            p.pct_u5_wasting_moderate,
            p.pct_u5_wasting_risk,
            p.pct_u5_wasting_normal,
            p.pct_u5_underweight,
            p.pct_u5_underweight_risk,
            p.pct_u5_underweight_normal,
            p.pct_u5_stunting,
            p.pct_u5_stunting_risk,
            p.pct_u5_stunting_normal,
            p.pct_u5_overweight_risk,
            p.pct_u5_overweight,
            p.pct_u5_obesity,
            p.pct_5_18_stunting,
            p.pct_5_18_stunting_risk,
            p.pct_5_18_stunting_normal,
            p.pct_5_18_thinness_risk,
            p.pct_5_18_bmi_normal,
            p.pct_5_18_overweight,
            p.pct_5_18_obesity
        FROM subregion s
        JOIN perfil_antioquia p ON s.id_subregion = p.id_subregion
        WHERE ST_Contains(
            s.geometry,
            ST_SetSRID(ST_Point(%s, %s), 4326)
        )
        AND p.year = %s
        LIMIT 1
    """
    rows = query_rows(sql, (lng, lat, year))
    return rows[0] if rows else None

def get_muni_salud(id_mun: str, year: int) -> dict:
    """Todos los indicadores de salud de un municipio."""
    params = {"id_mun": id_mun, "year": year}
    result: dict = {}

    rows = query_rows("""
        SELECT SUM(total_cases_per_capita) AS desnutricion_aguda
        FROM v_acute_malnutrition_5_pc
        WHERE id_mun = %(id_mun)s AND year = %(year)s
    """, params)
    result["desnutricion_aguda"] = rows[0]["desnutricion_aguda"] if rows else None

    rows = query_rows("""
        SELECT SUM(total_cases_per_capita) AS mortalidad_malnutricion
        FROM v_mortality_malnutrition_pc
        WHERE id_mun = %(id_mun)s AND year = %(year)s
    """, params)
    result["mortalidad_malnutricion"] = rows[0]["mortalidad_malnutricion"] if rows else None

    rows = query_rows("""
        SELECT SUM(total_cases_per_capita) AS bajo_peso_nacer
        FROM v_low_birth_weight_pc
        WHERE id_mun = %(id_mun)s AND year = %(year)s
    """, params)
    result["bajo_peso_nacer"] = rows[0]["bajo_peso_nacer"] if rows else None

    return result


def get_muni_socioeconomico(id_mun: str, year: int) -> dict:
    """Indicadores socioeconómicos de un municipio."""
    params = {"id_mun": id_mun, "year": year}
    result: dict = {}

    # Cada indicador se consulta de forma independiente para que tablas
    # ausentes no silencien los demás indicadores.
    rows = query_rows("""
        SELECT AVG(mp_idx_val) AS pobreza_monetaria
        FROM mp_sex_head_hh m
        JOIN dim_divipola d ON d.id_dept = m.id_dept
        WHERE d.id_mun = %(id_mun)s AND m.year = %(year)s
    """, params)
    result["pobreza_monetaria"] = (rows[0]["pobreza_monetaria"] if rows else None)

    rows = query_rows("""
        SELECT estimacion AS pobreza_monetaria_mun
        FROM pobreza_monetaria_municipal
        WHERE id_mun = %(id_mun)s AND year = %(year)s
        LIMIT 1
    """, params)
    result["pobreza_monetaria_mun"] = (rows[0]["pobreza_monetaria_mun"] if rows else None)

    rows = query_rows("""
        SELECT SUM(total) AS poblacion_empleada
        FROM employed_population
        WHERE id_mun = %(id_mun)s AND year = %(year)s
    """, params)
    result["poblacion_empleada"] = (rows[0]["poblacion_empleada"] if rows else None)

    rows = query_rows("""
        SELECT SUM(total_cases_per_capita) AS cobertura_escolar
        FROM v_school_education_pc
        WHERE id_mun = %(id_mun)s AND year = %(year)s
    """, params)
    result["cobertura_escolar"] = (rows[0]["cobertura_escolar"] if rows else None)

    rows = query_rows("""
        SELECT SUM(total_cases_per_capita) AS cobertura_superior
        FROM v_higher_education_pc
        WHERE id_mun = %(id_mun)s AND year = %(year)s
    """, params)
    result["cobertura_superior"] = (rows[0]["cobertura_superior"] if rows else None)

    edu = query_rows("""
        SELECT prof_technician, technologist, university,
               specialization, master, doctorate
        FROM higher_education
        WHERE id_mun = %(id_mun)s AND year = %(year)s
        LIMIT 1
    """, params)
    result["educacion_superior"] = edu[0] if edu else {}

    return result


def get_muni_ambiente(id_mun: str, year: int) -> dict:
    """Indicadores de ambiente y territorio de un municipio."""
    # Calidad del agua
    agua = query_rows("""
        SELECT AVG(irca_value) AS irca_value
        FROM water_quality_index
        WHERE id_mun = %(id_mun)s AND year = %(year)s
    """, {"id_mun": id_mun, "year": year})

    # Petróleo y gas — tablas espaciales sin id_mun: usan el año global
    oil = query_rows("""
        SELECT SUM(produc_bls) AS produccion
        FROM oil_production
        WHERE year = %(year)s
        AND ST_Within(
            ST_SetSRID(geometry, 4326),
            (SELECT geometry FROM dim_divipola WHERE id_mun = %(id_mun)s)
        )
    """, {"id_mun": id_mun, "year": year})

    gas = query_rows("""
        SELECT SUM(production_value) AS produccion
        FROM gas_production
        WHERE year = %(year)s
        AND ST_Within(
            ST_SetSRID(geometry, 4326),
            (SELECT geometry FROM dim_divipola WHERE id_mun = %(id_mun)s)
        )
    """, {"id_mun": id_mun, "year": year})

    royalties = query_rows("""
        SELECT SUM(royalties_cop) AS total
        FROM royalties
        WHERE year = %(year)s
        AND ST_Within(
            ST_SetSRID(geometry, 4326),
            (SELECT geometry FROM dim_divipola WHERE id_mun = %(id_mun)s)
        )
    """, {"id_mun": id_mun, "year": year})

    # Regalías minerales
    mineral = query_rows("""
        SELECT mineral_resource, SUM(royalties_cop) AS total
        FROM mineral_royalties
        WHERE id_mun = %(id_mun)s AND year = %(year)s
        GROUP BY mineral_resource
        ORDER BY total DESC
    """, {"id_mun": id_mun, "year": year})

    # Oro de aluvión
    oro = query_rows("""
        SELECT
            SUM(illicit_hectares) AS illicit_hectares,
            SUM(total_evidence)   AS total_evidence
        FROM alluvial_gold_mining
        WHERE id_mun = %(id_mun)s AND year = %(year)s
    """, {"id_mun": id_mun, "year": year})

    # Cultivos ilícitos
    crops = query_rows("""
        SELECT id_illicit_crop, SUM(quantity) AS total
        FROM erad_illicit_crops
        WHERE id_mun = %(id_mun)s AND year = %(year)s
        GROUP BY id_illicit_crop
    """, {"id_mun": id_mun, "year": year})

    # Clima
    clima = query_rows("""
        SELECT variable, value, annual_aggregation
        FROM terraclimate
        WHERE id_mun = %(id_mun)s AND year = %(year)s
        ORDER BY variable
    """, {"id_mun": id_mun, "year": year})

    return {
        "irca":           agua[0] if agua else {},
        "petroleo":       oil[0] if oil else {},
        "gas":            gas[0] if gas else {},
        "regalias":       royalties[0] if royalties else {},
        "minerales":      mineral,
        "oro_aluvion":    oro[0] if oro else {},
        "cultivos":       crops,
        "clima":          clima,
    }


def get_muni_agropecuario(id_mun: str, year: int) -> dict:
    """Censo pecuario del municipio."""
    livestock = query_rows("""
        SELECT type, SUM(total_animals) AS total_animals,
               SUM(total_farms) AS total_farms
        FROM census_livestock
        WHERE id_mun = %(id_mun)s AND year = %(year)s
        GROUP BY type
        ORDER BY total_animals DESC
    """, {"id_mun": id_mun, "year": year})

    markets = query_rows("""
        SELECT name
        FROM farmer_market fm
        WHERE ST_Within(
            fm.geometry,
            (SELECT geometry FROM dim_divipola WHERE id_mun = %(id_mun)s)
        )
    """, {"id_mun": id_mun, "year": year})
    agro = query_rows("""
        SELECT type,
               ROUND(AVG(yield)::numeric, 2)        AS avg_yield,
               ROUND(AVG(production)::numeric, 2)   AS avg_production,
               ROUND(AVG(area_harvested)::numeric, 2) AS avg_area
        FROM agricultural_production
        WHERE id_mun = %(id_mun)s
        AND year = %(year)s
        GROUP BY type
        ORDER BY avg_production DESC
    """, {"id_mun": id_mun, "year": year})
    return {
        "pecuario": livestock,
        "mercados": markets,
        "agricola": agro,
    }


def get_muni_conflicto(id_mun: str, year: int) -> dict:
    """Víctimas e IRACA del municipio."""
    victimas = query_rows("""
        SELECT e.event_name, SUM(v.victim_count) AS total
        FROM victim_unit v
        JOIN dim_victim_event e ON v.id_victim_event = e.id_victim_event
        WHERE v.id_mun = %(id_mun)s AND v.year = %(year)s
        GROUP BY e.event_name
        ORDER BY total DESC
    """, {"id_mun": id_mun, "year": year})

    return {
        "victimas": victimas,
    }


def get_muni_nbi(id_mun: str, year: int) -> dict:
    """Todos los indicadores NBI de un municipio para el año indicado."""
    rows = query_rows("""
        SELECT indicador, valor
        FROM nbi_municipal
        WHERE id_mun = %s AND year = %s
        ORDER BY indicador
    """, (id_mun, year))
    if not rows:
        rows = query_rows("""
            SELECT indicador, valor
            FROM nbi_municipal
            WHERE id_mun = %s
            ORDER BY year DESC, indicador
            LIMIT 7
        """, (id_mun,))
    return {r["indicador"]: r["valor"] for r in rows} if rows else {}


CATEGORY_QUERY_MAP = {
    "salud":          get_muni_salud,
    "socioeconomico": get_muni_socioeconomico,
    "ambiente":       get_muni_ambiente,
    "agropecuario":   get_muni_agropecuario,
    "conflicto":      get_muni_conflicto,
    "nbi_municipal":  get_muni_nbi,
}
def test_connection() -> tuple[bool, str]:
    try:
        conn = _get_connection()
        cur  = conn.cursor()
        cur.execute("SELECT PostGIS_Version();")
        version = cur.fetchone()[0]
        cur.close()
        conn.close()
        return True, f"PostGIS {version}"
    except Exception as e:
        return False, str(e)
