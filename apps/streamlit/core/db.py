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
    rows = query_rows("""
        SELECT
            (SELECT SUM(total_cases) FROM acute_malnutrition_5
             WHERE id_mun = %(id_mun)s AND year = %(year)s) AS desnutricion_aguda,

            (SELECT SUM(total_cases) FROM mortality_malnutrition
             WHERE id_mun = %(id_mun)s AND year = %(year)s) AS mortalidad_malnutricion,

            (SELECT SUM(total_cases) FROM low_birth_weight
             WHERE id_mun = %(id_mun)s AND year = %(year)s) AS bajo_peso_nacer
    """, {"id_mun": id_mun, "year": year})
    return rows[0] if rows else {}


def get_muni_socioeconomico(id_mun: str, year: int) -> dict:
    """Indicadores socioeconómicos de un municipio."""
    rows = query_rows("""
        SELECT
            (SELECT AVG(mp_idx_val) FROM mp_sex_head_hh m
             JOIN dim_divipola d ON d.id_dept = m.id_dept
             WHERE d.id_mun = %(id_mun)s AND m.year = %(year)s
            ) AS pobreza_monetaria,

            (SELECT SUM(total) FROM employed_population
             WHERE id_mun = %(id_mun)s AND year = %(year)s
            ) AS poblacion_empleada,

            (SELECT SUM(total) FROM school_education
             WHERE id_mun = %(id_mun)s AND year = %(year)s
            ) AS cobertura_escolar
    """, {"id_mun": id_mun, "year": year})

    # Educación superior por nivel
    edu = query_rows("""
        SELECT prof_technician, technologist, university,
               specialization, master, doctorate
        FROM higher_education
        WHERE id_mun = %(id_mun)s AND year = %(year)s
        LIMIT 1
    """, {"id_mun": id_mun, "year": year})

    result = rows[0] if rows else {}
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

    # Petróleo y gas
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

    # Regalías
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

    return {
        "pecuario": livestock,
        "mercados": markets,
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

    iraca = query_rows("""
        SELECT SUM(beneficiaries_count) AS total,
               status, type
        FROM iraca_beneficiaries
        WHERE id_mun = %(id_mun)s AND year = %(year)s
        GROUP BY status, type
    """, {"id_mun": id_mun, "year": year})

    return {
        "victimas": victimas,
        "iraca":    iraca,
    }


CATEGORY_QUERY_MAP = {
    "salud":          get_muni_salud,
    "socioeconomico": get_muni_socioeconomico,
    "ambiente":       get_muni_ambiente,
    "agropecuario":   get_muni_agropecuario,
    "conflicto":      get_muni_conflicto,
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
