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
