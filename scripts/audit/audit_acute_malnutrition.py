"""
scripts/audit/audit_acute_malnutrition.py

Auditoría de v_acute_malnutrition_5_pc (desnutrición aguda en menores
de 5 años) antes de usarla en el índice compuesto de vulnerabilidad.

Uso:
    # Contra la conexión por defecto (apps/streamlit/core/db.py, vía .env)
    uv run python scripts/audit/audit_acute_malnutrition.py

    # Contra la BD de pruebas (credenciales en .env.audit, no versionado)
    uv run python scripts/audit/audit_acute_malnutrition.py --source test
"""

import argparse
import sys

import pandas as pd

from data_audit import (
    print_schema,
    get_coverage_overview,
    get_coverage_by_municipio,
    classify_completeness,
    summarize_completeness,
    get_coverage_by_year,
    detect_coverage_drops,
    quality_checks,
    build_summary_row,
    make_query_fn,
    query_rows as default_query_fn,
    TEST_DB_ENV_FILE,
)

TABLE_NAME = "v_acute_malnutrition_5_pc"

# Columnas identificadas en el paso 1 (confirmar contra el esquema impreso):
#   - año:       year
#   - municipio: id_mun (código DIVIPOLA)
#   - valor:     total_cases_per_capita (tasa por 100.000 hab.)
YEAR_COL = "year"
GEO_COL = "id_mun"
VALUE_COL = "total_cases_per_capita"

DEPT_VALUE_ANTIOQUIA = "05"

# Rango lógico para una tasa per cápita (x 100.000 hab.): no puede ser
# negativa; un valor por encima de 100.000 implicaría más casos que
# habitantes y es señal de error de cálculo o de población mal cruzada.
PLAUSIBLE_MIN = 0
PLAUSIBLE_MAX = 100_000


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        choices=["default", "test"],
        default="default",
        help=(
            "default: usa la conexión de apps/streamlit/core/db.py (.env). "
            "test: usa la BD de pruebas con credenciales en .env.audit (no versionado)."
        ),
    )
    return parser.parse_args()


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    pd.set_option("display.max_rows", 200)
    pd.set_option("display.width", 120)

    args = parse_args()
    if args.source == "test":
        print(f"Usando BD de pruebas (credenciales en {TEST_DB_ENV_FILE})")
        query_fn = make_query_fn(TEST_DB_ENV_FILE)
    else:
        print("Usando la conexión por defecto (apps/streamlit/core/db.py)")
        query_fn = default_query_fn

    # 1. Identificar columnas reales
    schema = print_schema(TABLE_NAME, query_fn=query_fn)
    print(
        f"\nColumnas usadas para el análisis -> "
        f"año: '{YEAR_COL}', municipio: '{GEO_COL}', valor: '{VALUE_COL}'"
    )
    if not schema.empty:
        present = set(schema["column_name"])
        for col in (YEAR_COL, GEO_COL, VALUE_COL):
            if col not in present:
                print(f"  ADVERTENCIA: la columna '{col}' no existe en {TABLE_NAME}")

    # 2. Cobertura general
    overview = get_coverage_overview(
        TABLE_NAME, YEAR_COL, GEO_COL, dept_value=DEPT_VALUE_ANTIOQUIA, query_fn=query_fn
    )
    print(f"\n=== Cobertura general de {TABLE_NAME} ===")
    for k, v in overview.items():
        print(f"  {k}: {v}")

    # 3. Cobertura por municipio (restringido al universo de Antioquia)
    coverage_by_muni = get_coverage_by_municipio(
        TABLE_NAME,
        YEAR_COL,
        GEO_COL,
        geo_universe_table="dim_divipola",
        dept_value=DEPT_VALUE_ANTIOQUIA,
        query_fn=query_fn,
    )
    print(
        f"\n=== Cobertura por municipio de Antioquia "
        f"({len(coverage_by_muni)} municipios con dato) ==="
    )
    print(coverage_by_muni.describe().to_string())

    # 4. Clasificación de completitud
    classified = classify_completeness(coverage_by_muni, n_years_total=overview["n_years"])
    completeness_summary = summarize_completeness(classified)
    print("\n=== Clasificación de completitud ===")
    print(completeness_summary.to_string(index=False))

    # 5. Cobertura año por año (restringido al universo de Antioquia)
    coverage_by_year = get_coverage_by_year(
        TABLE_NAME,
        YEAR_COL,
        GEO_COL,
        geo_universe_table="dim_divipola",
        dept_value=DEPT_VALUE_ANTIOQUIA,
        query_fn=query_fn,
    )
    print("\n=== Cobertura año por año (municipios de Antioquia) ===")
    print(coverage_by_year.to_string(index=False))

    coverage_drops = detect_coverage_drops(coverage_by_year)
    if not coverage_drops["reliable"]:
        print(f"\n  {coverage_drops['message']}")
    elif coverage_drops["flagged_years"]:
        print(f"\n  Caída abrupta de cobertura detectada en: {coverage_drops['flagged_years']}")
    else:
        print("\n  Sin caídas abruptas de cobertura entre años.")

    # 6. Calidad del dato
    quality = quality_checks(
        TABLE_NAME,
        VALUE_COL,
        plausible_min=PLAUSIBLE_MIN,
        plausible_max=PLAUSIBLE_MAX,
        query_fn=query_fn,
    )
    print(f"\n=== Calidad del dato ({VALUE_COL}) ===")
    for k, v in quality.items():
        if k != "flags":
            print(f"  {k}: {v}")
    print(f"  banderas: {quality['flags'] or 'sin hallazgos'}")

    # Entregable: una sola fila resumen
    summary = build_summary_row(
        TABLE_NAME, overview, completeness_summary, coverage_drops, quality
    )
    print("\n=== RESUMEN (entregable) ===")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
