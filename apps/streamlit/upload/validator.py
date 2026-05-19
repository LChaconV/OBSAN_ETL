"""
upload/validator.py — Validación de archivos cargados

Valida:
  1. Extensión del archivo
  2. Que el archivo no esté vacío
  3. Columnas requeridas (para Excel/CSV)
  4. Integridad básica del GeoJSON (para archivos espaciales)
"""

from dataclasses import dataclass
from typing import BinaryIO
import pandas as pd
import json


@dataclass
class ValidationResult:
    valid:   bool
    message: str
    details: list[str] = None

    def __post_init__(self):
        if self.details is None:
            self.details = []


def validate_file(
    file_obj,
    filename: str,
    variable_config: dict,
) -> ValidationResult:
    """
    Punto de entrada principal. Ejecuta todas las validaciones
    en orden y retorna el primer error encontrado.
    """
    ext = _get_extension(filename)

    # 1. Validar extensión
    result = _validate_extension(ext, variable_config["allowed_types"])
    if not result.valid:
        return result

    # 2. Validar que no esté vacío
    result = _validate_not_empty(file_obj, filename)
    if not result.valid:
        return result

    # 3. Validar estructura según tipo
    required_cols = variable_config.get("required_columns", [])

    if ext in ("xlsx", "csv"):
        result = _validate_tabular(file_obj, filename, required_cols)
    elif ext == "geojson":
        result = _validate_geojson(file_obj)
    elif ext == "kml":
        result = _validate_kml(file_obj)

    return result


# ─────────────────────────────────────────────────────────────
#  VALIDACIONES INDIVIDUALES
# ─────────────────────────────────────────────────────────────

def _get_extension(filename: str) -> str:
    return filename.rsplit(".", 1)[-1].lower() if "." in filename else ""


def _validate_extension(ext: str, allowed: list[str]) -> ValidationResult:
    if ext not in allowed:
        return ValidationResult(
            valid   = False,
            message = f"Tipo de archivo no permitido: `.{ext}`",
            details = [f"Formatos aceptados: {', '.join(f'.{t}' for t in allowed)}"],
        )
    return ValidationResult(valid=True, message="Extensión válida")


def _validate_not_empty(file_obj, filename: str) -> ValidationResult:
    content = file_obj.read()
    file_obj.seek(0)
    if not content or len(content) == 0:
        return ValidationResult(
            valid   = False,
            message = "El archivo está vacío.",
        )
    return ValidationResult(valid=True, message="Archivo no vacío")


def _validate_tabular(file_obj, filename: str, required_cols: list) -> ValidationResult:
    """Valida Excel y CSV: lee las primeras filas y verifica columnas."""
    ext = _get_extension(filename)
    try:
        if ext == "xlsx" or ext == "xls":
            df = pd.read_excel(file_obj, nrows=5)
        else:
            df = pd.read_csv(file_obj, nrows=5)
        file_obj.seek(0)
    except Exception as e:
        return ValidationResult(
            valid   = False,
            message = "No se pudo leer el archivo.",
            details = [str(e)],
        )

    if df.empty:
        return ValidationResult(
            valid   = False,
            message = "El archivo no contiene filas de datos.",
        )

    if required_cols:
        cols_lower    = [c.lower() for c in df.columns]
        missing       = [c for c in required_cols if c.lower() not in cols_lower]
        if missing:
            return ValidationResult(
                valid   = False,
                message = f"Faltan {len(missing)} columna(s) requerida(s).",
                details = [f"Columnas faltantes: {', '.join(missing)}",
                           f"Columnas encontradas: {', '.join(df.columns.tolist())}"],
            )

    return ValidationResult(
        valid   = True,
        message = f"Archivo válido — {len(df.columns)} columnas encontradas",
        details = [f"Columnas: {', '.join(df.columns.tolist())}"],
    )


def _validate_geojson(file_obj) -> ValidationResult:
    """Valida estructura básica de un GeoJSON."""
    try:
        data = json.load(file_obj)
        file_obj.seek(0)
    except json.JSONDecodeError as e:
        return ValidationResult(
            valid   = False,
            message = "El archivo no es un JSON válido.",
            details = [str(e)],
        )

    if data.get("type") != "FeatureCollection":
        return ValidationResult(
            valid   = False,
            message = 'El GeoJSON debe ser de tipo "FeatureCollection".',
        )

    features = data.get("features", [])
    if not features:
        return ValidationResult(
            valid   = False,
            message = "El GeoJSON no contiene features.",
        )

    return ValidationResult(
        valid   = True,
        message = f"GeoJSON válido — {len(features)} features",
    )


def _validate_kml(file_obj) -> ValidationResult:
    """Validación básica de KML: verifica que sea XML con etiqueta kml."""
    try:
        content = file_obj.read().decode("utf-8", errors="ignore")
        file_obj.seek(0)
        if "<kml" not in content.lower():
            return ValidationResult(
                valid   = False,
                message = "El archivo no parece ser un KML válido.",
            )
    except Exception as e:
        return ValidationResult(
            valid   = False,
            message = "No se pudo leer el archivo KML.",
            details = [str(e)],
        )
    return ValidationResult(valid=True, message="KML válido")
