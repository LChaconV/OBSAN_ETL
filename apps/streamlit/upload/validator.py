"""
upload/validator.py — Validación de archivos cargados

Valida:
  1. Extensión del archivo
  2. Que el archivo no esté vacío
  3. Columnas requeridas (para Excel/CSV)
  4. Integridad básica del GeoJSON (para archivos espaciales)
"""

from dataclasses import dataclass
import pandas as pd

from upload.backend_logging import log_upload_event, log_upload_exception

SAMPLE_BYTES = 65536

# Cabecera OLE2/CFB: archivos .xlsx cifrados se guardan en este formato en vez de ZIP.
_OLE2_MAGIC = b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"

_ENCRYPTION_KEYWORDS = (
    "encrypt",
    "password",
    "protected",
    "badzipfile",
    "not a zip file",
    "is encrypted",
    "workbook is encrypted",
)


def _detect_encrypted(file_obj, ext: str) -> bool:
    """
    Devuelve True si el archivo parece estar cifrado o protegido con contraseña.
    Para .xlsx compara los primeros bytes con la firma OLE2 (que reemplaza al ZIP).
    """
    if ext in ("xlsx",):
        try:
            header = file_obj.read(8)
            file_obj.seek(0)
            if header == _OLE2_MAGIC:
                return True
        except Exception:
            file_obj.seek(0)
    return False


def _is_encryption_error(exc: Exception) -> bool:
    """detecta si la excepción viene de un archivo cifrado/protegido."""
    msg       = str(exc).lower()
    type_name = type(exc).__name__.lower()
    return (
        any(kw in msg for kw in _ENCRYPTION_KEYWORDS)
        or "badzipfile" in type_name
    )


@dataclass
class ValidationResult:
    valid:   bool
    message: str
    details: list[str] = None

    def __post_init__(self):
        if self.details is None:
            self.details = []


def _encrypted_result() -> ValidationResult:
    return ValidationResult(
        valid   = False,
        message = "El archivo está protegido con contraseña o cifrado.",
        details = [
            "No es posible leer un archivo encriptado o protegido con contraseña.",
            "Por favor, desbloquéelo o guárdelo sin protección antes de subirlo.",
        ],
    )


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
    size_bytes = None
    try:
        size_bytes = _get_file_size(file_obj)
        log_upload_event(
            "INFO",
            "validation_start",
            "Iniciando validación de archivo",
            filename=filename,
            extension=ext,
            size_bytes=size_bytes,
            allowed_types=variable_config.get("allowed_types", []),
            pipeline=variable_config.get("pipeline"),
            label=variable_config.get("label"),
        )

        def finish(result: ValidationResult) -> ValidationResult:
            log_upload_event(
                "INFO" if result.valid else "WARNING",
                "validation_complete" if result.valid else "validation_failed",
                result.message,
                filename=filename,
                extension=ext,
                size_bytes=size_bytes,
                pipeline=variable_config.get("pipeline"),
                label=variable_config.get("label"),
                details=result.details,
            )
            return result

        # 1. Validar extensión
        result = _validate_extension(ext, variable_config["allowed_types"])
        if not result.valid:
            return finish(result)

        # 2. Validar que no esté vacío
        result = _validate_not_empty(file_obj, filename)
        if not result.valid:
            return finish(result)

        # 3. Validar estructura según tipo
        required_cols = variable_config.get("required_columns", [])

        if ext in ("xlsx", "xls", "csv"):
            result = _validate_tabular(file_obj, filename, required_cols)
        elif ext == "geojson":
            result = _validate_geojson(file_obj)
        elif ext == "kml":
            result = _validate_kml(file_obj)
        else:
            result = ValidationResult(valid=True, message="Archivo listo para procesar.")

        return finish(result)
    except Exception as e:
        file_obj.seek(0)
        log_upload_exception(
            "validation_error",
            "Error inesperado validando archivo",
            e,
            filename=filename,
            extension=ext,
            size_bytes=size_bytes,
            pipeline=variable_config.get("pipeline"),
            label=variable_config.get("label"),
        )
        return ValidationResult(
            valid=False,
            message="No se pudo validar el archivo.",
            details=[str(e)],
        )


# ─────────────────────────────────────────────────────────────
#  VALIDACIONES INDIVIDUALES
# ─────────────────────────────────────────────────────────────

def _get_extension(filename: str) -> str:
    return filename.rsplit(".", 1)[-1].lower() if "." in filename else ""


def _get_file_size(file_obj) -> int:
    size = getattr(file_obj, "size", None)
    if isinstance(size, int) and size >= 0:
        return size

    current_pos = file_obj.tell()
    file_obj.seek(0, 2)
    size = file_obj.tell()
    file_obj.seek(current_pos)
    return size


def _validate_extension(ext: str, allowed: list[str]) -> ValidationResult:
    if ext not in allowed:
        return ValidationResult(
            valid   = False,
            message = f"Tipo de archivo no permitido: `.{ext}`",
            details = [f"Formatos aceptados: {', '.join(f'.{t}' for t in allowed)}"],
        )
    return ValidationResult(valid=True, message="Extensión válida")


def _validate_not_empty(file_obj, filename: str) -> ValidationResult:
    size = _get_file_size(file_obj)
    file_obj.seek(0)
    if size <= 0:
        return ValidationResult(
            valid   = False,
            message = "El archivo está vacío.",
        )
    return ValidationResult(valid=True, message="Archivo no vacío")


def _validate_tabular(file_obj, filename: str, required_cols: list) -> ValidationResult:
    """Valida Excel y CSV: lee las primeras filas y verifica columnas."""
    ext = _get_extension(filename)

    if _detect_encrypted(file_obj, ext):
        return _encrypted_result()

    try:
        if ext == "xlsx" or ext == "xls":
            df = pd.read_excel(file_obj, nrows=5)
        else:
            df = pd.read_csv(file_obj, nrows=5)
        file_obj.seek(0)
    except Exception as e:
        if _is_encryption_error(e):
            return _encrypted_result()
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
    """Valida de forma liviana para no bloquear archivos GeoJSON grandes."""
    try:
        sample = file_obj.read(SAMPLE_BYTES)
        file_obj.seek(0)
        text = sample.decode("utf-8", errors="ignore")
    except Exception as e:
        return ValidationResult(
            valid   = False,
            message = "No se pudo leer el GeoJSON.",
            details = [str(e)],
        )

    if not text.lstrip().startswith("{"):
        return ValidationResult(
            valid   = False,
            message = "El archivo no parece ser un GeoJSON válido.",
        )

    details = ["Validación rápida completada. La estructura geoespacial se revisa durante el pipeline."]
    if "FeatureCollection" not in text:
        details.append('No se detectó "FeatureCollection" en los primeros 64 KB.')

    return ValidationResult(
        valid   = True,
        message = "GeoJSON listo para procesar",
        details = details,
    )


def _validate_kml(file_obj) -> ValidationResult:
    """Validación básica de KML sin leer el archivo completo en memoria."""
    try:
        sample = file_obj.read(SAMPLE_BYTES)
        file_obj.seek(0)
        content = sample.decode("utf-8", errors="ignore")
        if "<kml" not in content.lower():
            return ValidationResult(
                valid   = False,
                message = "El archivo no parece ser un KML válido en los primeros 64 KB.",
            )
    except Exception as e:
        return ValidationResult(
            valid   = False,
            message = "No se pudo leer el archivo KML.",
            details = [str(e)],
        )
    return ValidationResult(
        valid=True,
        message="KML listo para procesar",
        details=["Validación rápida completada. La estructura completa se revisa durante el pipeline."],
    )
