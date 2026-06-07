"""
upload/storage.py — Servicio de almacenamiento de archivos

Guarda el archivo en:
    data/bronze/{storage_folder}/{variable}_run_{YYYYMMDD_HHMMSS}.{ext}
"""

from datetime import datetime
from pathlib import Path
from typing import Callable

from upload.backend_logging import log_upload_event, log_upload_exception

# Raíz del repositorio unificado (etl/)
PROJECT_ROOT = Path(__file__).resolve().parents[3]
BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze"
DEFAULT_CHUNK_SIZE = 1024 * 1024


def _get_file_size(file_obj) -> int:
    size = getattr(file_obj, "size", None)
    if isinstance(size, int) and size >= 0:
        return size

    current_pos = file_obj.tell()
    file_obj.seek(0, 2)
    size = file_obj.tell()
    file_obj.seek(current_pos)
    return size


def save_file(
    file_obj,
    filename: str,
    variable_id: str,
    storage_folder: str,
    progress_callback: Callable[[int, int], None] | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> tuple[bool, str, str]:
    try:
        ext       = filename.rsplit(".", 1)[-1].lower()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_name  = f"{variable_id}_run_{timestamp}.{ext}"

        dest_folder = BRONZE_ROOT / storage_folder
        dest_folder.mkdir(parents=True, exist_ok=True)

        dest_path = dest_folder / new_name
        total_size = _get_file_size(file_obj)
        written = 0
        log_upload_event(
            "INFO",
            "storage_start",
            "Iniciando guardado de archivo",
            variable_id=variable_id,
            filename=filename,
            storage_folder=storage_folder,
            dest_path=str(dest_path),
            size_bytes=total_size,
        )
        file_obj.seek(0)
        if progress_callback:
            progress_callback(written, total_size)

        with dest_path.open("wb") as f:
            while True:
                chunk = file_obj.read(chunk_size)
                if not chunk:
                    break

                f.write(chunk)
                written += len(chunk)
                if progress_callback:
                    progress_callback(written, total_size)

        file_obj.seek(0)
        log_upload_event(
            "INFO",
            "storage_complete",
            "Archivo guardado correctamente",
            variable_id=variable_id,
            filename=filename,
            storage_folder=storage_folder,
            dest_path=str(dest_path),
            size_bytes=total_size,
            written_bytes=written,
        )

        return True, f"Archivo guardado en `data/bronze/{storage_folder}/{new_name}`", str(dest_path)

    except Exception as e:
        log_upload_exception(
            "storage_error",
            "Error al guardar archivo",
            e,
            variable_id=variable_id,
            filename=filename,
            storage_folder=storage_folder,
        )
        return False, f"Error al guardar el archivo: {e}", ""


def list_uploaded_files(variable_id: str, storage_folder: str) -> list[dict]:
    folder = BRONZE_ROOT / storage_folder
    if not folder.exists():
        return []

    files = []
    for f in sorted(folder.iterdir(), reverse=True):
        if f.stem.startswith(variable_id):
            files.append({
                "name":     f.name,
                "path":     str(f),
                "size_kb":  round(f.stat().st_size / 1024, 1),
                "modified": datetime.fromtimestamp(
                    f.stat().st_mtime
                ).strftime("%Y-%m-%d %H:%M"),
            })
    return files
