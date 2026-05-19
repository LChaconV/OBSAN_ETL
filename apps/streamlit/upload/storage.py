"""
upload/storage.py — Servicio de almacenamiento de archivos

Guarda el archivo en:
    data/bronze/{storage_folder}/{variable}_run_{YYYYMMDD_HHMMSS}.{ext}
"""

from datetime import datetime
from pathlib import Path

# Raíz del repositorio unificado (etl/)
PROJECT_ROOT = Path(__file__).resolve().parents[3]
BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze"


def save_file(
    file_obj,
    filename: str,
    variable_id: str,
    storage_folder: str,
) -> tuple[bool, str, str]:
    try:
        ext       = filename.rsplit(".", 1)[-1].lower()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_name  = f"{variable_id}_run_{timestamp}.{ext}"

        dest_folder = BRONZE_ROOT / storage_folder
        dest_folder.mkdir(parents=True, exist_ok=True)

        dest_path = dest_folder / new_name
        with dest_path.open("wb") as f:
            f.write(file_obj.read())
        file_obj.seek(0)

        return True, f"Archivo guardado en `data/bronze/{storage_folder}/{new_name}`", str(dest_path)

    except Exception as e:
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
