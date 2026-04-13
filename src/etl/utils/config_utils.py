from pathlib import Path
from typing import Dict, Any

import yaml
import pandas as pd
import logging
from typing import Optional

def load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo: {path}")

    with path.open("r", encoding="utf-8") as file:
        data = yaml.safe_load(file) or {}

    if not isinstance(data, dict):
        raise ValueError(f"El archivo {path} no tiene una estructura válida.")

    return data

def save_yaml(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as file:
        yaml.safe_dump(data, file, sort_keys=False, allow_unicode=True)



def load_source_config(key: str, path: Path) -> Dict[str, Any]:
    config = load_yaml(path)

    if key not in config:
        raise KeyError(
            f"No existe la configuración '{key}' en {path}. "
            f"Claves disponibles: {list(config.keys())}"
        )

    return config[key]

def load_state(key: str, path_state: Path) -> Dict[str, Any]:
    default_state = {
        "last_incremental_value": None,
        "incremental_column": None,
        "last_run_at": None,
        "last_row_count": 0,
        "extraction_mode": None,
    }

    if not path_state.exists():
        return default_state

    state = load_yaml(path_state)
    return state.get(key,
                     { "last_incremental_value": None, 
                      "incremental_column": None, 
                      "last_run_at": None, 
                      "last_row_count": 0, 
                      "extraction_mode": None, 
                      },
                 )
    


def update_state(
    *,
    key: str,
    incremental_value: Optional[str],
    incremental_column: Optional[str],
    row_count: int,
    extraction_mode: str,
    path_state: Path ,
) -> None:
    full_state = {}

    if path_state.exists():
        full_state = load_yaml(path_state)

    full_state[key] = {
        "last_incremental_value": incremental_value,
        "incremental_column": incremental_column,
        "last_run_at": pd.Timestamp.utcnow().isoformat(),
        "last_row_count": int(row_count),
        "extraction_mode": extraction_mode,
    }

    save_yaml(path_state, full_state)
    logging.info(f"State actualizado correctamente para '{key}'.")

def get_incremental_max_value(df: pd.DataFrame, config: dict) -> str | None:
    incremental_column = config.get("incremental", {}).get("column")

    if df.empty or not incremental_column:
        return None

    col_name = incremental_column.lower()

    if col_name not in df.columns:
        logging.warning(
            "La columna incremental '%s' no está en los datos descargados.",
            incremental_column,
        )
        return None

    parsed = pd.to_datetime(df[col_name], errors="coerce", utc=True)
    max_value = parsed.dropna().max()

    if pd.isna(max_value):
        return None

    return pd.Timestamp(max_value).isoformat()