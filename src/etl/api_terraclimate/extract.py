from __future__ import annotations

import logging
import sys
import time
from pathlib import Path
import pandas as pd
import requests
import yaml
from pathlib import Path

from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.config_utils import load_yaml


PROJECT_ROOT = Path(__file__).resolve().parents[3]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

CONFIG_PATH = PROJECT_ROOT / "config" / "sources.yaml"
LOG_DIR = PROJECT_ROOT / "logs"
# seguimiento al yaml
STATE_PATH = PROJECT_ROOT / "state" / "state.yaml"


def load_state() -> dict:
    if not STATE_PATH.exists():
        return {}

    state = load_yaml(STATE_PATH)
    return state.get("terraclimate", {})

def save_yaml(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as file:
        yaml.safe_dump(
            data,
            file,
            sort_keys=False,
            allow_unicode=True
        )

def save_state(terraclimate_state: dict) -> None:
    full_state = {}

    if STATE_PATH.exists():
        full_state = load_yaml(STATE_PATH)

    full_state["terraclimate"] = terraclimate_state

    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    save_yaml(STATE_PATH, full_state)

def is_downloaded(state: dict, variable: str, year: int) -> bool:
    return (
        state
        .get("downloaded_files", {})
        .get(variable, {})
        .get(str(year), {})
        .get("status") == "downloaded"
    )

def update_download_state(
    state: dict,
    variable: str,
    year: int,
    output_path: Path,
) -> dict:
    state.setdefault("variables", {})
    state["variables"].setdefault(variable, {})

    state["variables"][variable] = {
        "last_incremental_value": int(year),
        "last_file": output_path.name,
        "last_path": str(output_path.relative_to(PROJECT_ROOT)),
        "last_downloaded_at": pd.Timestamp.utcnow().isoformat(),
        "status": "downloaded",
    }

    return state

#------------------
def get_next_year_to_download(state: dict, variable: str, start_year: int) -> int:
    last_value = (
        state
        .get("variables", {})
        .get(variable, {})
        .get("last_incremental_value")
    )

    if last_value is None:
        return start_year

    return int(last_value) + 1

def load_source_config() -> dict:
    config = load_yaml(CONFIG_PATH)

    if "terraclimate" not in config:
        raise KeyError("No existe la configuración 'terraclimate' en sources.yaml")

    return config["terraclimate"]


def build_download_url(config: dict, variable: str, year: int) -> str:
    return config["base_url_pattern"].format(variable=variable, year=year)


def download_file(url: str, output_path: Path, timeout: int, max_retries: int) -> bool:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, max_retries + 1):
        try:
            logging.info("Descargando: %s", url)

            response = requests.get(url, stream=True, timeout=timeout)

            if response.status_code == 404:
                logging.info("Archivo no disponible en servidor: %s", url)
                return False

            response.raise_for_status()

            temp_path = output_path.with_suffix(output_path.suffix + ".part")

            with temp_path.open("wb") as file:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        file.write(chunk)

            temp_path.rename(output_path)

            logging.info("Archivo guardado en: %s", output_path)

            return True  

        except Exception as e:
            if attempt < max_retries:
                wait_seconds = 2 ** (attempt - 1)
                logging.warning(
                    "Error descargando archivo. Reintento %s/%s en %s segundos. Error: %s",
                    attempt,
                    max_retries,
                    wait_seconds,
                    e,
                )
                time.sleep(wait_seconds)
            else:
                raise


def run() -> None:
    setup_logging(LOG_DIR, "terraclimate_extract.log")
    logging.info("Iniciando extracción incremental de TerraClimate")

    config = load_source_config()
    state = load_state()

    variables = config["variables"]
    start_year = config["years"]["start_year"]
    max_year_attempts = config["years"].get("max_year_attempts", 5)

    bronze_dir = PROJECT_ROOT / config["storage"]["bronze_dir"]
    timeout = config["request"]["timeout_seconds"]
    max_retries = config["request"]["max_retries"]

    downloaded_count = 0

    for variable in variables:
        next_year = get_next_year_to_download(
            state=state,
            variable=variable,
            start_year=start_year,
        )

        logging.info(
            "Variable %s iniciará búsqueda desde el año %s",
            variable,
            next_year,
        )

        for year in range(next_year, next_year + max_year_attempts):
            url = build_download_url(config, variable, year)
            output_path = bronze_dir / variable / f"TerraClimate_{variable}_{year}.nc"

            downloaded = download_file(
                url=url,
                output_path=output_path,
                timeout=timeout,
                max_retries=max_retries,
            )

            if not downloaded:
                logging.info(
                    "No hay archivo disponible para variable=%s año=%s. "
                    "Se detiene esta variable.",
                    variable,
                    year,
                )
                break

            state = update_download_state(
                state=state,
                variable=variable,
                year=year,
                output_path=output_path,
            )

            save_state(state)
            downloaded_count += 1

            logging.info(
                "State actualizado: variable=%s last_incremental_value=%s",
                variable,
                year,
            )

    if downloaded_count == 0:
        logging.info("No se encontraron nuevos archivos TerraClimate para descargar.")
    else:
        logging.info("Archivos nuevos descargados: %s", downloaded_count)

    logging.info("Extracción TerraClimate finalizada.")

if __name__ == "__main__":
    run()