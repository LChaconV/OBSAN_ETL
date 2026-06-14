
import os
import sys
import time
import requests
import logging

SERVER_DOWN_CODES = {502, 503, 504}

def build_headers() -> dict:
    headers = {
        "Accept": "application/json",
        "User-Agent": "observatorio-san-etl/1.0",
    }

    app_token = os.getenv("SOCRATA_APP_TOKEN")
    if app_token:
        headers["X-App-Token"] = app_token

    return headers


def fetch_api_page(
    base_url: str,
    params: dict,
    timeout: int,
    max_retries: int = 5,
) -> list[dict]:
    headers = build_headers()
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(
                base_url,
                params=params,
                timeout=timeout,
                headers=headers,
            )
            response.raise_for_status()

            data = response.json()

            if not isinstance(data, list):
                raise ValueError("La respuesta de la API no es una lista de registros.")

            return data

        except requests.exceptions.HTTPError as e:
            last_error = e
            status_code = e.response.status_code if e.response is not None else None

            if status_code in {429, 500, 502, 503, 504} and attempt < max_retries:
                wait_seconds = 2 ** (attempt - 1)
                logging.warning(
                    "Error HTTP %s. Reintentando en %s segundos. Intento %s/%s",
                    status_code,
                    wait_seconds,
                    attempt,
                    max_retries,
                )
                time.sleep(wait_seconds)
                continue

            if status_code in SERVER_DOWN_CODES:
                logging.error(
                    "El servidor de la fuente de datos está caído o sobrecargado (HTTP %s). "
                    "No se pudo completar la extracción después de %s intentos. "
                    "Intenta ejecutar el pipeline más tarde; el checkpoint está guardado y no perderás progreso.",
                    status_code,
                    max_retries,
                )
                sys.exit(1)

            raise

        except Exception as e:
            last_error = e

            if attempt < max_retries:
                wait_seconds = 2 ** (attempt - 1)
                logging.warning(
                    "Error de conexión. Reintentando en %s segundos. Intento %s/%s",
                    wait_seconds,
                    attempt,
                    max_retries,
                )
                time.sleep(wait_seconds)
                continue

            raise

    raise RuntimeError(f"No se pudo consultar la API después de varios intentos: {last_error}")
