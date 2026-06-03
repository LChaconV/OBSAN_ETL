"""
upload/pipeline_runner.py — Ejecución de pipelines ETL existentes

Invoca los scripts pipeline.py que ya existen en:
    src/etl/{etl_folder}/pipeline.py

No reimplementa la lógica ETL — la delega al script original
usando subprocess para mantener el entorno aislado.
"""
import os
import subprocess
import sys
from pathlib import Path
from typing import Mapping

from upload.pipelines.base import PipelineResult

# Raíz del repositorio unificado (etl/)
PROJECT_ROOT = Path(__file__).resolve().parents[3]
ETL_ROOT = PROJECT_ROOT / "src" / "etl"
PIPELINE_TIMEOUT_SECONDS = int(os.getenv("OBSAN_PIPELINE_TIMEOUT_SECONDS", "900"))

# ─────────────────────────────────────────────────────────────
#  REGISTRO DE PIPELINES
#  clave  = pipeline ID en variables_config.py
#  valor  = nombre de la carpeta dentro de src/etl/
# ─────────────────────────────────────────────────────────────
PIPELINE_REGISTRY: dict = {
    "divipola":  "dw_divipola",
    "departamento": "dw_departamento",
    "mun_pdet": "dw_mun_pdet",
    "subregion_antioquia": "dw_subregiones_antioquia",
    "perfil_antioquia": "ld_perfil_antioquia",
    "resguardo_indigena": "ld_resguardo_indigena",
    "mercado_laboral": "ld_mercado_laboral",
    "mortalidad_desnutricion": "ld_mortalidad_desnutricion",
    "desnutricion_aguda_5": "ld_desnutricion_aguda_5",
    "bajo_peso_nacer": "ld_bajo_peso_nacer",
    "mercados_campesinos": "ld_mercados_campesinos",
    "censo_pecuario": "ld_censo_pecuario",
    "poblacion":"ld_poblacion",
    "agricola": "ld_agricultura",
    # Agrega más siguiendo el patrón:
    # "id_pipeline": "nombre_carpeta_en_etl",
}


def run_pipeline(
    pipeline_id: str,
    file_path: str,
    extra_env: Mapping[str, str] | None = None,
) -> PipelineResult:
    """
    Busca la carpeta ETL correspondiente y ejecuta su pipeline.py,
    enviando la ruta del archivo por variable de entorno.
    """
    etl_folder = PIPELINE_REGISTRY.get(pipeline_id)

    if etl_folder is None:
        return PipelineResult(
            success = False,
            message = f"Pipeline '{pipeline_id}' no está registrado.",
            logs    = [f"Pipelines disponibles: {list(PIPELINE_REGISTRY.keys())}"],
        )

    pipeline_path = ETL_ROOT / etl_folder / "pipeline.py"

    if not pipeline_path.exists():
        return PipelineResult(
            success = False,
            message = f"No se encontró el script del pipeline.",
            logs    = [f"Ruta esperada: {pipeline_path}"],
        )

    logs = [
        f"Pipeline: {pipeline_path}",
        f"Archivo:  {file_path}",
    ]
    env = {**os.environ, "OBSAN_INPUT_FILE": file_path}
    if extra_env:
        env.update(extra_env)

    try:
        command = [sys.executable, "-m", "src.runner", etl_folder]
        logs.append(f"Comando: {' '.join(command)}")

        result = subprocess.run(
            command,
            capture_output = True,
            text           = True,
            timeout        = PIPELINE_TIMEOUT_SECONDS,
            cwd            = str(PROJECT_ROOT),
            env            = env,
        )

        # Agregar stdout y stderr a los logs
        if result.stdout:
            logs += [f"[OUT] {line}" for line in result.stdout.strip().splitlines()]
        if result.stderr:
            logs += [f"[ERR] {line}" for line in result.stderr.strip().splitlines()]

        if result.returncode == 0:
            return PipelineResult(
                success = True,
                message = "Pipeline ejecutado correctamente",
                logs    = logs,
            )
        else:
            return PipelineResult(
                success = False,
                message = f"Pipeline terminó con código de error {result.returncode}",
                logs    = logs,
            )

    except subprocess.TimeoutExpired as e:
        if e.stdout:
            logs += [f"[OUT] {line}" for line in str(e.stdout).strip().splitlines()]
        if e.stderr:
            logs += [f"[ERR] {line}" for line in str(e.stderr).strip().splitlines()]
        return PipelineResult(
            success = False,
            message = f"El pipeline excedió el tiempo máximo de {PIPELINE_TIMEOUT_SECONDS} segundos.",
            logs    = logs,
        )
    except Exception as e:
        return PipelineResult(
            success = False,
            message = f"Error ejecutando el pipeline: {e}",
            logs    = logs,
        )
