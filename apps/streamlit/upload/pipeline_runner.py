"""
upload/pipeline_runner.py — Ejecución de pipelines ETL existentes

Invoca los scripts pipeline.py que ya existen en:
    src/etl/{etl_folder}/pipeline.py

No reimplementa la lógica ETL — la delega al script original
usando subprocess para mantener el entorno aislado.
"""
import os
import selectors
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Mapping

from upload.pipelines.base import PipelineResult

# Raíz del repositorio unificado (etl/)
PROJECT_ROOT = Path(__file__).resolve().parents[3]
ETL_ROOT = PROJECT_ROOT / "src" / "etl"
PIPELINE_TIMEOUT_SECONDS = int(os.getenv("OBSAN_PIPELINE_TIMEOUT_SECONDS", "900"))
PIPELINE_HEARTBEAT_SECONDS = int(os.getenv("OBSAN_PIPELINE_HEARTBEAT_SECONDS", "10"))


@dataclass
class PipelineEvent:
    kind: str
    message: str
    progress: float | None = None
    result: PipelineResult | None = None

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


def _resolve_pipeline(pipeline_id: str) -> tuple[str | None, Path | None, PipelineResult | None]:
    etl_folder = PIPELINE_REGISTRY.get(pipeline_id)

    if etl_folder is None:
        return None, None, PipelineResult(
            success=False,
            message=f"Pipeline '{pipeline_id}' no está registrado.",
            logs=[f"Pipelines disponibles: {list(PIPELINE_REGISTRY.keys())}"],
        )

    pipeline_path = ETL_ROOT / etl_folder / "pipeline.py"

    if not pipeline_path.exists():
        return etl_folder, pipeline_path, PipelineResult(
            success=False,
            message="No se encontró el script del pipeline.",
            logs=[f"Ruta esperada: {pipeline_path}"],
        )

    return etl_folder, pipeline_path, None


def _progress_from_line(line: str, current: float) -> tuple[float, str | None]:
    normalized = line.lower()
    steps = [
        ("iniciando transformación", 0.25, "Transformando archivo"),
        ("filas leídas", 0.35, "Archivo leído"),
        ("transformación completada", 0.55, "Transformación completada"),
        ("iniciando carga", 0.65, "Cargando en base de datos"),
        ("insertando", 0.78, "Insertando registros"),
        ("upsert", 0.82, "Sincronizando registros"),
        ("carga finalizada", 0.92, "Carga finalizada"),
        ("carga completada", 0.95, "Carga completada"),
        ("finalizada correctamente", 0.95, "Pipeline finalizando"),
    ]

    for needle, progress, label in steps:
        if needle in normalized:
            return max(current, progress), label

    return current, None


def _format_elapsed(seconds: float) -> str:
    minutes, secs = divmod(int(seconds), 60)
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def stream_pipeline(
    pipeline_id: str,
    file_path: str,
    extra_env: Mapping[str, str] | None = None,
) -> Iterator[PipelineEvent]:
    """Ejecuta un pipeline y emite eventos de log/progreso en tiempo real."""
    etl_folder, pipeline_path, error_result = _resolve_pipeline(pipeline_id)
    if error_result:
        yield PipelineEvent(
            kind="result",
            message=error_result.message,
            progress=1.0,
            result=error_result,
        )
        return

    assert etl_folder is not None
    assert pipeline_path is not None

    command = [sys.executable, "-u", "-m", "src.runner", etl_folder]
    logs = [
        f"Pipeline: {pipeline_path}",
        f"Archivo:  {file_path}",
        f"Comando: {' '.join(command)}",
    ]
    env = {**os.environ, "OBSAN_INPUT_FILE": file_path, "PYTHONUNBUFFERED": "1"}
    if extra_env:
        env.update(extra_env)

    for log_line in logs:
        yield PipelineEvent(kind="meta", message=log_line, progress=0.05)

    start = time.monotonic()
    last_heartbeat = start
    progress = 0.15
    yield PipelineEvent(kind="progress", message="Pipeline iniciado", progress=progress)

    process: subprocess.Popen[str] | None = None
    selector = selectors.DefaultSelector()

    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=str(PROJECT_ROOT),
            env=env,
        )

        if process.stdout is None:
            raise RuntimeError("No se pudo capturar stdout del pipeline.")

        selector.register(process.stdout, selectors.EVENT_READ)

        while process.poll() is None:
            elapsed = time.monotonic() - start
            if elapsed > PIPELINE_TIMEOUT_SECONDS:
                process.kill()
                remaining, _ = process.communicate(timeout=5)
                if remaining:
                    for raw_line in remaining.splitlines():
                        line = f"[OUT] {raw_line}"
                        logs.append(line)
                        yield PipelineEvent(kind="log", message=line, progress=progress)

                message = f"El pipeline excedió el tiempo máximo de {PIPELINE_TIMEOUT_SECONDS} segundos."
                result = PipelineResult(success=False, message=message, logs=logs)
                yield PipelineEvent(kind="result", message=message, progress=1.0, result=result)
                return

            events = selector.select(timeout=0.25)
            for key, _ in events:
                raw_line = key.fileobj.readline()
                if not raw_line:
                    continue
                raw_line = raw_line.rstrip()
                line = f"[OUT] {raw_line}"
                logs.append(line)
                progress, label = _progress_from_line(raw_line, progress)
                yield PipelineEvent(kind="log", message=line, progress=progress)
                if label:
                    yield PipelineEvent(kind="progress", message=label, progress=progress)

            now = time.monotonic()
            if now - last_heartbeat >= PIPELINE_HEARTBEAT_SECONDS:
                elapsed = now - start
                remaining = max(0, PIPELINE_TIMEOUT_SECONDS - int(elapsed))
                estimated_progress = min(0.9, max(progress, elapsed / PIPELINE_TIMEOUT_SECONDS * 0.9))
                progress = estimated_progress
                yield PipelineEvent(
                    kind="heartbeat",
                    message=(
                        "Pipeline en ejecución "
                        f"({ _format_elapsed(elapsed) } transcurridos, "
                        f"límite restante aprox. { _format_elapsed(remaining) })."
                    ),
                    progress=progress,
                )
                last_heartbeat = now

        if process.stdout is not None:
            for raw_line in process.stdout.read().splitlines():
                line = f"[OUT] {raw_line.rstrip()}"
                logs.append(line)
                progress, label = _progress_from_line(raw_line, progress)
                yield PipelineEvent(kind="log", message=line, progress=progress)
                if label:
                    yield PipelineEvent(kind="progress", message=label, progress=progress)

        return_code = process.wait()
        if return_code == 0:
            result = PipelineResult(
                success=True,
                message="Pipeline ejecutado correctamente",
                logs=logs,
            )
            yield PipelineEvent(kind="result", message=result.message, progress=1.0, result=result)
        else:
            result = PipelineResult(
                success=False,
                message=f"Pipeline terminó con código de error {return_code}",
                logs=logs,
            )
            yield PipelineEvent(kind="result", message=result.message, progress=1.0, result=result)

    except Exception as e:
        if process and process.poll() is None:
            process.kill()
        result = PipelineResult(
            success=False,
            message=f"Error ejecutando el pipeline: {e}",
            logs=logs,
        )
        yield PipelineEvent(kind="result", message=result.message, progress=1.0, result=result)
    finally:
        selector.close()


def run_pipeline(
    pipeline_id: str,
    file_path: str,
    extra_env: Mapping[str, str] | None = None,
) -> PipelineResult:
    """
    Busca la carpeta ETL correspondiente y ejecuta su pipeline.py,
    enviando la ruta del archivo por variable de entorno.
    """
    final_result: PipelineResult | None = None
    for event in stream_pipeline(pipeline_id, file_path, extra_env):
        if event.kind == "result":
            final_result = event.result

    return final_result or PipelineResult(
        success=False,
        message="El pipeline terminó sin devolver resultado.",
        logs=[],
    )
