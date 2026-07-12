"""
upload/pipeline_runner.py — Ejecución de pipelines ETL existentes

Invoca los scripts pipeline.py que ya existen en:
    src/etl/{etl_folder}/pipeline.py

No reimplementa la lógica ETL — la delega al script original
usando subprocess para mantener el entorno aislado.
"""
import os
import queue
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Mapping

# Raíz del repositorio unificado (etl/)
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from upload.backend_logging import log_upload_event, log_upload_exception
from upload.pipelines.base import PipelineResult
from src.etl.utils.execution_lock import ETLExecutionLockBusy, acquire_etl_execution_lock

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
    "api_beneficiarios_iraca": "api_beneficiarios_iraca",
    "api_edu_escolar": "api_edu_escolar",
    "api_edu_superior": "api_edu_superior",
    "api_erradicacion_cultivos_coca": "api_erradicacion_cultivos_coca",
    "api_familias_accion": "api_familias_accion",
    "api_indice_riesgo_irca": "api_indice_riesgo_irca",
    "api_minerales": "api_minerales",
    "api_produc_gas": "api_produc_gas",
    "api_produc_petroleo": "api_produc_petroleo",
    "api_regalias": "api_regalias",
    "api_victimas": "api_victimas",
    "url_terraclimate": "url_terraclimate",
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
    "ipm_departamentos": "ld_ipm_departamentos",
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


def _log_pipeline_event(event: PipelineEvent, pipeline_id: str, file_path: str | None) -> None:
    if event.kind == "log":
        log_upload_event(
            "INFO",
            "pipeline_output",
            event.message,
            pipeline_id=pipeline_id,
            file_path=file_path,
            progress=event.progress,
        )
    elif event.kind in {"progress", "heartbeat"}:
        log_upload_event(
            "INFO",
            f"pipeline_{event.kind}",
            event.message,
            pipeline_id=pipeline_id,
            file_path=file_path,
            progress=event.progress,
        )


def _drain_output_queue(
    output_queue: queue.Queue[str],
    logs: list[str],
    progress: float,
) -> tuple[float, list[PipelineEvent]]:
    events: list[PipelineEvent] = []
    while True:
        try:
            raw_line = output_queue.get_nowait()
        except queue.Empty:
            break

        raw_line = raw_line.rstrip()
        line = f"[OUT] {raw_line}"
        logs.append(line)
        progress, label = _progress_from_line(raw_line, progress)
        events.append(PipelineEvent(kind="log", message=line, progress=progress))
        if label:
            events.append(PipelineEvent(kind="progress", message=label, progress=progress))

    return progress, events


def stream_pipeline(
    pipeline_id: str,
    file_path: str | None = None,
    extra_env: Mapping[str, str] | None = None,
    timeout_seconds: int | None = None,
    lock_owner: str = "streamlit",
) -> Iterator[PipelineEvent]:
    """Ejecuta un pipeline y emite eventos de log/progreso en tiempo real."""
    timeout_seconds = timeout_seconds or PIPELINE_TIMEOUT_SECONDS
    etl_folder, pipeline_path, error_result = _resolve_pipeline(pipeline_id)
    if error_result:
        log_upload_event(
            "ERROR",
            "pipeline_resolve_error",
            error_result.message,
            pipeline_id=pipeline_id,
            file_path=file_path,
            details=error_result.logs,
        )
        yield PipelineEvent(
            kind="result",
            message=error_result.message,
            progress=1.0,
            result=error_result,
        )
        return

    assert etl_folder is not None
    assert pipeline_path is not None

    command = ["uv", "run", "-m", "src.runner", etl_folder]
    logs = [
        f"Pipeline: {pipeline_path}",
        f"Comando: {' '.join(command)}",
    ]
    if file_path:
        logs.insert(1, f"Archivo:  {file_path}")

    env = {
        **os.environ,
        "ETL_PIPELINE_NAME": etl_folder,
        "PYTHONUNBUFFERED": "1",
        "PYTHONIOENCODING": "utf-8",
    }
    env.pop("ETL_SCHEDULES", None)
    if file_path:
        env["OBSAN_INPUT_FILE"] = file_path
    if extra_env:
        env.update(extra_env)

    log_upload_event(
        "INFO",
        "pipeline_start",
        "Iniciando pipeline ETL",
        pipeline_id=pipeline_id,
        etl_folder=etl_folder,
        pipeline_path=str(pipeline_path),
        file_path=file_path,
        command=command,
        extra_env_keys=sorted((extra_env or {}).keys()),
        timeout_seconds=timeout_seconds,
        lock_owner=lock_owner,
    )

    for log_line in logs:
        yield PipelineEvent(kind="meta", message=log_line, progress=0.05)

    start = time.monotonic()
    last_heartbeat = start
    progress = 0.15
    yield PipelineEvent(kind="progress", message="Pipeline iniciado", progress=progress)

    process: subprocess.Popen[str] | None = None
    output_queue: queue.Queue[str] = queue.Queue()
    lock_context = None

    try:
        try:
            lock_context = acquire_etl_execution_lock(
                pipeline_name=etl_folder,
                owner=lock_owner,
                blocking=False,
            )
            lock_context.__enter__()
        except ETLExecutionLockBusy as busy:
            running = busy.metadata
            running_label = running.get("pipeline", "otro ETL")
            owner = running.get("owner", "desconocido")
            started_at = running.get("started_at", "sin hora registrada")
            message = (
                "Ya hay una ejecución ETL en curso: "
                f"{running_label} ({owner}, inicio {started_at})."
            )
            result = PipelineResult(success=False, message=message, logs=logs)
            log_upload_event(
                "WARNING",
                "pipeline_busy",
                message,
                pipeline_id=pipeline_id,
                etl_folder=etl_folder,
                file_path=file_path,
                running=running,
            )
            yield PipelineEvent(kind="result", message=message, progress=1.0, result=result)
            return

        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=str(PROJECT_ROOT),
            env=env,
        )
        log_upload_event(
            "INFO",
            "pipeline_process_started",
            "Subprocess del pipeline iniciado",
            pipeline_id=pipeline_id,
            etl_folder=etl_folder,
            file_path=file_path,
            pid=process.pid,
        )

        if process.stdout is None:
            raise RuntimeError("No se pudo capturar stdout del pipeline.")

        def read_stdout() -> None:
            assert process is not None
            assert process.stdout is not None
            for line in process.stdout:
                output_queue.put(line)

        reader_thread = threading.Thread(target=read_stdout, daemon=True)
        reader_thread.start()

        while process.poll() is None or not output_queue.empty():
            elapsed = time.monotonic() - start
            if elapsed > timeout_seconds:
                process.kill()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass

                reader_thread.join(timeout=1)
                progress, queued_events = _drain_output_queue(output_queue, logs, progress)
                for event in queued_events:
                    _log_pipeline_event(event, pipeline_id, file_path)
                    yield event

                message = f"El pipeline excedió el tiempo máximo de {timeout_seconds} segundos."
                result = PipelineResult(success=False, message=message, logs=logs)
                log_upload_event(
                    "ERROR",
                    "pipeline_timeout",
                    message,
                    pipeline_id=pipeline_id,
                    etl_folder=etl_folder,
                    file_path=file_path,
                    timeout_seconds=timeout_seconds,
                    logs_tail=logs[-40:],
                )
                yield PipelineEvent(kind="result", message=message, progress=1.0, result=result)
                return

            progress, queued_events = _drain_output_queue(output_queue, logs, progress)
            for event in queued_events:
                _log_pipeline_event(event, pipeline_id, file_path)
                yield event

            now = time.monotonic()
            if now - last_heartbeat >= PIPELINE_HEARTBEAT_SECONDS:
                elapsed = now - start
                remaining = max(0, timeout_seconds - int(elapsed))
                estimated_progress = min(0.9, max(progress, elapsed / timeout_seconds * 0.9))
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
                log_upload_event(
                    "INFO",
                    "pipeline_heartbeat",
                    "Pipeline en ejecución",
                    pipeline_id=pipeline_id,
                    etl_folder=etl_folder,
                    file_path=file_path,
                    elapsed_seconds=int(elapsed),
                    remaining_seconds=remaining,
                    progress=progress,
                )
                last_heartbeat = now

            if not queued_events:
                time.sleep(0.05)

        reader_thread.join(timeout=1)
        progress, queued_events = _drain_output_queue(output_queue, logs, progress)
        for event in queued_events:
            _log_pipeline_event(event, pipeline_id, file_path)
            yield event

        return_code = process.wait()
        if return_code == 0:
            result = PipelineResult(
                success=True,
                message="Pipeline ejecutado correctamente",
                logs=logs,
            )
            log_upload_event(
                "INFO",
                "pipeline_complete",
                result.message,
                pipeline_id=pipeline_id,
                etl_folder=etl_folder,
                file_path=file_path,
                return_code=return_code,
                logs_tail=logs[-40:],
            )
            yield PipelineEvent(kind="result", message=result.message, progress=1.0, result=result)
        else:
            result = PipelineResult(
                success=False,
                message=f"Pipeline terminó con código de error {return_code}",
                logs=logs,
            )
            log_upload_event(
                "ERROR",
                "pipeline_failed",
                result.message,
                pipeline_id=pipeline_id,
                etl_folder=etl_folder,
                file_path=file_path,
                return_code=return_code,
                logs_tail=logs[-80:],
            )
            yield PipelineEvent(kind="result", message=result.message, progress=1.0, result=result)

    except Exception as e:
        if process and process.poll() is None:
            process.kill()
        log_upload_exception(
            "pipeline_exception",
            "Error ejecutando pipeline",
            e,
            pipeline_id=pipeline_id,
            etl_folder=etl_folder,
            file_path=file_path,
            logs_tail=logs[-80:],
        )
        result = PipelineResult(
            success=False,
            message=f"Error ejecutando el pipeline: {e}",
            logs=logs,
        )
        yield PipelineEvent(kind="result", message=result.message, progress=1.0, result=result)
    finally:
        if lock_context is not None:
            lock_context.__exit__(None, None, None)


def run_pipeline(
    pipeline_id: str,
    file_path: str | None = None,
    extra_env: Mapping[str, str] | None = None,
    timeout_seconds: int | None = None,
    lock_owner: str = "streamlit",
) -> PipelineResult:
    """
    Busca la carpeta ETL correspondiente y ejecuta su pipeline.py,
    enviando la ruta del archivo por variable de entorno.
    """
    final_result: PipelineResult | None = None
    for event in stream_pipeline(
        pipeline_id,
        file_path,
        extra_env,
        timeout_seconds=timeout_seconds,
        lock_owner=lock_owner,
    ):
        if event.kind == "result":
            final_result = event.result

    return final_result or PipelineResult(
        success=False,
        message="El pipeline terminó sin devolver resultado.",
        logs=[],
    )
