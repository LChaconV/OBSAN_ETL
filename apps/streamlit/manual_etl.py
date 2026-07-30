"""
manual_etl.py — Página para disparar pipelines programados desde Streamlit.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.etl.utils.execution_lock import read_active_lock_metadata
from upload.pipeline_runner import PIPELINE_REGISTRY, stream_pipeline
from upload.pipelines.base import PipelineResult


MANUAL_TIMEOUT_SECONDS = int(os.getenv("OBSAN_MANUAL_ETL_TIMEOUT_SECONDS", "21600"))


@dataclass(frozen=True)
class ScheduledPipeline:
    id: str
    label: str
    schedule: str
    estimate: str
    description: str = ""


SCHEDULED_PIPELINES = [
    ScheduledPipeline("api_edu_superior", "Educación superior", "00:10", "1 min",
        "Actualiza los datos de matrícula en educación superior (universidades, institutos técnicos y tecnológicos) por municipio."),
    ScheduledPipeline("api_erradicacion_cultivos_coca", "Erradicación cultivos de coca", "00:15", "1 min",
        "Actualiza el registro de erradicación de cultivos de coca por municipio."),
    ScheduledPipeline("api_indice_riesgo_irca", "Índice de riesgo IRCA", "00:20", "1 min",
        "Actualiza el Índice de Riesgo de la Calidad del Agua para Consumo Humano (IRCA) por municipio."),
    ScheduledPipeline("api_minerales", "Minerales", "00:25", "1 min",
        "Actualiza el registro de regalías por explotación de minerales pagadas a cada municipio."),
    ScheduledPipeline("api_oro_aluvion", "Oro de aluvión", "00:30", "1 min",
        "Actualiza los datos de minería de oro de aluvión — hectáreas ilícitas y evidencias registradas — por municipio."),
    ScheduledPipeline("api_produc_gas", "Producción de gas", "00:30", "1 min",
        "Actualiza la producción de gas natural por municipio."),
    ScheduledPipeline("api_produc_petroleo", "Producción de petróleo", "00:35", "1 min",
        "Actualiza la producción de petróleo por municipio."),
    ScheduledPipeline("api_regalias", "Regalías", "00:40", "1 min",
        "Actualiza el registro general de regalías pagadas por municipio."),
    ScheduledPipeline("url_terraclimate", "Terraclimate", "00:45", "1 h",
        "Actualiza las variables climáticas (precipitación, temperatura, humedad, entre otras) por municipio. Puede tardar hasta una hora."),
    ScheduledPipeline("api_edu_escolar", "Educación escolar", "01:45", "2 h",
        "Actualiza los datos de cobertura en educación básica y media por municipio. Puede tardar hasta dos horas."),
    ScheduledPipeline("api_victimas", "Víctimas", "03:30", "3 h",
        "Actualiza el registro de víctimas del conflicto armado por municipio, año, tipo de hecho y sexo. Puede tardar hasta tres horas."),
    ScheduledPipeline("api_familias_accion", "Familias en Acción", "06:30", "1 h",
        "Actualiza el número de familias beneficiarias del programa Familias en Acción por municipio."),
]



def _pipeline_label(pipeline: ScheduledPipeline) -> str:
    return f"{pipeline.label} · {pipeline.id}"


def _active_scheduled_pipelines() -> list[ScheduledPipeline]:
    return [
        pipeline
        for pipeline in SCHEDULED_PIPELINES
        if pipeline.id in PIPELINE_REGISTRY
    ]


def _looks_like_error(line: str) -> bool:
    text = line.lower()
    needles = (
        "error",
        "exception",
        "traceback",
        "critical",
        "fallo",
        "falló",
        "timeout",
        "timed out",
        "killed",
        "exit status",
    )
    return any(needle in text for needle in needles)


def _render_execution_lock() -> None:
    metadata = read_active_lock_metadata()
    if not metadata:
        return

    pipeline_id = metadata.get("pipeline", "")
    friendly = next(
        (p.label for p in SCHEDULED_PIPELINES if p.id == pipeline_id),
        pipeline_id or "desconocida",
    )
    st.warning(
        f"Hay una actualización de datos en curso ({friendly}). "
        "Por favor espera a que termine antes de iniciar otra."
    )


def _render_pipeline_summary(pipeline: ScheduledPipeline) -> None:
    if pipeline.description:
        st.info(pipeline.description, icon="ℹ️")



def _run_selected_pipeline(pipeline: ScheduledPipeline) -> PipelineResult | None:
    status = st.status(f"Ejecutando `{pipeline.id}`...", expanded=True)
    progress_bar = st.progress(0, text="Preparando ejecución...")
    live_log = st.empty()
    logs: list[str] = []
    result: PipelineResult | None = None

    for event in stream_pipeline(
        pipeline.id,
        extra_env={"ETL_MANUAL_RUN": "1"},
        timeout_seconds=MANUAL_TIMEOUT_SECONDS,
        lock_owner="streamlit_manual",
    ):
        if event.kind in {"meta", "log", "heartbeat"}:
            logs.append(event.message)
            live_log.code("\n".join(logs[-180:]), language="text")

        if event.kind == "log" and _looks_like_error(event.message):
            status.update(label=event.message, state="error", expanded=True)

        if event.progress is not None:
            progress_value = max(0, min(100, int(event.progress * 100)))
            progress_text = "Procesando..." if event.kind == "log" else event.message
            progress_bar.progress(progress_value, text=progress_text)

        if event.kind in {"progress", "heartbeat", "result"}:
            status.update(label=event.message)

        if event.kind == "result":
            result = event.result

    if result is None:
        status.update(label="La ejecución terminó sin devolver resultado.", state="error")
        return None

    status.update(
        label=result.message,
        state="complete" if result.success else "error",
        expanded=not result.success,
    )

    if result.logs:
        with st.expander("Logs de la ejecución", expanded=not result.success):
            st.code("\n".join(result.logs[-250:]), language="text")

    return result


def render_manual_etl_page() -> None:
    st.markdown("## Ejecuciones ETL")
    st.markdown(
        "Esta página permite actualizar manualmente los datos del observatorio. "
        "Cada fuente se actualiza de forma automática, "
        "pero aquí puedes forzar una actualización inmediata cuando lo necesites — "
        "por ejemplo, después de corregir un error o cuando los datos automáticos no se hayan cargado correctamente. "
        "Selecciona la fuente que quieres actualizar, confirma la acción y espera a que el proceso termine."
    )
    st.divider()
    _render_execution_lock()

    pipelines = _active_scheduled_pipelines()
    if not pipelines:
        st.error("No hay pipelines programados registrados para ejecución manual.")
        return

    selected = st.selectbox(
        "Pipeline",
        options=pipelines,
        format_func=_pipeline_label,
    )
    _render_pipeline_summary(selected)

    confirm = st.checkbox(
        f"Confirmo la ejecución manual de `{selected.id}`",
        key=f"manual_confirm_{selected.id}",
    )
    run_clicked = st.button(
        "Ejecutar ETL ahora",
        type="primary",
        disabled=not confirm,
        width="stretch",
    )

    if not run_clicked:
        return

    result = _run_selected_pipeline(selected)
    if result is None:
        st.error("La ejecución terminó sin devolver resultado.")
    elif result.success:
        st.success(result.message)
    else:
        st.error(result.message)
