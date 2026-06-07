"""
upload/ui.py — Interfaz Streamlit del módulo de carga de archivos
"""

import os
import tomllib
import traceback
from datetime import datetime
from pathlib import Path

import streamlit as st
from upload.variables_config import UPLOAD_VARIABLES
from upload.validator import validate_file
from upload.storage import save_file, list_uploaded_files
from upload.pipeline_runner import PIPELINE_TIMEOUT_SECONDS, stream_pipeline


PROJECT_ROOT = Path(__file__).resolve().parents[3]
STREAMLIT_CONFIG_PATH = PROJECT_ROOT / ".streamlit" / "config.toml"
UPLOAD_LOG_KEY = "upload_diagnostic_events"
UPLOAD_LAST_SEEN_KEY = "upload_last_seen_signature"
UPLOAD_LAST_VALIDATED_KEY = "upload_last_validated_signature"
UPLOAD_LAST_SIZE_WARNING_KEY = "upload_last_size_warning_signature"
MAX_UPLOAD_LOG_EVENTS = 80


def _format_bytes(size: int | None) -> str:
    if size is None:
        return "tamaño desconocido"

    value = float(size)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} {unit}"
        value /= 1024

    return f"{value:.1f} GB"


def _format_seconds(seconds: int) -> str:
    minutes, secs = divmod(seconds, 60)
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def _read_max_upload_size_mb() -> int | None:
    try:
        with STREAMLIT_CONFIG_PATH.open("rb") as config_file:
            config = tomllib.load(config_file)
    except (OSError, tomllib.TOMLDecodeError):
        return None

    value = config.get("server", {}).get("maxUploadSize")
    return value if isinstance(value, int) else None


def _get_uploaded_size(uploaded) -> int | None:
    size = getattr(uploaded, "size", None)
    return size if isinstance(size, int) and size >= 0 else None


def _file_signature(variable_id: str, uploaded) -> str:
    return f"{variable_id}:{uploaded.name}:{_get_uploaded_size(uploaded)}"


def _get_upload_events() -> list[dict]:
    if UPLOAD_LOG_KEY not in st.session_state:
        st.session_state[UPLOAD_LOG_KEY] = []
    return st.session_state[UPLOAD_LOG_KEY]


def _add_upload_event(
    level: str,
    stage: str,
    message: str,
    details: list[str] | None = None,
) -> None:
    events = _get_upload_events()
    events.append({
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "level": level.upper(),
        "stage": stage,
        "message": message,
        "details": details or [],
    })
    del events[:-MAX_UPLOAD_LOG_EVENTS]


def _register_uploaded_file(variable_id: str, uploaded) -> None:
    signature = _file_signature(variable_id, uploaded)
    if st.session_state.get(UPLOAD_LAST_SEEN_KEY) == signature:
        return

    st.session_state[UPLOAD_LAST_SEEN_KEY] = signature
    _add_upload_event(
        "INFO",
        "Recepción",
        "Archivo recibido por Streamlit",
        [
            f"Variable: {variable_id}",
            f"Archivo: {uploaded.name}",
            f"Tamaño: {_format_bytes(_get_uploaded_size(uploaded))}",
        ],
    )


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
        "denied",
        "refused",
    )
    return any(needle in text for needle in needles)


def _render_upload_diagnostic_panel() -> None:
    events = _get_upload_events()
    has_errors = any(event["level"] == "ERROR" for event in events)
    max_upload_size_mb = _read_max_upload_size_mb()
    max_upload_label = (
        _format_bytes(max_upload_size_mb * 1024 * 1024)
        if max_upload_size_mb is not None
        else "no disponible"
    )

    with st.expander("🧾 Diagnóstico de carga", expanded=has_errors):
        st.caption(
            " · ".join([
                f"Límite de subida: {max_upload_label}",
                f"Timeout del pipeline: {_format_seconds(PIPELINE_TIMEOUT_SECONDS)}",
            ])
        )
        st.caption(
            "Si aparece Axios Error y no hay evento de recepción, la transferencia falló antes de llegar al backend."
        )
        _render_browser_error_log()

        if st.button("Limpiar diagnóstico", key="clear_upload_diagnostic", width="stretch"):
            st.session_state[UPLOAD_LOG_KEY] = []
            return

        if not events:
            st.info("Sin eventos registrados en esta sesión.")
            return

        for event in reversed(events[-25:]):
            icon = {
                "ERROR": "🔴",
                "WARNING": "🟠",
                "SUCCESS": "🟢",
                "INFO": "🔵",
            }.get(event["level"], "⚪")
            st.markdown(
                f"{icon} `{event['time']}` **{event['stage']}** — {event['message']}"
            )
            if event["details"]:
                st.code("\n".join(event["details"][-40:]), language="text")


def _render_browser_error_log() -> None:
    st.html(
        """
        <div id="obsan-client-log" style="
            border:1px solid #ddd;
            border-radius:6px;
            font:12px/1.4 system-ui, sans-serif;
            max-height:150px;
            overflow:auto;
            padding:8px;
            white-space:pre-wrap;
        ">Log del navegador: sin errores de red detectados.</div>
        <script>
        (() => {
            const root = document.getElementById("obsan-client-log");
            const loggerVersion = 2;

            if (
                window.__obsanUploadLoggerPatched &&
                window.__obsanUploadLoggerVersion !== loggerVersion &&
                !window.sessionStorage.getItem("obsanUploadLoggerReloaded")
            ) {
                window.sessionStorage.setItem("obsanUploadLoggerReloaded", "1");
                window.location.reload();
                return;
            }

            function shouldIgnore(message) {
                const text = String(message || "").toLowerCase();
                return (
                    text.includes("global scope is shutting down") ||
                    text.includes("aborterror: the operation was aborted") ||
                    text.includes("the user aborted a request")
                );
            }

            function render() {
                const entries = window.__obsanUploadClientErrors || [];
                if (!entries.length) {
                    root.textContent = "Log del navegador: sin errores de red detectados.";
                    return;
                }
                root.textContent = entries
                    .slice(-12)
                    .reverse()
                    .map((entry) => {
                        const suffix = entry.count > 1 ? ` (x${entry.count})` : "";
                        return `${entry.time} [${entry.level}] ${entry.message}${suffix}`;
                    })
                    .join("\\n");
            }

            function push(level, message) {
                if (shouldIgnore(message)) {
                    return;
                }

                window.__obsanUploadClientErrors =
                    window.__obsanUploadClientErrors || [];

                const entries = window.__obsanUploadClientErrors;
                const lastEntry = entries[entries.length - 1];
                if (lastEntry && lastEntry.level === level && lastEntry.message === message) {
                    lastEntry.count = (lastEntry.count || 1) + 1;
                    lastEntry.time = new Date().toLocaleTimeString();
                    render();
                    return;
                }

                entries.push({
                    level,
                    message,
                    count: 1,
                    time: new Date().toLocaleTimeString(),
                });
                window.__obsanUploadClientErrors = entries.slice(-30);
                render();
            }

            try {
                if (window.__obsanUploadLoggerVersion !== loggerVersion) {
                    window.__obsanUploadLoggerPatched = true;
                    window.__obsanUploadLoggerVersion = loggerVersion;

                    const originalFetch = window.fetch;
                    if (originalFetch) {
                        window.fetch = async function(...args) {
                            try {
                                const response = await originalFetch.apply(this, args);
                                if (!response.ok) {
                                    push("ERROR", `fetch ${response.status} ${response.url}`);
                                }
                                return response;
                            } catch (error) {
                                push("ERROR", `fetch ${error.name || "Error"}: ${error.message || error}`);
                                throw error;
                            }
                        };
                    }

                    const XHR = window.XMLHttpRequest;
                    if (XHR) {
                        const originalOpen = XHR.prototype.open;
                        const originalSend = XHR.prototype.send;

                        XHR.prototype.open = function(method, url) {
                            this.__obsanMethod = method;
                            this.__obsanUrl = String(url);
                            return originalOpen.apply(this, arguments);
                        };

                        XHR.prototype.send = function() {
                            this.addEventListener("error", () => {
                                push("ERROR", `XHR error ${this.__obsanMethod || ""} ${this.__obsanUrl || this.responseURL || ""}`);
                            });
                            this.addEventListener("timeout", () => {
                                push("ERROR", `XHR timeout ${this.__obsanMethod || ""} ${this.__obsanUrl || this.responseURL || ""}`);
                            });
                            this.addEventListener("loadend", () => {
                                if (this.status >= 400) {
                                    push("ERROR", `XHR ${this.status} ${this.statusText || ""} ${this.__obsanUrl || this.responseURL || ""}`);
                                }
                            });
                            return originalSend.apply(this, arguments);
                        };
                    }

                    window.addEventListener("unhandledrejection", (event) => {
                        const reason = event.reason || {};
                        push("ERROR", `Promise ${reason.message || reason}`);
                    });
                    window.addEventListener("error", (event) => {
                        push("ERROR", `${event.message || "Error de navegador"}`);
                    });
                }
                render();
                setInterval(render, 1000);
            } catch (error) {
                root.textContent = `Log del navegador no disponible: ${error.message || error}`;
            }
        })();
        </script>
        """,
        unsafe_allow_javascript=True,
    )


def _render_upload_receipt(uploaded) -> None:
    size = _get_uploaded_size(uploaded)
    st.progress(
        100,
        text=f"Archivo recibido por la aplicación: 100% ({_format_bytes(size)})",
    )
    st.caption(f"{uploaded.name} listo para validar y procesar.")


def _check_upload_size(variable_id: str, uploaded) -> bool:
    size = _get_uploaded_size(uploaded)
    max_upload_size_mb = _read_max_upload_size_mb()
    if size is None or max_upload_size_mb is None:
        return True

    limit = max_upload_size_mb * 1024 * 1024
    ratio = size / limit if limit else 0
    signature = _file_signature(variable_id, uploaded)

    if size > limit:
        message = (
            f"El archivo pesa {_format_bytes(size)} y supera el límite configurado "
            f"de {_format_bytes(limit)}."
        )
        if st.session_state.get(UPLOAD_LAST_SIZE_WARNING_KEY) != signature:
            _add_upload_event("ERROR", "Tamaño", message)
            st.session_state[UPLOAD_LAST_SIZE_WARNING_KEY] = signature
        st.error(f"❌ {message}")
        return False

    if ratio >= 0.8:
        message = (
            f"Archivo grande: {_format_bytes(size)} de {_format_bytes(limit)} permitidos. "
            "Si aparece un error de conexión, prueba comprimir/partir el archivo o subirlo desde una conexión estable."
        )
        if st.session_state.get(UPLOAD_LAST_SIZE_WARNING_KEY) != signature:
            _add_upload_event("WARNING", "Tamaño", message)
            st.session_state[UPLOAD_LAST_SIZE_WARNING_KEY] = signature
        st.warning(f"⚠️ {message}")

    return True


def render_upload_page():
    """Renderiza la página completa de carga de archivos."""

    st.markdown("## 📂 Carga de archivos")
    st.markdown("Selecciona la variable, carga el archivo y el sistema ejecutará el pipeline ETL automáticamente.")

    st.markdown("---")

    col_form, col_history = st.columns([3, 2])

    with col_form:
        _render_upload_form()

        """    with col_history:
                _render_upload_history()"""
    with col_history:
        selected_id = st.session_state.get("upload_variable_select", "")
        if selected_id:
            config = UPLOAD_VARIABLES.get(selected_id, {})

            # Imagen del formato
            image_path = config.get("format_image", "")
            if image_path and os.path.exists(image_path):
                st.image(image_path, caption="Formato esperado", width="stretch")
            else:
                st.caption("No hay imagen de referencia para esta variable.")

            # Enlace de descarga
            download_url = config.get("download_url", "")
            if download_url:
                st.markdown(
                    f"📥 [Descargar datos fuente]({download_url})",
                    unsafe_allow_html=False,
                )
        else:
            st.caption("Selecciona una variable para ver el formato esperado.")

        st.markdown("---")
        _render_upload_diagnostic_panel()

# ─────────────────────────────────────────────────────────────
#  FORMULARIO DE CARGA
# ─────────────────────────────────────────────────────────────

def _render_upload_form():
    st.markdown("### 1. Selecciona la variable")

    # Selectbox con las variables disponibles
    options     = {"": "— Selecciona una variable —"}
    options    |= {k: v["label"] for k, v in UPLOAD_VARIABLES.items()}
    selected_id = st.selectbox(
        label            = "Variable",
        options          = list(options.keys()),
        format_func      = lambda k: options[k],
        label_visibility = "collapsed",
        key              = "upload_variable_select",
    )

    if not selected_id:
        return

    config = UPLOAD_VARIABLES[selected_id]

    # ── Mensaje informativo del formato ──────────────────────
    st.markdown("### 2. Formato esperado")
    st.info(config["format_hint"])
    extra_values = _render_extra_fields(config)
    # ── Uploader ─────────────────────────────────────────────
    st.markdown("### 3. Carga el archivo")
    allowed_ext  = config["allowed_types"]
    allowed_str  = ", ".join([f".{e}" for e in allowed_ext])

    uploaded = st.file_uploader(
        label   = f"Formatos permitidos: {allowed_str}",
        type    = allowed_ext,
        key     = f"uploader_{selected_id}",
    )

    if uploaded is None:
        return

    _register_uploaded_file(selected_id, uploaded)
    _render_upload_receipt(uploaded)
    if not _check_upload_size(selected_id, uploaded):
        return

    # ── Validación automática ─────────────────────────────────
    st.markdown("### 4. Validación")
    validation_signature = _file_signature(selected_id, uploaded)
    should_log_validation = (
        st.session_state.get(UPLOAD_LAST_VALIDATED_KEY) != validation_signature
    )
    if should_log_validation:
        _add_upload_event("INFO", "Validación", "Validando estructura del archivo")

    with st.spinner("Validando archivo..."):
        result = validate_file(uploaded, uploaded.name, config)

    if not result.valid:
        if should_log_validation:
            _add_upload_event("ERROR", "Validación", result.message, result.details)
            st.session_state[UPLOAD_LAST_VALIDATED_KEY] = validation_signature
        st.error(f"❌ {result.message}")
        for detail in result.details:
            st.caption(detail)
        return

    if should_log_validation:
        _add_upload_event("SUCCESS", "Validación", result.message, result.details)
        st.session_state[UPLOAD_LAST_VALIDATED_KEY] = validation_signature

    st.success(f"✅ {result.message}")
    for detail in result.details:
        st.caption(detail)

    # ── Confirmar y procesar ──────────────────────────────────
    st.markdown("### 5. Procesar")
    btn_disabled = extra_values is None
    if btn_disabled:
        st.caption("⚠️ Completa todos los parámetros adicionales para continuar.")

    if st.button(
        "🚀 Guardar y ejecutar pipeline",
        width="stretch",
        type="primary",
        disabled=btn_disabled,
    ):
        try:
            _process_file(uploaded, selected_id, config, extra_values or {})
        except Exception as e:
            _add_upload_event(
                "ERROR",
                "Interfaz",
                f"Error inesperado en la carga: {e}",
                traceback.format_exception(type(e), e, e.__traceback__),
            )
            st.error(f"❌ Error inesperado en la carga: {e}")

def _render_extra_fields(config: dict) -> dict | None:
    """
    Renderiza los campos adicionales definidos en extra_fields.
    Retorna un dict con los valores seleccionados, o None si falta algún requerido.
    """
    extra_fields = config.get("extra_fields", [])
    if not extra_fields:
        return {}

    st.markdown("### 2b. Parámetros adicionales")
    values = {}

    for field in extra_fields:
        fid     = field["id"]
        label   = field["label"]
        ftype   = field["type"]
        options = field.get("options", [])

        if ftype == "selectbox":
            placeholder = [f"— Selecciona {label.lower()} —"]
            all_options = placeholder + [str(o) for o in options]
            selected    = st.selectbox(
                label = label,
                options = all_options,
                key   = f"extra_{fid}",
            )
            if selected == placeholder[0]:
                if field.get("required"):
                    return None   # campo requerido sin valor
            else:
                values[fid] = selected

        elif ftype == "text_input":
            val = st.text_input(label=label, key=f"extra_{fid}")
            if not val and field.get("required"):
                return None
            values[fid] = val

        elif ftype == "number_input":
                    val = st.number_input(
                        label     = label,
                        min_value = field.get("min_value", 0),
                        max_value = field.get("max_value", 9999),
                        value     = None,
                        step      = 1,
                        key       = f"extra_{fid}",
                        placeholder = f"Ingresa {label.lower()}...",
                    )
                    if val is None and field.get("required"):
                        return None
                    if val is not None:
                        values[fid] = int(val)

    return values

def _process_file(uploaded, variable_id: str, config: dict, extra_values: dict = {}):
    """Guarda el archivo y ejecuta el pipeline ETL."""
    # Construir sufijo con los valores extra para el nombre del archivo
    # ejemplo: censo_bovino_bovino_2023_run_20240315.xlsx
    extra_suffix = "_".join([str(v) for v in extra_values.values()])
    # Guardar archivo
    st.markdown("**Guardando archivo...**")
    _add_upload_event(
        "INFO",
        "Guardado",
        "Guardando archivo en capa bronze",
        [
            f"Variable: {variable_id}",
            f"Archivo: {uploaded.name}",
            f"Tamaño: {_format_bytes(_get_uploaded_size(uploaded))}",
            f"Pipeline: {config['pipeline']}",
        ],
    )
    save_progress = st.progress(0, text="Guardando archivo: 0%")
    save_detail = st.empty()

    def update_save_progress(written: int, total: int) -> None:
        if total > 0:
            percent = min(100, max(0, int(written / total * 100)))
            detail = f"{_format_bytes(written)} de {_format_bytes(total)} guardados"
        else:
            percent = 100 if written else 0
            detail = f"{_format_bytes(written)} guardados"

        save_progress.progress(percent, text=f"Guardando archivo: {percent}%")
        save_detail.caption(detail)

    success, msg, saved_path = save_file(
        file_obj          = uploaded,
        filename          = uploaded.name,
        variable_id       = variable_id,
        storage_folder    = config["storage_folder"],
        progress_callback = update_save_progress,
    )

    if not success:
        _add_upload_event("ERROR", "Guardado", msg)
        st.error(f"❌ {msg}")
        return

    save_progress.progress(100, text="Guardando archivo: 100%")
    _add_upload_event("SUCCESS", "Guardado", msg, [saved_path])
    st.success(f"💾 {msg}")
    st.markdown("**Ejecutando pipeline ETL...**")
    status = st.status(f"Ejecutando pipeline `{config['pipeline']}`...", expanded=True)
    progress_bar = st.progress(0, text="Preparando ejecución...")
    live_log = st.empty()
    logs: list[str] = []
    pipeline_result = None

    for event in stream_pipeline(
        config["pipeline"],
        saved_path,
        extra_env={f"OBSAN_{k.upper()}": str(v) for k, v in extra_values.items()},
    ):
        if event.kind in {"meta", "log", "heartbeat"}:
            logs.append(event.message)
            live_log.code("\n".join(logs[-120:]), language="text")
            if event.kind == "log" and _looks_like_error(event.message):
                _add_upload_event("ERROR", "Pipeline", event.message)

        if event.progress is not None:
            progress_value = max(0, min(100, int(event.progress * 100)))
            progress_text = "Procesando..." if event.kind == "log" else event.message
            progress_bar.progress(progress_value, text=progress_text)

        if event.kind in {"progress", "heartbeat", "result"}:
            status.update(label=event.message)

        if event.kind == "result":
            pipeline_result = event.result

    if pipeline_result is None:
        _add_upload_event(
            "ERROR",
            "Pipeline",
            "El pipeline terminó sin devolver resultado.",
            logs[-30:],
        )
        status.update(label="El pipeline terminó sin devolver resultado.", state="error")
        st.error("❌ Error al cargar: el pipeline terminó sin devolver resultado.")
        return

    status.update(
        label=pipeline_result.message,
        state="complete" if pipeline_result.success else "error",
        expanded=not pipeline_result.success,
    )

    if pipeline_result.logs:
        with st.expander("📋 Logs del pipeline", expanded=not pipeline_result.success):
            for log_line in pipeline_result.logs:
                st.caption(f"› {log_line}")

    if pipeline_result.success:
        _add_upload_event("SUCCESS", "Pipeline", pipeline_result.message)
        st.success(
            f"✅ {pipeline_result.message}"
            + (f" ({pipeline_result.rows_processed:,} filas)" if pipeline_result.rows_processed else "")
        )
        st.balloons()
    else:
        _add_upload_event(
            "ERROR",
            "Pipeline",
            pipeline_result.message,
            pipeline_result.logs[-40:],
        )
        st.error(f"❌ Error al cargar: {pipeline_result.message}")


# ─────────────────────────────────────────────────────────────
#  HISTORIAL DE CARGAS
# ─────────────────────────────────────────────────────────────

def _render_upload_history():
    st.markdown("### Historial de cargas")

    selected_id = st.session_state.get("upload_variable_select", "")

    if not selected_id:
        st.caption("Selecciona una variable para ver su historial.")
        return

    config = UPLOAD_VARIABLES.get(selected_id, {})
    files  = list_uploaded_files(selected_id, config.get("storage_folder", ""))

    if not files:
        st.caption("No hay archivos cargados para esta variable.")
        return

    st.caption(f"{len(files)} archivo(s) encontrado(s)")

    for f in files[:10]:   # mostrar máximo 10
        with st.container():
            st.markdown(
                f"📄 **{f['name']}**  \n"
                f"<span style='font-size:11px;color:#888;'>"
                f"{f['modified']} · {f['size_kb']} KB"
                f"</span>",
                unsafe_allow_html=True,
            )
