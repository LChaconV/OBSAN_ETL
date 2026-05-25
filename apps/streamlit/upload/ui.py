"""
upload/ui.py — Interfaz Streamlit del módulo de carga de archivos
"""

import os

import streamlit as st
from upload.variables_config import UPLOAD_VARIABLES
from upload.validator import validate_file
from upload.storage import save_file, list_uploaded_files
from upload.pipeline_runner import run_pipeline


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
                st.image(image_path, caption="Formato esperado",
                        use_container_width=True)
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

    # ── Validación automática ─────────────────────────────────
    st.markdown("### 4. Validación")
    with st.spinner("Validando archivo..."):
        result = validate_file(uploaded, uploaded.name, config)

    if not result.valid:
        st.error(f"❌ {result.message}")
        for detail in result.details:
            st.caption(detail)
        return

    st.success(f"✅ {result.message}")
    for detail in result.details:
        st.caption(detail)

    # ── Confirmar y procesar ──────────────────────────────────
    st.markdown("### 5. Procesar")
    btn_disabled = extra_values is None
    if btn_disabled:
        st.caption("⚠️ Completa todos los parámetros adicionales para continuar.")

    if st.button("🚀 Guardar y ejecutar pipeline", use_container_width=True, type="primary"):
        _process_file(uploaded, selected_id, config, extra_values or {})

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
    with st.spinner("Guardando archivo..."):
        success, msg, saved_path = save_file(
            file_obj       = uploaded,
            filename       = uploaded.name,
            variable_id    = variable_id,
            storage_folder = config["storage_folder"],
        )

    if not success:
        st.error(f"❌ {msg}")
        return

    st.success(f"💾 {msg}")
    # Ejecutar pipeline
    st.markdown("**Ejecutando pipeline ETL...**")
    log_container = st.empty()

    with st.spinner(f"Ejecutando pipeline `{config['pipeline']}`..."):
        pipeline_result = run_pipeline(
            config["pipeline"],
            saved_path,
            extra_env = {f"OBSAN_{k.upper()}": str(v) for k, v in extra_values.items()}
        )
    # Mostrar logs del pipeline
    #if pipeline_result.logs:
    #    with st.expander("📋 Logs del pipeline", expanded=not pipeline_result.success):
    #        for log_line in pipeline_result.logs:
    #            st.caption(f"› {log_line}")

    if pipeline_result.success:
        st.success(
            f"✅ {pipeline_result.message}"
            + (f" ({pipeline_result.rows_processed:,} filas)" if pipeline_result.rows_processed else "")
        )
        st.balloons()
    else:
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