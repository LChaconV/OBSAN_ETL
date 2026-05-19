# OBSAN - Código unificado (ETL + Streamlit)

Este repositorio ahora concentra toda la base de código en una sola unidad: **proceso ETL** y **aplicación web Streamlit**.

## División funcional del código

- **Web (Streamlit):** `apps/streamlit/`
- **ETL y orquestación:** `src/etl/`, `src/runner.py`, `src/scheduler.py`
- **Configuración ETL:** `config/`
- **SQL de apoyo:** `sql/`
- **Datos de entrada (bronze):** `data/bronze/` (creado en ejecución)

## Estructura principal (analizada con `erd`)

```text
etl/
├── apps/
│   └── streamlit/
│       ├── app.py
│       ├── components/
│       ├── config/
│       ├── core/
│       ├── pages/
│       └── upload/
├── src/
│   ├── runner.py
│   ├── scheduler.py
│   └── etl/
├── config/
├── sql/
├── pyproject.toml
└── uv.lock
```

## Entorno (solo uv)

```bash
uv sync
```

Si usas fish:

```fish
source .venv/bin/activate.fish
```

## Ejecución de la app web (Streamlit)

Desde la raíz:

```bash
uv run streamlit run apps/streamlit/app.py
```

## Ejecución ETL

Ejecutar una fuente puntual:

```bash
uv run -m src.runner "nombre_fuente"
```

Ejecutar scheduler:

```bash
uv run -m src.scheduler
```

## Lógica del proceso ETL

Flujo de alto nivel:

```text
Extract → Bronze → Transform → Silver/Golden → Load → PostgreSQL
```

1. **Extract:** descarga/lectura de fuentes (API, archivos, geodatos).
2. **Bronze:** persistencia de insumos crudos en `data/bronze/`.
3. **Transform:** estandarización, limpieza, reglas y enriquecimiento.
4. **Load:** carga en PostgreSQL/PostGIS (dimensiones, hechos y tablas analíticas).

La app web usa el módulo `apps/streamlit/upload/` para recibir archivos y disparar pipelines ETL registrados en `pipeline_runner.py`.

## Configuración de base de datos (variables de entorno)

La capa ETL y la app Streamlit leen la conexión PostgreSQL/PostGIS desde `.env`.

Variables requeridas:

```dotenv
DB_HOST=localhost
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=tu_password
```
