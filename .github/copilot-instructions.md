# Copilot instructions for OBSAN_ETL

## Commands

- Install the environment with `uv sync`.
- There is no separate build step defined in the repository.
- Run the Streamlit app from the repository root with `uv run streamlit run apps/streamlit/app.py`.
- Run one ETL source from the CLI with `uv run -m src.runner "<source_folder_name>"`. Example: `uv run -m src.runner "api_edu_superior"`.
- Run the scheduler with `uv run -m src.scheduler`.
- There is no in-repo automated test suite or lint configuration (`pytest`, `ruff`, `mypy`, GitHub Actions, and `tests/` are absent). For narrow validation, run a single pipeline module directly, for example:
  - API-backed pipeline: `uv run -m src.etl.api_edu_superior.pipeline`
  - File-upload pipeline: `OBSAN_INPUT_FILE=/abs/path/to/file.geojson uv run -m src.etl.dw_divipola.pipeline`

## High-level architecture

- This repository combines two runtime surfaces that share the same PostgreSQL/PostGIS database:
  - `src/` contains ETL entrypoints and source-specific pipelines.
  - `apps/streamlit/` contains the observatory UI and a file-upload UI that triggers ETL pipelines.
- The ETL side is organized as one folder per dataset under `src/etl/<pipeline_folder>/`. Each dataset usually follows the same shape:
  - `pipeline.py` orchestrates `extract -> transform -> load`
  - `extract.py` reads remote/API sources into `data/bronze/...`
  - `transform.py` reads the latest bronze run, applies YAML-driven typing/dedup/renaming rules, and writes parquet outputs to `data/silver/...` and often `data/golden/...`
  - `load.py` loads the latest golden parquet into Postgres, usually via the shared `load_parquet_to_postgres()` helper
- `src/runner.py` is dynamic: it imports `src.etl.<source_folder>.pipeline`, so the ETL folder name is the runtime contract for CLI execution.
- API extraction behavior is configured in `config/sources.yaml`; transform behavior is configured per dataset in `config/transform/*_transform.yaml`.
- Checkpoint/state is split across two files:
  - `state/state.yaml` tracks extraction checkpoints for bronze ingestion
  - `config/state_db.yaml` tracks which transformed/golden files have already been loaded to Postgres
- The Streamlit map is metadata-driven. `apps/streamlit/config/layers_config.py` is the main registry for map layers, their grouping, and the database tables/views they query. The sidebar and map components consume that registry rather than hardcoding individual layers.
- The Streamlit upload flow is also registry-driven:
  - `apps/streamlit/upload/variables_config.py` defines the uploadable dataset, accepted file types, required columns, and the pipeline ID shown in the UI.
  - `apps/streamlit/upload/pipeline_runner.py` maps that pipeline ID to a folder in `src/etl/` and executes the existing `pipeline.py` in a subprocess.

## Key conventions

- Use `uv` commands, not ad hoc `pip`/`python` workflows; the README only documents `uv`.
- Repository language is mostly Spanish in comments, logs, YAML keys, and UI text. Preserve existing Spanish naming and messages unless there is a strong reason to change them.
- ETL datasets rely on registry alignment across multiple files. When adding or renaming a dataset, keep these in sync:
  - ETL folder in `src/etl/<folder>`
  - source key in `config/sources.yaml` when extraction exists
  - transform config file and top-level YAML key in `config/transform/*`
  - loader state key used in `config/state_db.yaml`
  - upload registry entries in `apps/streamlit/upload/variables_config.py` and `apps/streamlit/upload/pipeline_runner.py` when the dataset is user-uploaded
- File-upload pipelines do not receive CLI args. The Streamlit upload runner passes inputs through environment variables such as `OBSAN_INPUT_FILE`, plus dataset-specific variables like `OBSAN_YEAR` and `OBSAN_ANIMAL_TYPE`.
- The normal ETL file lifecycle is `bronze run directory -> silver parquet -> golden parquet -> Postgres`. Helpers in `src/etl/utils/transform_utils.py` assume bronze runs are stored in `run_*` directories and that transformed parquet outputs are named from the dataset `file_prefix` plus the run name.
- Database access is expected to come from `.env` (`DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`) for both ETL and Streamlit. Do not introduce a second connection mechanism unless you update both runtime surfaces.
- For map work, prefer editing `apps/streamlit/config/layers_config.py` first. It is the intended single place to add, remove, or regroup layers, and its entries must match the actual Postgres table/view names produced by the ETL side.
