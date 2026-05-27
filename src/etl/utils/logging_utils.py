import logging
import os
import sys
from pathlib import Path

import pandas as pd


class _ETLContextFilter(logging.Filter):
    def __init__(self, pipeline_name: str, component_name: str):
        super().__init__()
        self.pipeline_name = pipeline_name
        self.component_name = component_name

    def filter(self, record: logging.LogRecord) -> bool:
        record.pipeline_name = getattr(record, "pipeline_name", self.pipeline_name)
        record.component_name = getattr(record, "component_name", self.component_name)
        return True


def setup_logging(log_dir: Path, log_file: str = "etl.log") -> None:
    """
    Configura el sistema de logging del proyecto.

    Parameters
    ----------
    log_dir : Path
        Directorio donde se guardarán los logs.
    log_file : str
        Nombre del archivo de log.
    """

    log_dir.mkdir(parents=True, exist_ok=True)

    pipeline_name = os.getenv("ETL_PIPELINE_NAME", "etl")
    component_name = Path(log_file).stem
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | [%(pipeline_name)s] [%(component_name)s] %(message)s"
    )
    context_filter = _ETLContextFilter(
        pipeline_name=pipeline_name,
        component_name=component_name,
    )

    handlers = [
        logging.FileHandler(log_dir / log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]

    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        handler.close()
    root_logger.setLevel(logging.INFO)

    for handler in handlers:
        handler.setFormatter(formatter)
        handler.addFilter(context_filter)
        root_logger.addHandler(handler)


def log_run_summary(df: pd.DataFrame, config: dict) -> None:
    if df.empty:
        logging.info("No se descargaron filas.")
        return

    logging.info("Filas totales descargadas: %s", len(df))
    logging.info("Columnas descargadas: %s", df.columns.tolist())

    incremental_column = config.get("incremental", {}).get("column")
    if incremental_column and incremental_column.lower() in df.columns:
        col = incremental_column.lower()
        parsed = pd.to_datetime(df[col], errors="coerce", utc=True)
        logging.info("Valor mínimo incremental: %s", parsed.min())
        logging.info("Valor máximo incremental: %s", parsed.max())
