import logging
from pathlib import Path
import pandas as pd

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

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_dir / log_file, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )


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
