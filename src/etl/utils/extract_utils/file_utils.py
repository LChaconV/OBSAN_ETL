
import pandas as pd
from pathlib import Path
import logging

def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df.columns = [str(col).strip().lower() for col in df.columns]

    return df

def create_run_directory(config: dict, project_root: Path) -> Path:
    bronze_dir = project_root / config["storage"]["bronze_dir"]
    run_id = pd.Timestamp.utcnow().strftime("run_%Y%m%d_%H%M%S")

    run_dir = bronze_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    return run_dir

def save_bronze_page(df: pd.DataFrame, run_dir: Path, file_prefix: str, page_number: int) -> Path:
    output_path = run_dir / f"{file_prefix}_page_{page_number}.parquet"
    df.to_parquet(output_path, index=False)

    logging.info("Página %s guardada en %s", page_number, output_path)
    return output_path
