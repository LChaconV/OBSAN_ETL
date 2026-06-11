import os
from pathlib import Path

from src.etl.utils.config_utils import load_yaml
from . import  transform, load
from apscheduler.schedulers.blocking import BlockingScheduler

from src.etl.utils.pipeline_cleanup import run_with_cleanup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sources_config = load_yaml(load.SOURCES_CONFIG_PATH)

def _run_steps(**kwargs):
    env_path = os.environ.get("OBSAN_INPUT_FILE")
    silver_path = Path(env_path) if env_path else PROJECT_ROOT / sources_config["divipola"]["source"]["silver_fact_dir"]

    if silver_path.suffix == ".geojson":
        print("Iniciando transformación...")
        transform.run(input_path=silver_path)
        print("Transformación completada")

        print("Iniciando carga...")
        load.run(input_path=None)  # usará silver_fact_dir del config
        print("Carga completada.")

    elif silver_path.suffix == ".parquet":
        print("Iniciando carga...")
        load.run(input_path=silver_path)
        print("Carga completada.")


def run(**kwargs):
    return run_with_cleanup(__name__, _run_steps, **kwargs)

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(run, "interval", seconds=15)

    try:
        print("Iniciando el scheduler...")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("Scheduler detenido.")