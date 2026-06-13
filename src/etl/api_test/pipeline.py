from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler

from src.etl.utils.pipeline_cleanup import run_with_cleanup

def _run_steps(**kwargs):
    print(f"Pipeline ejecutada a las {datetime.now()} con argumentos: {kwargs}")

    with open("pipeline_log.txt", "a") as log_file:
        log_file.write(f"Pipeline ejecutada a las {datetime.now()} con argumentos: {kwargs}\n")


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