from . import  extract,transform, load
from apscheduler.schedulers.blocking import BlockingScheduler

from src.etl.utils.pipeline_cleanup import run_with_cleanup

def _run_steps(**kwargs):

    print("Iniciando extracción...")
    extract.run()
    print("Extracción completada")

    print("Iniciando transformación...")
    transform.run()
    print("Transformación completada")

    print("Iniciando carga...")
    load.run()
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