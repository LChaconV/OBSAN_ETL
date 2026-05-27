from . import extract, transform, load
from apscheduler.schedulers.blocking import BlockingScheduler

def run(**kwargs):
    print("Iniciando extracción...")
    run_dir = extract.run(**kwargs)
    print("Extracción completada")

    if run_dir is None:
        print("No hay datos nuevos. Se omite transformación y carga.")
        return

    print("Iniciando transformación...")
    transform.run()
    print("Transformación completada")

    print("Iniciando carga...")
    load.run()
    print("Carga completada.")

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(run, "interval", seconds=15)

    try:
        print("Iniciando el scheduler...")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("Scheduler detenido.")