from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler

def run(**kwargs):
    print(f"Pipeline ejecutada a las {datetime.now()} con argumentos: {kwargs}")

    with open("pipeline_log.txt", "a") as log_file:
        log_file.write(f"Pipeline ejecutada a las {datetime.now()} con argumentos: {kwargs}\n")

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(run, "interval", seconds=15)

    try:
        print("Iniciando el scheduler...")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("Scheduler detenido.")