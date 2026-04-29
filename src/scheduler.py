# scheduler.py
from apscheduler.schedulers.blocking import BlockingScheduler
from runner import run_pipeline

scheduler = BlockingScheduler()

# Cada fuente con su propia 
"""
scheduler.add_job(
    lambda: run_pipeline("edu_superior"),
    trigger="cron",
    day="last",
    hour=6,
    minute=0
)
"""


scheduler.start()