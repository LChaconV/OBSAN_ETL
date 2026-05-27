import json
import logging
import os
import re
import subprocess
import sys
import threading
from datetime import datetime, timedelta
from pathlib import Path

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.blocking import BlockingScheduler

LOGGER = logging.getLogger("etl_scheduler")
REPO_ROOT = Path(__file__).resolve().parents[1]
INTERVAL_FIELDS = ("weeks", "days", "hours", "minutes", "seconds")
CRON_FIELDS = ("year", "month", "day", "week", "day_of_week", "hour", "minute", "second")
CHILD_LOG_PATTERN = re.compile(
    r"^(?P<timestamp>[^|]+)\|\s*(?P<level>[A-Z]+)\s*\|\s*(?P<context>(?:\[[^\]]+\]\s*)*)(?P<message>.*)$"
)
CHILD_CONTEXT_PATTERN = re.compile(r"\[([^\]]+)\]")
LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


def configure_logging() -> None:
    log_level = os.getenv("ETL_SCHEDULER_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )


def load_schedule_config() -> list[dict]:
    raw_schedules = os.getenv("ETL_SCHEDULES")
    if not raw_schedules:
        raise ValueError("La variable ETL_SCHEDULES es obligatoria.")

    schedules = json.loads(raw_schedules)
    if not isinstance(schedules, list) or not schedules:
        raise ValueError("ETL_SCHEDULES debe ser una lista JSON con al menos un pipeline.")

    for schedule in schedules:
        if not isinstance(schedule, dict) or not schedule.get("name"):
            raise ValueError("Cada pipeline debe definir al menos el campo 'name'.")

    return schedules


def build_trigger(schedule: dict) -> tuple[str, dict]:
    trigger = schedule.get("trigger", "interval")

    if trigger == "interval":
        trigger_kwargs = {
            field: int(schedule[field])
            for field in INTERVAL_FIELDS
            if schedule.get(field) is not None
        }
        if not trigger_kwargs:
            raise ValueError(
                f"El pipeline '{schedule['name']}' debe definir un intervalo valido."
            )

        start_delay_seconds = int(schedule.get("startDelaySeconds", 0))
        if start_delay_seconds > 0:
            trigger_kwargs["start_date"] = datetime.now() + timedelta(seconds=start_delay_seconds)

        return trigger, trigger_kwargs

    if trigger == "cron":
        trigger_kwargs = {
            field: str(schedule[field])
            for field in CRON_FIELDS
            if schedule.get(field) is not None
        }
        if not trigger_kwargs:
            raise ValueError(
                f"El pipeline '{schedule['name']}' debe definir al menos un campo cron."
            )

        return trigger, trigger_kwargs

    raise ValueError(
        f"El pipeline '{schedule['name']}' usa un trigger no soportado: {trigger}."
    )


def run_pipeline_subprocess(source_name: str) -> None:
    command = ["uv", "run", "-m", "src.runner", source_name]
    env = os.environ.copy()
    env.pop("ETL_SCHEDULES", None)
    env["ETL_PIPELINE_NAME"] = source_name
    env.setdefault("PYTHONUNBUFFERED", "1")

    LOGGER.info("Iniciando pipeline '%s'.", source_name)
    process = subprocess.Popen(
        command,
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )

    stdout_thread = threading.Thread(
        target=forward_subprocess_logs,
        args=(process.stdout, source_name, "stdout"),
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=forward_subprocess_logs,
        args=(process.stderr, source_name, "stderr"),
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()

    return_code = process.wait()
    stdout_thread.join()
    stderr_thread.join()

    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, command)


def forward_subprocess_logs(pipe, source_name: str, stream_name: str) -> None:
    if pipe is None:
        return

    try:
        for raw_line in iter(pipe.readline, ""):
            line = raw_line.rstrip()
            if not line:
                continue

            level, message = parse_subprocess_log_line(
                source_name=source_name,
                stream_name=stream_name,
                line=line,
            )
            LOGGER.log(level, "%s", message)
    finally:
        pipe.close()


def parse_subprocess_log_line(source_name: str, stream_name: str, line: str) -> tuple[int, str]:
    match = CHILD_LOG_PATTERN.match(line)
    if not match:
        fallback_level = logging.ERROR if stream_name == "stderr" else logging.INFO
        return fallback_level, f"[{source_name}] [{stream_name}] {line}"

    contexts = [
        context
        for context in CHILD_CONTEXT_PATTERN.findall(match.group("context"))
        if context != source_name
    ]
    context_prefix = " ".join(f"[{context}]" for context in [source_name, *contexts])
    message = match.group("message").strip()
    level = LOG_LEVELS.get(match.group("level"), logging.INFO)

    return level, f"{context_prefix} {message}".strip()


def log_job_event(event) -> None:
    if event.exception:
        LOGGER.error("El pipeline '%s' fallo.\n%s", event.job_id, event.traceback)
        return

    LOGGER.info("Pipeline '%s' finalizado correctamente.", event.job_id)


def create_scheduler(
    schedules: list[dict],
    max_workers: int,
    misfire_grace_time: int,
) -> BlockingScheduler:
    scheduler = BlockingScheduler(
        executors={"default": ThreadPoolExecutor(max_workers=max_workers)},
        job_defaults={
            "coalesce": True,
            "misfire_grace_time": misfire_grace_time,
        },
    )
    scheduler.add_listener(log_job_event, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    for schedule in schedules:
        trigger, trigger_kwargs = build_trigger(schedule)
        scheduler.add_job(
            run_pipeline_subprocess,
            trigger=trigger,
            id=schedule["name"],
            kwargs={"source_name": schedule["name"]},
            replace_existing=True,
            max_instances=int(schedule.get("maxInstances", 1)),
            **trigger_kwargs,
        )

    return scheduler


def main() -> None:
    configure_logging()
    schedules = load_schedule_config()
    max_workers = int(os.getenv("ETL_MAX_CONCURRENT_JOBS", "1"))
    misfire_grace_time = int(os.getenv("ETL_JOB_MISFIRE_GRACE_TIME", "30"))

    scheduler = create_scheduler(
        schedules=schedules,
        max_workers=max_workers,
        misfire_grace_time=misfire_grace_time,
    )

    LOGGER.info(
        "Scheduler ETL iniciado con %s pipelines y concurrencia maxima %s.",
        len(schedules),
        max_workers,
    )
    scheduler.start()


if __name__ == "__main__":
    main()