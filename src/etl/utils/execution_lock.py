from __future__ import annotations

import json
import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

_IS_WINDOWS = sys.platform == "win32"

if not _IS_WINDOWS:
    import fcntl

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LOCK_PATH = PROJECT_ROOT / "state" / "etl_execution.lock"


class ETLExecutionLockBusy(RuntimeError):
    def __init__(self, metadata: dict | None = None):
        self.metadata = metadata or {}
        super().__init__("Ya hay una ejecución ETL en curso.")


def get_lock_path() -> Path:
    configured_path = os.getenv("ETL_EXECUTION_LOCK_PATH")
    return Path(configured_path) if configured_path else DEFAULT_LOCK_PATH


def read_lock_metadata(path: Path | None = None) -> dict:
    lock_path = path or get_lock_path()

    try:
        raw = lock_path.read_text(encoding="utf-8").strip()
    except OSError:
        return {}

    if not raw:
        return {}

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"raw": raw}


def read_active_lock_metadata(path: Path | None = None) -> dict:
    if _IS_WINDOWS:
        return {}

    lock_path = path or get_lock_path()

    try:
        with lock_path.open("a+", encoding="utf-8") as lock_file:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return read_lock_metadata(lock_path)

            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            return {}
    except OSError:
        return {}


@contextmanager
def acquire_etl_execution_lock(
    *,
    pipeline_name: str,
    owner: str,
    blocking: bool,
) -> Iterator[dict]:
    if _IS_WINDOWS:
        metadata = {
            "pipeline": pipeline_name,
            "owner": owner,
            "pid": os.getpid(),
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        }
        yield metadata
        return

    lock_path = get_lock_path()
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    with lock_path.open("a+", encoding="utf-8") as lock_file:
        operation = fcntl.LOCK_EX
        if not blocking:
            operation |= fcntl.LOCK_NB

        try:
            fcntl.flock(lock_file.fileno(), operation)
        except BlockingIOError as exc:
            raise ETLExecutionLockBusy(read_lock_metadata(lock_path)) from exc

        metadata = {
            "pipeline": pipeline_name,
            "owner": owner,
            "pid": os.getpid(),
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        }
        lock_file.seek(0)
        lock_file.truncate()
        json.dump(metadata, lock_file, ensure_ascii=False)
        lock_file.flush()
        os.fsync(lock_file.fileno())

        try:
            yield metadata
        finally:
            lock_file.seek(0)
            lock_file.truncate()
            lock_file.flush()
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
