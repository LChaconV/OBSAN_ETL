"""
backend_logging.py — Logging estructurado para cargas desde Streamlit.
"""

import json
import logging
import os
import sys
import traceback
from datetime import datetime, timezone
from typing import Any


LOGGER_NAME = "obsan.upload"
DEFAULT_LOG_LEVEL = "INFO"


def get_upload_logger() -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    if getattr(logger, "_obsan_upload_configured", False):
        return logger

    level_name = os.getenv("OBSAN_UPLOAD_LOG_LEVEL", DEFAULT_LOG_LEVEL).upper()
    level = getattr(logging, level_name, logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))

    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
    logger._obsan_upload_configured = True
    return logger


def _safe_value(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (list, tuple)):
        return [_safe_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _safe_value(val) for key, val in value.items()}
    return str(value)


def log_upload_event(
    level: str,
    stage: str,
    message: str,
    **fields: Any,
) -> None:
    payload = {
        "event": "upload",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "stage": stage,
        "message": message,
        **{key: _safe_value(value) for key, value in fields.items() if value is not None},
    }
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    get_upload_logger().log(numeric_level, json.dumps(payload, ensure_ascii=False, sort_keys=True))


def log_upload_exception(
    stage: str,
    message: str,
    exc: BaseException,
    **fields: Any,
) -> None:
    log_upload_event(
        "ERROR",
        stage,
        message,
        error_type=type(exc).__name__,
        error_message=str(exc),
        traceback=traceback.format_exception(type(exc), exc, exc.__traceback__),
        **fields,
    )
