"""Minimal structured JSON logging without secret-bearing configuration dumps."""

import json
import logging
from collections import deque
from datetime import UTC, datetime
from typing import Any

ERROR_LOGS: deque[dict[str, str]] = deque(maxlen=200)


class JsonFormatter(logging.Formatter):
    """Emit one JSON object per log record."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


class ErrorBufferHandler(logging.Handler):
    """Keep recent errors in memory for the local dashboard."""

    def emit(self, record: logging.LogRecord) -> None:
        if record.levelno < logging.ERROR:
            return
        item = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            item["exception"] = logging.Formatter().formatException(record.exc_info)
        ERROR_LOGS.appendleft(item)


def recent_errors(limit: int = 50) -> list[dict[str, str]]:
    return list(ERROR_LOGS)[:limit]


def configure_logging(level: str) -> None:
    """Configure the root logger exactly once for the API process."""
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    error_buffer = ErrorBufferHandler()
    logging.basicConfig(level=level.upper(), handlers=[handler, error_buffer], force=True)
