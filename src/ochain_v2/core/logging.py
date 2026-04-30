"""
Structured JSON logger for OChain v2.

Every log record is emitted as a single JSON line so it can be ingested
by any log aggregator (Loki, Datadog, CloudWatch, etc.).

Usage:
    from ochain_v2.core.logging import get_logger
    log = get_logger(__name__)
    log.info("snapshot saved", extra={"symbol": "NIFTY", "snapshot_id": 123})
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

_configured = False


class _JsonFormatter(logging.Formatter):
    """Formats each LogRecord as a single JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S.%f"
            )[:-3]
            + "Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }

        # Include any extra fields passed via extra={...}
        skip = {
            "name", "msg", "args", "levelname", "levelno", "pathname",
            "filename", "module", "exc_info", "exc_text", "stack_info",
            "lineno", "funcName", "created", "msecs", "relativeCreated",
            "thread", "threadName", "processName", "process", "message",
            "taskName",
        }
        for key, val in record.__dict__.items():
            if key not in skip:
                payload[key] = val

        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        elif record.exc_text:
            payload["exc"] = record.exc_text

        return json.dumps(payload, default=str)


def configure_logging(
    level: str = "INFO",
    log_file: Optional[str] = None,
) -> None:
    """
    Configure the root logger with a JSON formatter.
    Call once at process startup (idempotent: subsequent calls are no-ops).
    """
    global _configured
    if _configured:
        return
    _configured = True

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    fmt = _JsonFormatter()

    # Stdout handler
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    root.addHandler(sh)

    # Rotating file handler (optional)
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        fh = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding="utf-8",
        )
        fh.setFormatter(fmt)
        root.addHandler(fh)

    # Silence noisy third-party loggers
    for noisy in ("uvicorn.access", "apscheduler.executors.default"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger. configure_logging() need not be called first."""
    return logging.getLogger(name)
