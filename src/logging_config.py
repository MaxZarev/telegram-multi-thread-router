"""Logging configuration — dual output (human-readable stderr + JSON file with rotation)."""

import json
import logging
import logging.handlers
import os
import time
import traceback
from contextlib import asynccontextmanager
from datetime import datetime, timezone


# Extra fields that JsonFormatter will pick up from LogRecord
_EXTRA_FIELDS = (
    "thread_id",
    "session_id",
    "duration_ms",
    "tool_name",
    "tool_index",
    "queue_size",
    "msg_type",
    "msg_index",
    "message_id",
    "text_length",
    "file_size",
    "tool_count",
    "stuck_duration_ms",
    "last_tool",
    "sdk_msg_count",
    "silence_duration_ms",
    "last_msg_type",
    "result",
    "resets_at",
)


class JsonFormatter(logging.Formatter):
    """Format log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        data: dict = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for field in _EXTRA_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                data[field] = value
        if record.exc_info and record.exc_info[1] is not None:
            data["traceback"] = "".join(traceback.format_exception(*record.exc_info))
        return json.dumps(data, ensure_ascii=False, default=str)


def setup_logging(
    *,
    log_dir: str = "logs",
    log_level: str = "INFO",
    max_bytes: int = 50_000_000,
    backup_count: int = 3,
) -> None:
    """Configure root logger with stderr (text) + file (JSON) handlers."""
    os.makedirs(log_dir, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Remove existing handlers (from basicConfig in __main__.py)
    for h in root.handlers[:]:
        root.removeHandler(h)
        h.close()

    # Stderr — human-readable
    stderr_handler = logging.StreamHandler()
    stderr_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    )
    root.addHandler(stderr_handler)

    # File — JSON with rotation
    file_handler = logging.handlers.RotatingFileHandler(
        os.path.join(log_dir, "bot.log"),
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(JsonFormatter())
    root.addHandler(file_handler)

    # Quiet noisy external loggers
    for name in ("aiogram", "aiohttp", "claude_agent_sdk", "httpx", "httpcore"):
        logging.getLogger(name).setLevel(logging.WARNING)


@asynccontextmanager
async def timed(
    logger: logging.Logger,
    msg: str,
    *,
    warn_seconds: float = 300.0,
    error_seconds: float = 900.0,
    **ctx,
):
    """Async context manager that logs `msg` with `duration_ms` on exit.

    Level is INFO normally, WARNING if duration > warn_seconds, ERROR if > error_seconds.
    Extra keyword arguments are attached to the log record via `extra=`.
    """
    start = time.monotonic()
    try:
        yield
    finally:
        elapsed = time.monotonic() - start
        duration_ms = int(elapsed * 1000)
        ctx["duration_ms"] = duration_ms

        if elapsed >= error_seconds:
            level = logging.ERROR
        elif elapsed >= warn_seconds:
            level = logging.WARNING
        else:
            level = logging.INFO

        logger.log(level, msg, extra=ctx)
