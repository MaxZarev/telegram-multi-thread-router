"""Tests for logging_config module."""

import asyncio
import json
import logging
import os
import tempfile

import pytest

from src.logging_config import JsonFormatter, setup_logging, timed


class TestJsonFormatter:
    def test_basic_fields(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test.module",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="hello world",
            args=None,
            exc_info=None,
        )
        line = formatter.format(record)
        data = json.loads(line)
        assert data["level"] == "INFO"
        assert data["logger"] == "test.module"
        assert data["msg"] == "hello world"
        assert "ts" in data

    def test_extra_fields_included(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.WARNING,
            pathname="test.py",
            lineno=1,
            msg="tool used",
            args=None,
            exc_info=None,
        )
        record.thread_id = 12345
        record.duration_ms = 500
        line = formatter.format(record)
        data = json.loads(line)
        assert data["thread_id"] == 12345
        assert data["duration_ms"] == 500

    def test_exception_included(self):
        formatter = JsonFormatter()
        try:
            raise ValueError("boom")
        except ValueError:
            import sys
            exc_info = sys.exc_info()
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="test.py",
            lineno=1,
            msg="error",
            args=None,
            exc_info=exc_info,
        )
        line = formatter.format(record)
        data = json.loads(line)
        assert "traceback" in data
        assert "boom" in data["traceback"]


class TestSetupLogging:
    def test_creates_log_dir_and_file_handler(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_dir = os.path.join(tmpdir, "logs")
            setup_logging(
                log_dir=log_dir,
                log_level="DEBUG",
                max_bytes=1_000_000,
                backup_count=1,
            )
            # Log something
            test_logger = logging.getLogger("test.setup")
            test_logger.info("test message")
            # Check file exists
            assert os.path.exists(os.path.join(log_dir, "bot.log"))
            # Read and verify JSON
            with open(os.path.join(log_dir, "bot.log")) as f:
                lines = f.readlines()
            assert len(lines) >= 1
            data = json.loads(lines[-1])
            assert data["msg"] == "test message"

            # Cleanup: remove handlers we added to avoid leaking into other tests
            root = logging.getLogger()
            for h in root.handlers[:]:
                if hasattr(h, "baseFilename"):
                    root.removeHandler(h)
                    h.close()


class TestTimed:
    def test_logs_duration(self):
        records = []

        class Handler(logging.Handler):
            def emit(self, record):
                records.append(record)

        test_logger = logging.getLogger("test.timed")
        test_logger.addHandler(Handler())
        test_logger.setLevel(logging.DEBUG)

        async def run():
            async with timed(test_logger, "operation done", thread_id=99):
                await asyncio.sleep(0.05)

        asyncio.run(run())
        assert len(records) == 1
        assert records[0].msg == "operation done"
        assert hasattr(records[0], "duration_ms")
        assert records[0].duration_ms >= 40  # at least ~50ms
        assert records[0].thread_id == 99

        test_logger.handlers.clear()

    def test_warn_threshold(self):
        records = []

        class Handler(logging.Handler):
            def emit(self, record):
                records.append(record)

        test_logger = logging.getLogger("test.timed.warn")
        test_logger.addHandler(Handler())
        test_logger.setLevel(logging.DEBUG)

        async def run():
            async with timed(test_logger, "slow op", warn_seconds=0.01, error_seconds=100):
                await asyncio.sleep(0.02)

        asyncio.run(run())
        assert records[0].levelno == logging.WARNING

        test_logger.handlers.clear()
