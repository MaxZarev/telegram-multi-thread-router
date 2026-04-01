"""Entry point: python -m src"""

import asyncio
import logging
import os
import sys
import tempfile
import fcntl

import uvloop
from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from src.bot.dispatcher import build_dispatcher
from src.config import settings
from src.logging_config import setup_logging

setup_logging(
    log_dir=settings.log_dir,
    log_level=settings.log_level,
    max_bytes=settings.log_max_bytes,
    backup_count=settings.log_backup_count,
)
logger = logging.getLogger(__name__)

# PID file lock to prevent duplicate bot instances
_LOCK_FILE = os.path.join(tempfile.gettempdir(), "tg-multi-thread-router.lock")


def _acquire_lock() -> int:
    """Acquire exclusive lock file. Returns fd on success, kills stale process if needed."""
    fd = os.open(_LOCK_FILE, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        # Another instance holds the lock — read its PID and abort
        content = os.read(fd, 64).decode().strip()
        os.close(fd)
        logger.error("Another bot instance is already running (PID %s). Exiting.", content)
        sys.exit(1)

    # Write our PID
    os.ftruncate(fd, 0)
    os.lseek(fd, 0, os.SEEK_SET)
    os.write(fd, str(os.getpid()).encode())
    return fd


async def main() -> None:
    """Initialize bot and start polling."""
    bot = Bot(
        token=settings.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    dp = build_dispatcher()

    logger.info(
        "Starting bot (owner=%d, chat=%s)",
        settings.owner_user_id,
        settings.chat_id or "auto-detect",
    )
    await dp.start_polling(bot)


if __name__ == "__main__":
    lock_fd = _acquire_lock()
    try:
        with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
            runner.run(main())
    finally:
        os.close(lock_fd)
