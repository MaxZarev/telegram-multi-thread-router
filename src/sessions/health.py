"""Background health monitoring for Claude session subprocesses."""

import asyncio
import logging
import time

from aiogram import Bot

from src.db.queries import update_session_state
from src.sessions.manager import SessionManager

logger = logging.getLogger(__name__)


async def health_check_loop(
    manager: SessionManager,
    bot: Bot,
    chat_id: int,
    interval: int = 60,
) -> None:
    """Run forever as a background task. Every `interval` seconds, check for dead runners.

    A runner is considered dead if:
    - Its asyncio task has completed (task.done()) but state is not STOPPED
    - This indicates the ClaudeSDKClient subprocess died unexpectedly

    Dead sessions are stopped, marked in DB, and their topic is notified.
    """
    while True:
        await asyncio.sleep(interval)
        try:
            dead_threads: list[int] = []

            for thread_id, runner in manager.list_all():
                # Check if the runner task died unexpectedly
                if not runner.is_alive and runner.state.name not in ("STOPPED",):
                    dead_threads.append(thread_id)
                    logger.warning(
                        "Zombie detected: thread %d, state=%s, task_alive=%s",
                        thread_id,
                        runner.state.name,
                        runner.is_alive,
                    )

                # Check for stuck sessions (in RUNNING too long)
                turn_started = getattr(runner, "_turn_started_at", None)
                if turn_started is not None and runner.state.name == "RUNNING":
                    from src.config import settings
                    stuck_seconds = time.monotonic() - turn_started
                    last_tool = getattr(runner, "_last_tool_name", None)
                    sdk_msg_count = getattr(runner, "_sdk_msg_count", 0)
                    session_id = getattr(runner, "session_id", None)
                    stuck_ms = int(stuck_seconds * 1000)

                    if stuck_seconds >= settings.turn_error_seconds:
                        logger.error(
                            "Session likely hung thread=%d stuck_duration=%ds last_tool=%s sdk_msgs=%d",
                            thread_id, int(stuck_seconds), last_tool, sdk_msg_count,
                            extra={
                                "thread_id": thread_id,
                                "session_id": session_id,
                                "stuck_duration_ms": stuck_ms,
                                "last_tool": last_tool,
                                "sdk_msg_count": sdk_msg_count,
                            },
                        )
                    elif stuck_seconds >= settings.turn_warn_seconds:
                        logger.warning(
                            "Session stuck in RUNNING thread=%d stuck_duration=%ds last_tool=%s sdk_msgs=%d",
                            thread_id, int(stuck_seconds), last_tool, sdk_msg_count,
                            extra={
                                "thread_id": thread_id,
                                "session_id": session_id,
                                "stuck_duration_ms": stuck_ms,
                                "last_tool": last_tool,
                                "sdk_msg_count": sdk_msg_count,
                            },
                        )

            for thread_id in dead_threads:
                try:
                    await manager.stop(thread_id)
                except Exception as e:
                    logger.error("Error stopping zombie session %d: %s", thread_id, e)

                try:
                    await update_session_state(thread_id, "stopped")
                except Exception as e:
                    logger.error("Error updating DB for zombie %d: %s", thread_id, e)

                try:
                    await bot.send_message(
                        chat_id=chat_id,
                        message_thread_id=thread_id,
                        text="Session terminated: Claude process died unexpectedly.",
                    )
                except Exception as e:
                    logger.error("Error notifying topic %d about zombie: %s", thread_id, e)

            # Summary (DEBUG level — for log file analysis)
            all_sessions = manager.list_all()
            if all_sessions:
                states = {}
                for _, r in all_sessions:
                    s = r.state.name
                    states[s] = states.get(s, 0) + 1
                logger.debug(
                    "Health check: %d sessions, states=%s",
                    len(all_sessions), states,
                )

            if dead_threads:
                logger.info("Health check cleaned up %d zombie session(s)", len(dead_threads))

        except Exception as e:
            logger.error("Health check loop error: %s", e)
