"""SchedulerService — cron-based task scheduler for Telegram provider sessions."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from aiogram import Bot
from aiogram.methods import CreateForumTopic
from croniter import croniter

from src.db import queries as q
from src.sessions.state import SessionState

if TYPE_CHECKING:
    from src.sessions.manager import SessionManager
    from src.sessions.permissions import PermissionManager
    from src.sessions.questions import QuestionManager

logger = logging.getLogger(__name__)


MAX_CONSECUTIVE_FAILURES = 3


@dataclass
class ScheduledTask:
    id: int
    name: str
    cron_expr: str
    prompt: str
    target_thread_id: int | None
    new_session_workdir: str | None
    new_session_server: str
    new_session_provider: str | None
    pinned_thread_id: int | None
    enabled: bool
    last_run_at: datetime | None
    next_run_at: datetime | None
    run_count: int
    consecutive_failures: int


def _row_to_task(row: dict) -> ScheduledTask:
    """Convert a DB row dict to a ScheduledTask dataclass."""

    def _parse_dt(val: str | None) -> datetime | None:
        if not val:
            return None
        # SQLite stores as 'YYYY-MM-DD HH:MM:SS'
        try:
            return datetime.strptime(val, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            return None

    return ScheduledTask(
        id=row["id"],
        name=row["name"],
        cron_expr=row["cron_expr"],
        prompt=row["prompt"],
        target_thread_id=row.get("target_thread_id"),
        new_session_workdir=row.get("new_session_workdir"),
        new_session_server=row.get("new_session_server") or "local",
        new_session_provider=row.get("new_session_provider"),
        pinned_thread_id=row.get("pinned_thread_id"),
        enabled=bool(row.get("enabled", 1)),
        last_run_at=_parse_dt(row.get("last_run_at")),
        next_run_at=_parse_dt(row.get("next_run_at")),
        run_count=row.get("run_count") or 0,
        consecutive_failures=row.get("consecutive_failures") or 0,
    )


def _compute_next_run(cron_expr: str, base: datetime | None = None) -> str:
    """Compute next run datetime string from cron expression."""
    base_dt = base or datetime.now(tz=timezone.utc)
    # croniter works with naive datetimes; pass utcnow equivalent
    cron = croniter(cron_expr, base_dt.replace(tzinfo=None))
    next_dt = cron.get_next(datetime)
    return next_dt.strftime("%Y-%m-%d %H:%M:%S")


class SchedulerService:
    """Runs scheduled tasks on a cron schedule."""

    TICK_INTERVAL = 60  # seconds

    def __init__(
        self,
        session_manager: SessionManager,
        bot: Bot,
        chat_id: int,
        permission_manager: PermissionManager,
        question_manager: QuestionManager | None = None,
    ) -> None:
        self._session_manager = session_manager
        self._bot = bot
        self._chat_id = chat_id
        self._permission_manager = permission_manager
        self._question_manager = question_manager
        self._task: asyncio.Task | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Load tasks from DB and start the tick loop."""
        logger.info("SchedulerService starting")
        # Ensure all enabled tasks have next_run_at set
        await self._initialize_next_runs()
        self._task = asyncio.create_task(self._loop(), name="scheduler_tick")

    async def stop(self) -> None:
        """Stop the tick loop."""
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("SchedulerService stopped")

    # ------------------------------------------------------------------
    # Internal loop
    # ------------------------------------------------------------------

    async def _loop(self) -> None:
        """Main loop: tick every TICK_INTERVAL seconds."""
        logger.info("Scheduler tick loop started (interval=%ds)", self.TICK_INTERVAL)
        while True:
            try:
                await self._tick()
            except Exception:
                logger.exception("Scheduler tick error")
            await asyncio.sleep(self.TICK_INTERVAL)

    async def _initialize_next_runs(self) -> None:
        """For enabled tasks without next_run_at, compute and save it."""
        rows = await q.get_all_scheduled_tasks()
        for row in rows:
            if row.get("enabled") and not row.get("next_run_at"):
                next_run = _compute_next_run(row["cron_expr"])
                await q.update_scheduled_task(row["id"], next_run_at=next_run)
                logger.info(
                    "Initialized next_run_at for task id=%d name=%r next=%s",
                    row["id"], row["name"], next_run,
                )

    async def _tick(self) -> None:
        """Query due tasks and execute each one."""
        due_rows = await q.get_due_scheduled_tasks()
        if not due_rows:
            return

        logger.info("Scheduler tick: %d due task(s)", len(due_rows))
        for row in due_rows:
            task = _row_to_task(row)
            try:
                await self._execute_task(task)
            except Exception:
                logger.exception("Error executing scheduled task id=%d name=%r", task.id, task.name)
                await self._record_failure(task)

    async def _execute_task(self, task: ScheduledTask) -> None:
        """Execute a single scheduled task and update its run tracking."""
        logger.info(
            "Executing scheduled task id=%d name=%r run_count=%d",
            task.id, task.name, task.run_count,
        )

        try:
            if task.target_thread_id is not None:
                await self._execute_existing(task)
            else:
                await self._execute_fresh(task)
        except Exception:
            logger.exception("Task id=%d name=%r execution failed", task.id, task.name)
            await self._record_failure(task)
            return

        # Success — reset failure counter and update run tracking
        now_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        next_run = _compute_next_run(task.cron_expr)
        new_count = task.run_count + 1
        await q.update_scheduled_task_run(
            task_id=task.id,
            last_run_at=now_str,
            next_run_at=next_run,
            run_count=new_count,
        )
        if task.consecutive_failures > 0:
            await q.update_scheduled_task(task.id, consecutive_failures=0)
        logger.info(
            "Task id=%d name=%r updated: run_count=%d next_run=%s",
            task.id, task.name, new_count, next_run,
        )

    async def _record_failure(self, task: ScheduledTask) -> None:
        """Increment failure counter, auto-pause after MAX_CONSECUTIVE_FAILURES, advance next_run."""
        failures = task.consecutive_failures + 1
        now_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        next_run = _compute_next_run(task.cron_expr)

        await q.update_scheduled_task(task.id, consecutive_failures=failures)
        await q.update_scheduled_task_run(
            task_id=task.id,
            last_run_at=now_str,
            next_run_at=next_run,
            run_count=task.run_count + 1,
        )

        if failures >= MAX_CONSECUTIVE_FAILURES:
            await q.set_scheduled_task_enabled(task.id, False)
            logger.warning(
                "Task id=%d name=%r auto-paused after %d consecutive failures",
                task.id, task.name, failures,
            )
            # Notify in Telegram
            thread_id = task.pinned_thread_id or task.target_thread_id
            if thread_id:
                try:
                    await self._bot.send_message(
                        chat_id=self._chat_id,
                        message_thread_id=thread_id,
                        text=(
                            f"⚠️ Schedule '{task.name}' auto-paused after "
                            f"{failures} consecutive failures. "
                            f"Use resume_schedule({task.id}) to re-enable."
                        ),
                    )
                except Exception:
                    logger.exception("Failed to send auto-pause notification")

    async def _execute_existing(self, task: ScheduledTask) -> None:
        """Send prompt to an existing session (target_thread_id mode)."""
        thread_id = task.target_thread_id
        runner = self._session_manager.get(thread_id)

        if runner is None:
            logger.warning(
                "Scheduler: no runner for thread_id=%d (task id=%d), skipping",
                thread_id, task.id,
            )
            return

        state = runner.state
        if state in (SessionState.RUNNING, SessionState.WAITING_PERMISSION):
            logger.info(
                "Scheduler: task id=%d skipped — runner thread=%d is %s",
                task.id, thread_id, state.name,
            )
            return

        if state == SessionState.IDLE:
            await runner.enqueue(task.prompt)
            logger.info(
                "Scheduler: enqueued prompt to thread=%d (task id=%d)",
                thread_id, task.id,
            )
        else:
            logger.info(
                "Scheduler: task id=%d skipped — runner thread=%d is %s",
                task.id, thread_id, state.name,
            )

    async def _execute_fresh(self, task: ScheduledTask) -> None:
        """Create/reuse pinned thread, stop+clear existing session, create fresh session."""
        run_number = task.run_count + 1
        now_utc = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        header = f"⏰ {task.name} — Run #{run_number} — {now_utc} UTC"

        thread_id = task.pinned_thread_id

        if thread_id is None:
            # Create a new pinned Telegram thread
            topic = await self._bot(
                CreateForumTopic(chat_id=self._chat_id, name=f"⏰ {task.name}")
            )
            thread_id = topic.message_thread_id
            # Persist to DB
            await q.insert_topic(thread_id, f"⏰ {task.name}")
            await q.update_scheduled_task_pinned_thread(task.id, thread_id)
            logger.info(
                "Scheduler: created pinned thread=%d for task id=%d name=%r",
                thread_id, task.id, task.name,
            )
        else:
            # Stop existing session in this thread (if any) and clear session_id for fresh start
            existing = self._session_manager.get(thread_id)
            if existing is not None:
                try:
                    await self._session_manager.stop(thread_id)
                except Exception:
                    logger.exception("Failed to stop existing session thread=%d", thread_id)
            await q.clear_session_id(thread_id)

        # Insert DB session record (upsert-safe: clear_session_id does UPDATE, insert if new)
        try:
            await q.insert_session(
                thread_id=thread_id,
                workdir=task.new_session_workdir,
                server=task.new_session_server or "local",
                provider=task.new_session_provider,
            )
        except Exception as e:
            # Row may already exist (pinned thread from previous run) — IntegrityError is expected
            logger.debug("insert_session for thread=%d (expected if exists): %s", thread_id, e)

        # Create fresh session runner
        runner = await self._session_manager.create(
            thread_id=thread_id,
            workdir=task.new_session_workdir,
            bot=self._bot,
            chat_id=self._chat_id,
            permission_manager=self._permission_manager,
            provider=task.new_session_provider,
        )
        # Fresh scheduled sessions auto-approve all permissions
        runner.auto_mode = True

        # Send header message before enqueue so the user sees which run this is
        await self._bot.send_message(
            chat_id=self._chat_id,
            message_thread_id=thread_id,
            text=header,
        )

        await runner.enqueue(task.prompt)
        logger.info(
            "Scheduler: fresh session started thread=%d task id=%d run=%d",
            thread_id, task.id, run_number,
        )

    # ------------------------------------------------------------------
    # CRUD methods (for MCP tools)
    # ------------------------------------------------------------------

    async def create_task(
        self,
        name: str,
        cron_expr: str,
        prompt: str,
        target_thread_id: int | None = None,
        new_session_workdir: str | None = None,
        new_session_server: str = "local",
        new_session_provider: str | None = None,
    ) -> ScheduledTask:
        """Create a new scheduled task. Validates cron expression and target."""
        if not croniter.is_valid(cron_expr):
            raise ValueError(f"Invalid cron expression: {cron_expr!r}")
        if target_thread_id is None and new_session_workdir is None:
            raise ValueError(
                "Either target_thread_id or new_session_workdir must be set"
            )
        # If workdir is set, it's fresh mode — ignore target_thread_id
        if new_session_workdir is not None:
            target_thread_id = None

        next_run = _compute_next_run(cron_expr)
        task_id = await q.insert_scheduled_task(
            name=name,
            cron_expr=cron_expr,
            prompt=prompt,
            target_thread_id=target_thread_id,
            new_session_workdir=new_session_workdir,
            new_session_server=new_session_server,
            new_session_provider=new_session_provider,
        )
        # Save next_run_at immediately
        await q.update_scheduled_task(task_id, next_run_at=next_run)

        rows = await q.get_all_scheduled_tasks()
        for row in rows:
            if row["id"] == task_id:
                return _row_to_task(row)
        raise RuntimeError(f"Could not find task id={task_id} after insert")

    async def update_task(self, task_id: int, **kwargs) -> ScheduledTask | None:
        """Update fields of a scheduled task. Returns updated task or None if not found."""
        if "cron_expr" in kwargs and not croniter.is_valid(kwargs["cron_expr"]):
            raise ValueError(f"Invalid cron expression: {kwargs['cron_expr']!r}")
        # Recompute next_run_at if cron changed
        if "cron_expr" in kwargs and "next_run_at" not in kwargs:
            kwargs["next_run_at"] = _compute_next_run(kwargs["cron_expr"])
        await q.update_scheduled_task(task_id, **kwargs)
        # Return updated task
        tasks = await q.get_all_scheduled_tasks()
        for row in tasks:
            if row["id"] == task_id:
                return _row_to_task(row)
        return None

    async def delete_task(self, task_id: int, cleanup_thread: bool = True) -> ScheduledTask | None:
        """Delete a scheduled task by ID. Returns the deleted task or None if not found.

        If cleanup_thread=True and the task had a pinned thread, stops the session
        and deletes the thread.
        """
        tasks = await q.get_all_scheduled_tasks()
        task = None
        for t in tasks:
            if t["id"] == task_id:
                task = _row_to_task(t)
                break
        if task is None:
            return None

        await q.delete_scheduled_task(task_id)

        if cleanup_thread and task.pinned_thread_id:
            # Stop session if running in pinned thread
            if self._session_manager.get(task.pinned_thread_id):
                try:
                    await self._session_manager.stop(task.pinned_thread_id)
                except Exception:
                    logger.exception("Failed to stop session in pinned thread %d", task.pinned_thread_id)
            # Delete DB records and forum topic
            try:
                await q.delete_session_and_topic(task.pinned_thread_id)
            except Exception:
                pass
            try:
                await self._bot.delete_forum_topic(
                    chat_id=self._chat_id,
                    message_thread_id=task.pinned_thread_id,
                )
            except Exception:
                try:
                    await self._bot.close_forum_topic(
                        chat_id=self._chat_id,
                        message_thread_id=task.pinned_thread_id,
                    )
                except Exception:
                    pass

        return task

    async def set_task_enabled(self, task_id: int, enabled: bool) -> ScheduledTask | None:
        """Enable or disable a scheduled task. Returns updated task or None if not found."""
        await q.set_scheduled_task_enabled(task_id, enabled)
        if enabled:
            # Recompute next_run_at when re-enabling
            rows = await q.get_all_scheduled_tasks()
            for row in rows:
                if row["id"] == task_id:
                    if not row.get("next_run_at"):
                        next_run = _compute_next_run(row["cron_expr"])
                        await q.update_scheduled_task(task_id, next_run_at=next_run)
                    break
        # Return updated task
        tasks = await q.get_all_scheduled_tasks()
        for row in tasks:
            if row["id"] == task_id:
                return _row_to_task(row)
        return None

    async def list_tasks(self) -> list[ScheduledTask]:
        """Return all scheduled tasks."""
        rows = await q.get_all_scheduled_tasks()
        return [_row_to_task(row) for row in rows]
