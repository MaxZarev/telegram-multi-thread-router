# Scheduled Tasks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add cron-based scheduled tasks that execute prompts in existing or fresh provider sessions, managed via orchestrator MCP tools.

**Architecture:** New `SchedulerService` with asyncio tick loop (60s) + `croniter` for cron parsing. Tasks persisted in SQLite `scheduled_tasks` table. Six new MCP tools on the orchestrator for CRUD + pause/resume. Fresh-mode tasks get a pinned Telegram thread that is reused across runs with session_id cleared each time.

**Tech Stack:** croniter (cron parsing), aiosqlite (persistence), aiogram (Telegram topics), asyncio (tick loop)

---

### File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `src/sessions/scheduler.py` | Create | SchedulerService + ScheduledTask dataclass |
| `src/db/schema.py` | Modify | Add `scheduled_tasks` table migration |
| `src/db/queries.py` | Modify | Add scheduled task query functions |
| `src/sessions/orchestrator.py` | Modify | Add 6 MCP tools + update system prompt |
| `src/bot/dispatcher.py` | Modify | Start/stop SchedulerService in lifecycle |
| `pyproject.toml` | Modify | Add `croniter` dependency |
| `tests/test_scheduler.py` | Create | Tests for SchedulerService |

---

### Task 1: Add croniter dependency

**Files:**
- Modify: `pyproject.toml:7` (dependencies list)

- [ ] **Step 1: Add croniter to dependencies**

In `pyproject.toml`, add `croniter` to the dependencies list:

```toml
dependencies = [
    "aiogram>=3.26.0,<4",
    "aiosqlite>=0.22.0,<1",
    "claude-agent-sdk>=0.1.50",
    "croniter>=6.0.0,<7",
    "uvloop>=0.22.0,<1",
    "pydantic-settings>=2.13.0,<3",
    "faster-whisper>=1.1.0",
    "deepgram-sdk>=4.0.0",
    "aiohttp>=3.9.0,<4",
    "msgspec>=0.18.0",
    "mcp>=1.22.0",
    "uvicorn>=0.35.0,<1",
]
```

- [ ] **Step 2: Install the dependency**

Run: `cd /Users/zarev/claude/telegram-multi-thread-router && uv sync`
Expected: croniter installed successfully

- [ ] **Step 3: Verify import works**

Run: `cd /Users/zarev/claude/telegram-multi-thread-router && python -c "from croniter import croniter; print('ok')"`
Expected: `ok`

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "feat(scheduler): add croniter dependency"
```

---

### Task 2: Database schema — scheduled_tasks table

**Files:**
- Modify: `src/db/schema.py:72-97` (migrations list)
- Modify: `src/db/queries.py` (add new query functions at bottom)
- Test: `tests/test_scheduler.py`

- [ ] **Step 1: Write failing test for scheduled_tasks table**

Create `tests/test_scheduler.py`:

```python
"""Tests for scheduled tasks database and scheduler service."""

import pytest
from pathlib import Path

import aiosqlite

from src.db.schema import init_db
from src.db.connection import get_connection


@pytest.fixture
async def tmp_db(tmp_path):
    """Create a temporary database and initialize it."""
    db_path = tmp_path / "test.db"
    await init_db(db_path)
    return db_path


async def test_scheduled_tasks_table_exists(tmp_db):
    """scheduled_tasks table has correct columns after migration."""
    async with aiosqlite.connect(str(tmp_db)) as conn:
        cursor = await conn.execute("PRAGMA table_info(scheduled_tasks);")
        columns = {row[1] for row in await cursor.fetchall()}
        assert columns == {
            "id",
            "name",
            "cron_expr",
            "prompt",
            "target_thread_id",
            "new_session_workdir",
            "new_session_server",
            "new_session_provider",
            "pinned_thread_id",
            "enabled",
            "last_run_at",
            "next_run_at",
            "run_count",
            "created_at",
        }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/zarev/claude/telegram-multi-thread-router && python -m pytest tests/test_scheduler.py::test_scheduled_tasks_table_exists -v`
Expected: FAIL — table does not exist

- [ ] **Step 3: Add migration to schema.py**

In `src/db/schema.py`, add after the existing migrations list (after line 79, inside the `for migration in [...]` block), add a new SQL statement that creates the table. Since the migration pattern uses ALTER TABLE which can't CREATE TABLE, add the CREATE TABLE after the migration loop (after line 97, before `await conn.commit()`):

```python
        # Create scheduled_tasks table (idempotent)
        await conn.executescript("""
            CREATE TABLE IF NOT EXISTS scheduled_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                cron_expr TEXT NOT NULL,
                prompt TEXT NOT NULL,
                target_thread_id INTEGER,
                new_session_workdir TEXT,
                new_session_server TEXT NOT NULL DEFAULT 'local',
                new_session_provider TEXT,
                pinned_thread_id INTEGER,
                enabled INTEGER NOT NULL DEFAULT 1,
                last_run_at TEXT,
                next_run_at TEXT,
                run_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
        """)
```

Place this right before `await conn.commit()` on line 97.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/zarev/claude/telegram-multi-thread-router && python -m pytest tests/test_scheduler.py::test_scheduled_tasks_table_exists -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/db/schema.py tests/test_scheduler.py
git commit -m "feat(scheduler): add scheduled_tasks table migration"
```

---

### Task 3: Database query functions for scheduled tasks

**Files:**
- Modify: `src/db/queries.py` (add functions at bottom)
- Test: `tests/test_scheduler.py`

- [ ] **Step 1: Write failing tests for CRUD queries**

Append to `tests/test_scheduler.py`:

```python
from src.db import queries as q


async def test_insert_and_get_scheduled_tasks(tmp_db):
    """Insert a scheduled task and retrieve it."""
    # Monkeypatch DB_PATH for queries that use get_connection
    import src.db.schema as schema_mod
    import src.db.connection as conn_mod
    original = conn_mod.DB_PATH
    schema_mod.DB_PATH = tmp_db
    conn_mod.DB_PATH = tmp_db
    try:
        task_id = await q.insert_scheduled_task(
            name="daily check",
            cron_expr="0 9 * * *",
            prompt="check logs",
            target_thread_id=42,
        )
        assert task_id > 0

        tasks = await q.get_all_scheduled_tasks()
        assert len(tasks) == 1
        assert tasks[0]["name"] == "daily check"
        assert tasks[0]["cron_expr"] == "0 9 * * *"
        assert tasks[0]["target_thread_id"] == 42
        assert tasks[0]["enabled"] == 1
    finally:
        schema_mod.DB_PATH = original
        conn_mod.DB_PATH = original


async def test_update_scheduled_task(tmp_db):
    """Update fields of a scheduled task."""
    import src.db.schema as schema_mod
    import src.db.connection as conn_mod
    original = conn_mod.DB_PATH
    schema_mod.DB_PATH = tmp_db
    conn_mod.DB_PATH = tmp_db
    try:
        task_id = await q.insert_scheduled_task(
            name="test",
            cron_expr="0 9 * * *",
            prompt="hello",
        )
        await q.update_scheduled_task(task_id, name="updated", prompt="world")
        tasks = await q.get_all_scheduled_tasks()
        assert tasks[0]["name"] == "updated"
        assert tasks[0]["prompt"] == "world"
    finally:
        schema_mod.DB_PATH = original
        conn_mod.DB_PATH = original


async def test_delete_scheduled_task(tmp_db):
    """Delete a scheduled task."""
    import src.db.schema as schema_mod
    import src.db.connection as conn_mod
    original = conn_mod.DB_PATH
    schema_mod.DB_PATH = tmp_db
    conn_mod.DB_PATH = tmp_db
    try:
        task_id = await q.insert_scheduled_task(
            name="temp",
            cron_expr="0 9 * * *",
            prompt="x",
        )
        await q.delete_scheduled_task(task_id)
        tasks = await q.get_all_scheduled_tasks()
        assert len(tasks) == 0
    finally:
        schema_mod.DB_PATH = original
        conn_mod.DB_PATH = original


async def test_set_scheduled_task_enabled(tmp_db):
    """Pause and resume a scheduled task."""
    import src.db.schema as schema_mod
    import src.db.connection as conn_mod
    original = conn_mod.DB_PATH
    schema_mod.DB_PATH = tmp_db
    conn_mod.DB_PATH = tmp_db
    try:
        task_id = await q.insert_scheduled_task(
            name="test",
            cron_expr="0 9 * * *",
            prompt="x",
        )
        await q.set_scheduled_task_enabled(task_id, False)
        tasks = await q.get_all_scheduled_tasks()
        assert tasks[0]["enabled"] == 0

        await q.set_scheduled_task_enabled(task_id, True)
        tasks = await q.get_all_scheduled_tasks()
        assert tasks[0]["enabled"] == 1
    finally:
        schema_mod.DB_PATH = original
        conn_mod.DB_PATH = original


async def test_get_due_scheduled_tasks(tmp_db):
    """get_due_scheduled_tasks returns only enabled tasks with next_run_at <= now."""
    import src.db.schema as schema_mod
    import src.db.connection as conn_mod
    original = conn_mod.DB_PATH
    schema_mod.DB_PATH = tmp_db
    conn_mod.DB_PATH = tmp_db
    try:
        task_id = await q.insert_scheduled_task(
            name="due",
            cron_expr="0 9 * * *",
            prompt="x",
        )
        # Set next_run_at to the past
        await q.update_scheduled_task(task_id, next_run_at="2020-01-01 00:00:00")

        disabled_id = await q.insert_scheduled_task(
            name="disabled",
            cron_expr="0 9 * * *",
            prompt="x",
        )
        await q.update_scheduled_task(disabled_id, next_run_at="2020-01-01 00:00:00")
        await q.set_scheduled_task_enabled(disabled_id, False)

        due = await q.get_due_scheduled_tasks()
        assert len(due) == 1
        assert due[0]["name"] == "due"
    finally:
        schema_mod.DB_PATH = original
        conn_mod.DB_PATH = original
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/zarev/claude/telegram-multi-thread-router && python -m pytest tests/test_scheduler.py -v -k "not table_exists"`
Expected: FAIL — functions not defined

- [ ] **Step 3: Implement query functions**

Append to `src/db/queries.py`:

```python
# ---- Scheduled tasks ----

async def insert_scheduled_task(
    name: str,
    cron_expr: str,
    prompt: str,
    target_thread_id: int | None = None,
    new_session_workdir: str | None = None,
    new_session_server: str = "local",
    new_session_provider: str | None = None,
) -> int:
    """Insert a new scheduled task and return its ID."""
    async with get_connection() as conn:
        cursor = await conn.execute(
            "INSERT INTO scheduled_tasks "
            "(name, cron_expr, prompt, target_thread_id, "
            "new_session_workdir, new_session_server, new_session_provider) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (name, cron_expr, prompt, target_thread_id,
             new_session_workdir, new_session_server, new_session_provider),
        )
        await conn.commit()
        return cursor.lastrowid


async def get_all_scheduled_tasks() -> list[dict]:
    """Return all scheduled tasks."""
    async with get_connection() as conn:
        cursor = await conn.execute(
            "SELECT * FROM scheduled_tasks ORDER BY id"
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def get_due_scheduled_tasks() -> list[dict]:
    """Return enabled tasks with next_run_at <= now."""
    async with get_connection() as conn:
        cursor = await conn.execute(
            "SELECT * FROM scheduled_tasks "
            "WHERE enabled=1 AND next_run_at IS NOT NULL "
            "AND next_run_at <= datetime('now') "
            "ORDER BY next_run_at"
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def update_scheduled_task(task_id: int, **kwargs) -> None:
    """Update fields of a scheduled task. Pass only the fields to change."""
    if not kwargs:
        return
    set_parts = []
    values = []
    for key, value in kwargs.items():
        set_parts.append(f"{key}=?")
        values.append(value)
    values.append(task_id)
    async with get_connection() as conn:
        await conn.execute(
            f"UPDATE scheduled_tasks SET {', '.join(set_parts)} WHERE id=?",
            values,
        )
        await conn.commit()


async def delete_scheduled_task(task_id: int) -> None:
    """Delete a scheduled task by ID."""
    async with get_connection() as conn:
        await conn.execute("DELETE FROM scheduled_tasks WHERE id=?", (task_id,))
        await conn.commit()


async def set_scheduled_task_enabled(task_id: int, enabled: bool) -> None:
    """Enable or disable a scheduled task."""
    async with get_connection() as conn:
        await conn.execute(
            "UPDATE scheduled_tasks SET enabled=? WHERE id=?",
            (int(enabled), task_id),
        )
        await conn.commit()


async def update_scheduled_task_run(
    task_id: int, last_run_at: str, next_run_at: str, run_count: int
) -> None:
    """Update run tracking fields after a task executes."""
    async with get_connection() as conn:
        await conn.execute(
            "UPDATE scheduled_tasks SET last_run_at=?, next_run_at=?, run_count=? WHERE id=?",
            (last_run_at, next_run_at, run_count, task_id),
        )
        await conn.commit()


async def update_scheduled_task_pinned_thread(task_id: int, thread_id: int) -> None:
    """Save the auto-created pinned thread for a fresh-mode task."""
    async with get_connection() as conn:
        await conn.execute(
            "UPDATE scheduled_tasks SET pinned_thread_id=? WHERE id=?",
            (thread_id, task_id),
        )
        await conn.commit()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/zarev/claude/telegram-multi-thread-router && python -m pytest tests/test_scheduler.py -v`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add src/db/queries.py tests/test_scheduler.py
git commit -m "feat(scheduler): add scheduled task DB query functions"
```

---

### Task 4: SchedulerService — core tick loop

**Files:**
- Create: `src/sessions/scheduler.py`
- Test: `tests/test_scheduler.py`

- [ ] **Step 1: Write failing test for SchedulerService tick**

Append to `tests/test_scheduler.py`:

```python
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from dataclasses import dataclass

from src.sessions.scheduler import SchedulerService, ScheduledTask
from src.sessions.state import SessionState


def _make_scheduler(tmp_db):
    """Create a SchedulerService with mocked dependencies."""
    import src.db.schema as schema_mod
    import src.db.connection as conn_mod
    schema_mod.DB_PATH = tmp_db
    conn_mod.DB_PATH = tmp_db

    manager = MagicMock()
    manager.get = MagicMock(return_value=None)
    manager.create = AsyncMock()
    bot = AsyncMock()
    bot.send_message = AsyncMock()
    # Mock CreateForumTopic response
    topic_response = MagicMock()
    topic_response.message_thread_id = 77777
    bot.__call__ = AsyncMock(return_value=topic_response)
    permission_manager = MagicMock()
    question_manager = MagicMock()

    svc = SchedulerService(
        session_manager=manager,
        bot=bot,
        chat_id=-100999,
        permission_manager=permission_manager,
        question_manager=question_manager,
    )
    return svc, manager, bot


async def test_scheduler_tick_existing_idle(tmp_db):
    """Tick enqueues prompt into an existing IDLE session."""
    svc, manager, bot = _make_scheduler(tmp_db)

    # Create a task targeting an existing thread
    task_id = await q.insert_scheduled_task(
        name="test",
        cron_expr="* * * * *",
        prompt="do stuff",
        target_thread_id=42,
    )
    await q.update_scheduled_task(task_id, next_run_at="2020-01-01 00:00:00")

    # Mock existing session as IDLE
    runner = AsyncMock()
    runner.state = SessionState.IDLE
    runner.enqueue = AsyncMock()
    manager.get = MagicMock(return_value=runner)

    await svc.load_tasks()
    await svc._tick()

    runner.enqueue.assert_called_once_with("do stuff")


async def test_scheduler_tick_existing_running_skips(tmp_db):
    """Tick skips a task if target session is RUNNING."""
    svc, manager, bot = _make_scheduler(tmp_db)

    task_id = await q.insert_scheduled_task(
        name="busy",
        cron_expr="* * * * *",
        prompt="do stuff",
        target_thread_id=42,
    )
    await q.update_scheduled_task(task_id, next_run_at="2020-01-01 00:00:00")

    runner = AsyncMock()
    runner.state = SessionState.RUNNING
    runner.enqueue = AsyncMock()
    manager.get = MagicMock(return_value=runner)

    await svc.load_tasks()
    await svc._tick()

    runner.enqueue.assert_not_called()


async def test_scheduler_tick_fresh_creates_pinned_thread(tmp_db):
    """Fresh-mode tick creates a pinned thread on first run."""
    svc, manager, bot = _make_scheduler(tmp_db)

    task_id = await q.insert_scheduled_task(
        name="fresh-task",
        cron_expr="* * * * *",
        prompt="deploy",
        new_session_workdir="/home/deploy",
    )
    await q.update_scheduled_task(task_id, next_run_at="2020-01-01 00:00:00")

    # Mock session creation
    new_runner = AsyncMock()
    new_runner.state = SessionState.IDLE
    new_runner.auto_mode = False
    manager.create = AsyncMock(return_value=new_runner)
    manager.get = MagicMock(return_value=None)

    await svc.load_tasks()
    await svc._tick()

    # Should have created a forum topic
    bot.__call__.assert_called_once()
    # Should have persisted pinned_thread_id
    tasks = await q.get_all_scheduled_tasks()
    assert tasks[0]["pinned_thread_id"] == 77777
    # Should have enqueued the prompt
    new_runner.enqueue.assert_called_once_with("deploy")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/zarev/claude/telegram-multi-thread-router && python -m pytest tests/test_scheduler.py -v -k "scheduler_tick"`
Expected: FAIL — module not found

- [ ] **Step 3: Implement SchedulerService**

Create `src/sessions/scheduler.py`:

```python
"""SchedulerService — cron-based task scheduler for provider sessions."""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from croniter import croniter

logger = logging.getLogger(__name__)

_TICK_INTERVAL = 60  # seconds


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


def _row_to_task(row: dict) -> ScheduledTask:
    """Convert a DB row dict to a ScheduledTask dataclass."""
    return ScheduledTask(
        id=row["id"],
        name=row["name"],
        cron_expr=row["cron_expr"],
        prompt=row["prompt"],
        target_thread_id=row["target_thread_id"],
        new_session_workdir=row["new_session_workdir"],
        new_session_server=row["new_session_server"],
        new_session_provider=row["new_session_provider"],
        pinned_thread_id=row["pinned_thread_id"],
        enabled=bool(row["enabled"]),
        last_run_at=row["last_run_at"],
        next_run_at=row["next_run_at"],
        run_count=row["run_count"],
    )


def _compute_next_run(cron_expr: str) -> str:
    """Compute the next run time as ISO string from a cron expression."""
    now = datetime.now(timezone.utc)
    cron = croniter(cron_expr, now)
    next_dt = cron.get_next(datetime)
    return next_dt.strftime("%Y-%m-%d %H:%M:%S")


class SchedulerService:
    """Manages cron-scheduled tasks that execute in provider sessions."""

    def __init__(self, session_manager, bot, chat_id: int, permission_manager, question_manager):
        self._session_manager = session_manager
        self._bot = bot
        self._chat_id = chat_id
        self._permission_manager = permission_manager
        self._question_manager = question_manager
        self._tasks: dict[int, ScheduledTask] = {}
        self._tick_task: asyncio.Task | None = None

    async def start(self) -> None:
        """Load tasks from DB and start the tick loop."""
        await self.load_tasks()
        self._tick_task = asyncio.create_task(self._tick_loop())
        logger.info("Scheduler started with %d task(s)", len(self._tasks))

    async def stop(self) -> None:
        """Cancel the tick loop."""
        if self._tick_task:
            self._tick_task.cancel()
            try:
                await self._tick_task
            except asyncio.CancelledError:
                pass
            self._tick_task = None
        logger.info("Scheduler stopped")

    async def load_tasks(self) -> None:
        """Load all tasks from DB into memory and ensure next_run_at is set."""
        from src.db.queries import get_all_scheduled_tasks, update_scheduled_task

        rows = await get_all_scheduled_tasks()
        self._tasks.clear()
        for row in rows:
            task = _row_to_task(row)
            if task.enabled and task.next_run_at is None:
                task.next_run_at = _compute_next_run(task.cron_expr)
                await update_scheduled_task(task.id, next_run_at=task.next_run_at)
            self._tasks[task.id] = task

    async def _tick_loop(self) -> None:
        """Run _tick() every _TICK_INTERVAL seconds."""
        while True:
            await asyncio.sleep(_TICK_INTERVAL)
            try:
                await self._tick()
            except Exception:
                logger.exception("Scheduler tick error")

    async def _tick(self) -> None:
        """Check for due tasks and execute them."""
        from src.db.queries import get_due_scheduled_tasks

        due_rows = await get_due_scheduled_tasks()
        for row in due_rows:
            task = self._tasks.get(row["id"])
            if task is None:
                task = _row_to_task(row)
                self._tasks[task.id] = task

            try:
                if task.target_thread_id is not None:
                    executed = await self._execute_existing(task)
                    if not executed:
                        continue  # RUNNING, retry next tick
                else:
                    await self._execute_fresh(task)

                # Update run tracking
                now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                next_run = _compute_next_run(task.cron_expr)
                task.last_run_at = now_str
                task.next_run_at = next_run
                task.run_count += 1

                from src.db.queries import update_scheduled_task_run
                await update_scheduled_task_run(
                    task.id, now_str, next_run, task.run_count
                )
            except Exception:
                logger.exception("Error executing scheduled task %d (%s)", task.id, task.name)

    async def _execute_existing(self, task: ScheduledTask) -> bool:
        """Enqueue prompt into an existing session. Returns False if RUNNING (skip)."""
        from src.sessions.state import SessionState

        runner = self._session_manager.get(task.target_thread_id)
        if runner is None:
            logger.warning(
                "Scheduled task %d: target session %d not found, skipping",
                task.id, task.target_thread_id,
            )
            return True  # Don't retry — session doesn't exist

        if runner.state in (SessionState.RUNNING, SessionState.WAITING_PERMISSION):
            logger.info(
                "Scheduled task %d: session %d is %s, deferring to next tick",
                task.id, task.target_thread_id, runner.state.name,
            )
            return False

        await self._send_run_header(task.target_thread_id, task)
        await runner.enqueue(task.prompt)
        logger.info("Scheduled task %d: enqueued into session %d", task.id, task.target_thread_id)
        return True

    async def _execute_fresh(self, task: ScheduledTask) -> None:
        """Execute in fresh mode: reuse pinned thread, clear session, create new, enqueue."""
        from aiogram.methods import CreateForumTopic
        from src.db.queries import (
            insert_topic,
            insert_session,
            clear_session_id,
            update_scheduled_task_pinned_thread,
        )

        thread_id = task.pinned_thread_id

        # Create pinned thread on first run
        if thread_id is None:
            topic = await self._bot(
                CreateForumTopic(chat_id=self._chat_id, name=f"\u23f0 {task.name}")
            )
            thread_id = topic.message_thread_id
            task.pinned_thread_id = thread_id
            await insert_topic(thread_id, f"\u23f0 {task.name}")
            await insert_session(
                thread_id,
                task.new_session_workdir or "/tmp",
                server=task.new_session_server,
                provider=task.new_session_provider,
            )
            await update_scheduled_task_pinned_thread(task.id, thread_id)
            logger.info("Scheduled task %d: created pinned thread %d", task.id, thread_id)
        else:
            # Stop existing session and clear for fresh context
            existing = self._session_manager.get(thread_id)
            if existing is not None:
                if existing.state in (
                    __import__("src.sessions.state", fromlist=["SessionState"]).SessionState.RUNNING,
                    __import__("src.sessions.state", fromlist=["SessionState"]).SessionState.WAITING_PERMISSION,
                ):
                    logger.info(
                        "Scheduled task %d: pinned session %d still running, deferring",
                        task.id, thread_id,
                    )
                    return
                await self._session_manager.stop(thread_id)
            await clear_session_id(thread_id)

        # Create fresh session
        runner = self._session_manager.get(thread_id)
        if runner is None:
            runner = await self._session_manager.create(
                thread_id=thread_id,
                workdir=task.new_session_workdir or "/tmp",
                bot=self._bot,
                chat_id=self._chat_id,
                permission_manager=self._permission_manager,
                provider=task.new_session_provider,
            )
            runner.auto_mode = True

        await self._send_run_header(thread_id, task)
        await runner.enqueue(task.prompt)
        logger.info("Scheduled task %d: fresh run #%d in thread %d", task.id, task.run_count + 1, thread_id)

    async def _send_run_header(self, thread_id: int, task: ScheduledTask) -> None:
        """Send a visible header message before each cron run."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
        try:
            await self._bot.send_message(
                chat_id=self._chat_id,
                message_thread_id=thread_id,
                text=f"\u23f0 {task.name} — Run #{task.run_count + 1} — {now} UTC",
            )
        except Exception:
            logger.warning("Failed to send run header for task %d", task.id)

    # --- CRUD methods (called from MCP tools) ---

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
        """Create a new scheduled task, persist it, and add to in-memory cache."""
        if not croniter.is_valid(cron_expr):
            raise ValueError(f"Invalid cron expression: {cron_expr}")
        if target_thread_id is None and new_session_workdir is None:
            raise ValueError("Either target_thread_id or new_session_workdir must be set")

        from src.db.queries import insert_scheduled_task, update_scheduled_task

        task_id = await insert_scheduled_task(
            name=name,
            cron_expr=cron_expr,
            prompt=prompt,
            target_thread_id=target_thread_id,
            new_session_workdir=new_session_workdir,
            new_session_server=new_session_server,
            new_session_provider=new_session_provider,
        )
        next_run = _compute_next_run(cron_expr)
        await update_scheduled_task(task_id, next_run_at=next_run)

        task = ScheduledTask(
            id=task_id,
            name=name,
            cron_expr=cron_expr,
            prompt=prompt,
            target_thread_id=target_thread_id,
            new_session_workdir=new_session_workdir,
            new_session_server=new_session_server,
            new_session_provider=new_session_provider,
            pinned_thread_id=None,
            enabled=True,
            last_run_at=None,
            next_run_at=next_run,
            run_count=0,
        )
        self._tasks[task_id] = task
        return task

    async def update_task(self, task_id: int, **kwargs) -> ScheduledTask | None:
        """Update a scheduled task. Returns updated task or None if not found."""
        task = self._tasks.get(task_id)
        if task is None:
            return None

        # If cron_expr changed, recompute next_run_at
        if "cron_expr" in kwargs:
            new_cron = kwargs["cron_expr"]
            if not croniter.is_valid(new_cron):
                raise ValueError(f"Invalid cron expression: {new_cron}")
            kwargs["next_run_at"] = _compute_next_run(new_cron)

        from src.db.queries import update_scheduled_task
        await update_scheduled_task(task_id, **kwargs)

        # Update in-memory cache
        for key, value in kwargs.items():
            if hasattr(task, key):
                setattr(task, key, value)

        return task

    async def delete_task(self, task_id: int) -> bool:
        """Delete a scheduled task. Returns True if found and deleted."""
        if task_id not in self._tasks:
            return False

        from src.db.queries import delete_scheduled_task
        await delete_scheduled_task(task_id)
        del self._tasks[task_id]
        return True

    async def set_task_enabled(self, task_id: int, enabled: bool) -> ScheduledTask | None:
        """Pause or resume a task. Returns updated task or None."""
        task = self._tasks.get(task_id)
        if task is None:
            return None

        from src.db.queries import set_scheduled_task_enabled
        await set_scheduled_task_enabled(task_id, enabled)
        task.enabled = enabled

        # Recompute next_run if re-enabling
        if enabled and task.next_run_at is None:
            from src.db.queries import update_scheduled_task
            next_run = _compute_next_run(task.cron_expr)
            task.next_run_at = next_run
            await update_scheduled_task(task_id, next_run_at=next_run)

        return task

    def list_tasks(self) -> list[ScheduledTask]:
        """Return all tasks."""
        return list(self._tasks.values())
```

- [ ] **Step 4: Fix the import issue in _execute_fresh**

The `__import__` hack in `_execute_fresh` is ugly. Replace it with a proper import at the top of the method. The method already imports `SessionState` via `_execute_existing`. Refactor `_execute_fresh` to use a clean import:

Replace the `__import__` lines in `_execute_fresh` with:

```python
            from src.sessions.state import SessionState
            if existing.state in (SessionState.RUNNING, SessionState.WAITING_PERMISSION):
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd /Users/zarev/claude/telegram-multi-thread-router && python -m pytest tests/test_scheduler.py -v`
Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add src/sessions/scheduler.py tests/test_scheduler.py
git commit -m "feat(scheduler): implement SchedulerService with tick loop and CRUD"
```

---

### Task 5: Orchestrator MCP tools for schedule management

**Files:**
- Modify: `src/sessions/orchestrator.py:122-151` (system prompt) and `547-550` (tool list)
- Test: `tests/test_scheduler.py`

- [ ] **Step 1: Write failing test for create_schedule MCP tool**

Append to `tests/test_scheduler.py`:

```python
async def test_scheduler_create_task_validates_cron(tmp_db):
    """create_task rejects invalid cron expressions."""
    svc, manager, bot = _make_scheduler(tmp_db)

    with pytest.raises(ValueError, match="Invalid cron expression"):
        await svc.create_task(
            name="bad",
            cron_expr="not a cron",
            prompt="x",
            target_thread_id=42,
        )


async def test_scheduler_create_task_requires_target(tmp_db):
    """create_task rejects tasks with no target."""
    svc, manager, bot = _make_scheduler(tmp_db)

    with pytest.raises(ValueError, match="Either target_thread_id"):
        await svc.create_task(
            name="no-target",
            cron_expr="0 9 * * *",
            prompt="x",
        )


async def test_scheduler_crud_lifecycle(tmp_db):
    """Full CRUD lifecycle: create, list, update, pause, resume, delete."""
    svc, manager, bot = _make_scheduler(tmp_db)

    # Create
    task = await svc.create_task(
        name="test",
        cron_expr="0 9 * * *",
        prompt="hello",
        target_thread_id=42,
    )
    assert task.id > 0
    assert task.next_run_at is not None

    # List
    tasks = svc.list_tasks()
    assert len(tasks) == 1

    # Update
    updated = await svc.update_task(task.id, name="renamed", prompt="world")
    assert updated.name == "renamed"
    assert updated.prompt == "world"

    # Pause
    paused = await svc.set_task_enabled(task.id, False)
    assert paused.enabled is False

    # Resume
    resumed = await svc.set_task_enabled(task.id, True)
    assert resumed.enabled is True

    # Delete
    deleted = await svc.delete_task(task.id)
    assert deleted is True
    assert len(svc.list_tasks()) == 0
```

- [ ] **Step 2: Run tests to verify they pass** (these test SchedulerService CRUD which is already implemented)

Run: `cd /Users/zarev/claude/telegram-multi-thread-router && python -m pytest tests/test_scheduler.py -v -k "crud or validates or requires"`
Expected: all PASS

- [ ] **Step 3: Add MCP tools to orchestrator.py**

In `src/sessions/orchestrator.py`, the `create_orchestrator_mcp_server` function needs a new parameter `scheduler` and 6 new tools. Add the parameter and tools.

First, update the function signature at line 197:

```python
def create_orchestrator_mcp_server(
    bot: Bot,
    chat_id: int,
    orchestrator_thread_id: int,
    session_manager: SessionManager,
    permission_manager: PermissionManager,
    worker_registry,
    scheduler=None,
):
```

Then, before the `return create_sdk_mcp_server(...)` at line 547, add the 6 schedule tools:

```python
    # --- Schedule management tools ---

    @tool(
        "create_schedule",
        "Create a cron-scheduled task. For existing sessions, pass target_thread_id. "
        "For fresh sessions (clean context each run), pass workdir (and optionally server, provider). "
        "Returns the task ID and next run time.",
        {"name": str, "cron_expr": str, "prompt": str,
         "target_thread_id": int, "workdir": str, "server": str, "provider": str},
    )
    async def create_schedule(args: dict) -> dict:
        if scheduler is None:
            return {"content": [{"type": "text", "text": "Error: scheduler not available"}]}
        try:
            task = await scheduler.create_task(
                name=args["name"],
                cron_expr=args["cron_expr"],
                prompt=args["prompt"],
                target_thread_id=args.get("target_thread_id"),
                new_session_workdir=args.get("workdir"),
                new_session_server=args.get("server", "local"),
                new_session_provider=args.get("provider"),
            )
            return {"content": [{"type": "text", "text": (
                f"Schedule '{task.name}' created (ID: {task.id}). "
                f"Cron: {task.cron_expr}. Next run: {task.next_run_at}"
            )}]}
        except ValueError as e:
            return {"content": [{"type": "text", "text": f"Error: {e}"}]}
        except Exception as e:
            logger.error("create_schedule error: %s", e)
            return {"content": [{"type": "text", "text": f"Error: {e}"}]}

    @tool(
        "list_schedules",
        "List all scheduled tasks with their status, cron, next run time, and target.",
        {},
    )
    async def list_schedules(args: dict) -> dict:
        if scheduler is None:
            return {"content": [{"type": "text", "text": "Error: scheduler not available"}]}
        tasks = scheduler.list_tasks()
        if not tasks:
            return {"content": [{"type": "text", "text": "No scheduled tasks."}]}
        lines = []
        for t in tasks:
            status = "enabled" if t.enabled else "PAUSED"
            target = (
                f"thread {t.target_thread_id}"
                if t.target_thread_id
                else f"fresh @ {t.new_session_workdir}"
            )
            lines.append(
                f"- [{t.id}] {t.name} ({status}) cron={t.cron_expr} "
                f"next={t.next_run_at or 'N/A'} runs={t.run_count} target={target}"
            )
        return {"content": [{"type": "text", "text": "\n".join(lines)}]}

    @tool(
        "update_schedule",
        "Update a scheduled task. Pass task_id and any fields to change: "
        "name, cron_expr, prompt, target_thread_id, workdir, server, provider.",
        {"task_id": int, "name": str, "cron_expr": str, "prompt": str,
         "target_thread_id": int, "workdir": str, "server": str, "provider": str},
    )
    async def update_schedule(args: dict) -> dict:
        if scheduler is None:
            return {"content": [{"type": "text", "text": "Error: scheduler not available"}]}
        task_id = args["task_id"]
        kwargs = {}
        for key in ("name", "cron_expr", "prompt"):
            if key in args and args[key] is not None:
                kwargs[key] = args[key]
        if "target_thread_id" in args and args["target_thread_id"] is not None:
            kwargs["target_thread_id"] = args["target_thread_id"]
        if "workdir" in args and args["workdir"] is not None:
            kwargs["new_session_workdir"] = args["workdir"]
        if "server" in args and args["server"] is not None:
            kwargs["new_session_server"] = args["server"]
        if "provider" in args and args["provider"] is not None:
            kwargs["new_session_provider"] = args["provider"]
        try:
            task = await scheduler.update_task(task_id, **kwargs)
            if task is None:
                return {"content": [{"type": "text", "text": f"Schedule {task_id} not found."}]}
            return {"content": [{"type": "text", "text": (
                f"Schedule '{task.name}' updated. Next run: {task.next_run_at}"
            )}]}
        except ValueError as e:
            return {"content": [{"type": "text", "text": f"Error: {e}"}]}

    @tool(
        "delete_schedule",
        "Delete a scheduled task by ID.",
        {"task_id": int},
    )
    async def delete_schedule(args: dict) -> dict:
        if scheduler is None:
            return {"content": [{"type": "text", "text": "Error: scheduler not available"}]}
        deleted = await scheduler.delete_task(args["task_id"])
        if deleted:
            return {"content": [{"type": "text", "text": f"Schedule {args['task_id']} deleted."}]}
        return {"content": [{"type": "text", "text": f"Schedule {args['task_id']} not found."}]}

    @tool(
        "pause_schedule",
        "Pause a scheduled task (stop it from running without deleting).",
        {"task_id": int},
    )
    async def pause_schedule(args: dict) -> dict:
        if scheduler is None:
            return {"content": [{"type": "text", "text": "Error: scheduler not available"}]}
        task = await scheduler.set_task_enabled(args["task_id"], False)
        if task is None:
            return {"content": [{"type": "text", "text": f"Schedule {args['task_id']} not found."}]}
        return {"content": [{"type": "text", "text": f"Schedule '{task.name}' paused."}]}

    @tool(
        "resume_schedule",
        "Resume a paused scheduled task.",
        {"task_id": int},
    )
    async def resume_schedule(args: dict) -> dict:
        if scheduler is None:
            return {"content": [{"type": "text", "text": "Error: scheduler not available"}]}
        task = await scheduler.set_task_enabled(args["task_id"], True)
        if task is None:
            return {"content": [{"type": "text", "text": f"Schedule {args['task_id']} not found."}]}
        return {"content": [{"type": "text", "text": (
            f"Schedule '{task.name}' resumed. Next run: {task.next_run_at}"
        )}]}
```

- [ ] **Step 4: Update the tool list in the return statement**

Replace line 547-550:

```python
    return create_sdk_mcp_server(
        "orchestrator",
        tools=[
            create_session, list_sessions, stop_session, auto_mode,
            goal_mode, send_to_session,
            create_schedule, list_schedules, update_schedule,
            delete_schedule, pause_schedule, resume_schedule,
        ],
    )
```

- [ ] **Step 5: Update the orchestrator system prompt**

In `_build_orchestrator_system_prompt` (line 122-151), add schedule tool descriptions after the existing tool list. Insert before the closing `"You can browse filesystems..."` paragraph:

```python
        "- create_schedule(name, cron_expr, prompt, target_thread_id | workdir+server+provider): "
        "Create a cron-scheduled task. Use target_thread_id for existing sessions (keeps context), "
        "or workdir for fresh sessions (clean context each run, pinned thread).\n"
        "- list_schedules(): List all scheduled tasks\n"
        "- update_schedule(task_id, ...): Update a scheduled task\n"
        "- delete_schedule(task_id): Delete a scheduled task\n"
        "- pause_schedule(task_id): Pause without deleting\n"
        "- resume_schedule(task_id): Resume a paused task\n\n"
```

- [ ] **Step 6: Run all tests**

Run: `cd /Users/zarev/claude/telegram-multi-thread-router && python -m pytest tests/ -v`
Expected: all PASS

- [ ] **Step 7: Commit**

```bash
git add src/sessions/orchestrator.py tests/test_scheduler.py
git commit -m "feat(scheduler): add orchestrator MCP tools for schedule management"
```

---

### Task 6: Wire SchedulerService into dispatcher lifecycle

**Files:**
- Modify: `src/bot/dispatcher.py:84-131` (on_startup) and `140-183` (on_shutdown)
- Modify: `src/sessions/orchestrator.py` (pass scheduler to MCP server builder)

- [ ] **Step 1: Add scheduler startup in dispatcher.py**

In `on_startup`, after the health check task creation (line 95) and before the orchestrator MCP server setup (line 97), add:

```python
        # Start scheduler service
        from src.sessions.scheduler import SchedulerService
        scheduler = SchedulerService(
            session_manager=manager,
            bot=bot,
            chat_id=settings.chat_id,
            permission_manager=permission_manager,
            question_manager=question_manager,
        )
        await scheduler.start()
        dispatcher["scheduler"] = scheduler
```

- [ ] **Step 2: Pass scheduler to orchestrator MCP server creation**

In `src/sessions/orchestrator.py`, find where `create_orchestrator_mcp_server` is called inside `_build_orchestrator_runner` (line 584-591). Update to accept and pass `scheduler`:

Update `_build_orchestrator_runner` signature to accept `scheduler=None` parameter, and pass it through:

```python
def _build_orchestrator_runner(
    *,
    provider: str,
    thread_id: int,
    chat_id: int,
    bot: Bot,
    session_manager: SessionManager,
    permission_manager: PermissionManager,
    question_manager,
    worker_registry,
    model: str | None,
    session_id: str | None,
    backend_session_id: str | None,
    orchestrator_mcp_url: str | None,
    scheduler=None,
):
```

And update the `create_orchestrator_mcp_server` call inside it:

```python
    orch_mcp = create_orchestrator_mcp_server(
        bot,
        chat_id,
        thread_id,
        session_manager,
        permission_manager,
        worker_registry,
        scheduler=scheduler,
    )
```

Then propagate `scheduler` through `_start_orchestrator_runner`, `_attach_orchestrator_fallback`, `_fallback_orchestrator_provider`, and `ensure_orchestrator`.

Each function that calls `_build_orchestrator_runner` or `_start_orchestrator_runner` needs to accept and pass `scheduler=None`:

- `_start_orchestrator_runner`: add `scheduler=None` param, pass to `_build_orchestrator_runner`
- `_attach_orchestrator_fallback`: add `scheduler=None` param, pass to `_fallback_orchestrator_provider`
- `_fallback_orchestrator_provider`: add `scheduler=None` param, pass to `_start_orchestrator_runner` and `_attach_orchestrator_fallback`
- `ensure_orchestrator`: add `scheduler=None` param, pass to `_start_orchestrator_runner` and `_attach_orchestrator_fallback`

- [ ] **Step 3: Update ensure_orchestrator call in dispatcher.py**

In `on_startup` (line 115-123), pass scheduler:

```python
        orch_thread = await ensure_orchestrator(
            bot,
            settings.chat_id,
            manager,
            permission_manager,
            question_manager,
            worker_registry,
            orchestrator_mcp_url=orchestrator_mcp_url,
            scheduler=scheduler,
        )
```

- [ ] **Step 4: Add scheduler shutdown in dispatcher.py**

In `on_shutdown`, before the session stopping loop (line 165), add:

```python
    # Stop scheduler
    scheduler = dispatcher.get("scheduler")
    if scheduler:
        await scheduler.stop()
```

- [ ] **Step 5: Run all tests**

Run: `cd /Users/zarev/claude/telegram-multi-thread-router && python -m pytest tests/ -v`
Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add src/bot/dispatcher.py src/sessions/orchestrator.py
git commit -m "feat(scheduler): wire SchedulerService into bot lifecycle"
```

---

### Task 7: Update CLAUDE.md documentation

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Add scheduler to project structure**

In the Project Structure section of `CLAUDE.md`, add under `sessions/`:

```
    scheduler.py       - SchedulerService (cron tick loop, task CRUD)
```

- [ ] **Step 2: Add scheduler section to documentation**

Add a new section after "Telegram Bot Commands":

```markdown
## Scheduled Tasks

Cron-based task scheduler managed via orchestrator natural language.

**Two modes:**
- **Existing session**: `target_thread_id` — enqueue prompt with full context. Skips if session is RUNNING.
- **Fresh session**: `workdir` — pinned thread per task, session_id cleared each run (clean context).

**Orchestrator MCP tools:** `create_schedule`, `list_schedules`, `update_schedule`, `delete_schedule`, `pause_schedule`, `resume_schedule`

**Example:** "every day at 9am check logs in /home/deploy/myapp" → orchestrator calls `create_schedule`

**Persistence:** SQLite `scheduled_tasks` table, restored on restart via `SchedulerService.start()`.
```

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: add scheduled tasks to CLAUDE.md"
```

---

### Task 8: Final integration test

**Files:**
- Test: `tests/test_scheduler.py`

- [ ] **Step 1: Run full test suite**

Run: `cd /Users/zarev/claude/telegram-multi-thread-router && python -m pytest tests/ -v`
Expected: all PASS

- [ ] **Step 2: Verify imports work end-to-end**

Run: `cd /Users/zarev/claude/telegram-multi-thread-router && python -c "from src.sessions.scheduler import SchedulerService; print('scheduler ok')" && python -c "from src.db.queries import insert_scheduled_task, get_due_scheduled_tasks; print('queries ok')"`
Expected: both print ok

- [ ] **Step 3: Commit any remaining fixes**

If any tests needed fixes, commit them:

```bash
git add -A
git commit -m "fix(scheduler): integration test fixes"
```
