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
            "id", "name", "cron_expr", "prompt",
            "target_thread_id", "new_session_workdir",
            "new_session_server", "new_session_provider",
            "pinned_thread_id", "enabled", "last_run_at",
            "next_run_at", "run_count", "created_at",
        }


from src.db import queries as q


async def test_insert_and_get_scheduled_tasks(tmp_db):
    """Insert a scheduled task and retrieve it."""
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
            name="test", cron_expr="0 9 * * *", prompt="hello",
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
            name="temp", cron_expr="0 9 * * *", prompt="x",
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
            name="test", cron_expr="0 9 * * *", prompt="x",
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
            name="due", cron_expr="0 9 * * *", prompt="x",
        )
        await q.update_scheduled_task(task_id, next_run_at="2020-01-01 00:00:00")
        disabled_id = await q.insert_scheduled_task(
            name="disabled", cron_expr="0 9 * * *", prompt="x",
        )
        await q.update_scheduled_task(disabled_id, next_run_at="2020-01-01 00:00:00")
        await q.set_scheduled_task_enabled(disabled_id, False)
        due = await q.get_due_scheduled_tasks()
        assert len(due) == 1
        assert due[0]["name"] == "due"
    finally:
        schema_mod.DB_PATH = original
        conn_mod.DB_PATH = original


# ---------------------------------------------------------------------------
# SchedulerService tests
# ---------------------------------------------------------------------------

from unittest.mock import AsyncMock, MagicMock, patch

from src.sessions.scheduler import SchedulerService
from src.sessions.state import SessionState


def _make_scheduler(tmp_db):
    """Helper: build a SchedulerService wired to tmp_db."""
    import src.db.schema as schema_mod
    import src.db.connection as conn_mod
    schema_mod.DB_PATH = tmp_db
    conn_mod.DB_PATH = tmp_db

    manager = MagicMock()
    manager.get = MagicMock(return_value=None)
    manager.create = AsyncMock()
    manager.stop = AsyncMock()

    bot = AsyncMock()
    bot.send_message = AsyncMock()
    topic_response = MagicMock()
    topic_response.message_thread_id = 77777
    # Configure bot(...) to return topic_response when called as coroutine
    bot.return_value = topic_response

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
    """Tick enqueues prompt when runner is IDLE."""
    import src.db.schema as schema_mod
    import src.db.connection as conn_mod
    original = conn_mod.DB_PATH
    try:
        svc, manager, bot = _make_scheduler(tmp_db)

        # Create an idle runner mock
        runner = MagicMock()
        runner.state = SessionState.IDLE
        runner.enqueue = AsyncMock()
        manager.get = MagicMock(return_value=runner)

        # Insert a due task with target_thread_id
        task_id = await q.insert_scheduled_task(
            name="idle-test",
            cron_expr="* * * * *",
            prompt="do the thing",
            target_thread_id=42,
        )
        await q.update_scheduled_task(task_id, next_run_at="2020-01-01 00:00:00")

        await svc._tick()

        runner.enqueue.assert_called_once_with("do the thing")
    finally:
        schema_mod.DB_PATH = original
        conn_mod.DB_PATH = original


async def test_scheduler_tick_existing_running_skips(tmp_db):
    """Tick does NOT enqueue prompt when runner is RUNNING."""
    import src.db.schema as schema_mod
    import src.db.connection as conn_mod
    original = conn_mod.DB_PATH
    try:
        svc, manager, bot = _make_scheduler(tmp_db)

        runner = MagicMock()
        runner.state = SessionState.RUNNING
        runner.enqueue = AsyncMock()
        manager.get = MagicMock(return_value=runner)

        task_id = await q.insert_scheduled_task(
            name="running-test",
            cron_expr="* * * * *",
            prompt="do the thing",
            target_thread_id=42,
        )
        await q.update_scheduled_task(task_id, next_run_at="2020-01-01 00:00:00")

        await svc._tick()

        runner.enqueue.assert_not_called()
    finally:
        schema_mod.DB_PATH = original
        conn_mod.DB_PATH = original


async def test_scheduler_tick_fresh_creates_pinned_thread(tmp_db):
    """Fresh-mode tick creates a pinned thread and saves pinned_thread_id."""
    import src.db.schema as schema_mod
    import src.db.connection as conn_mod
    original = conn_mod.DB_PATH
    try:
        svc, manager, bot = _make_scheduler(tmp_db)

        # Fresh runner returned by create
        fresh_runner = MagicMock()
        fresh_runner.auto_mode = False
        fresh_runner.enqueue = AsyncMock()
        manager.create = AsyncMock(return_value=fresh_runner)

        task_id = await q.insert_scheduled_task(
            name="fresh-test",
            cron_expr="* * * * *",
            prompt="run fresh",
            new_session_workdir="/tmp/work",
        )
        await q.update_scheduled_task(task_id, next_run_at="2020-01-01 00:00:00")

        # Patch DB ops that require existing FK rows (topics table not pre-populated)
        with patch("src.sessions.scheduler.q.insert_session", new=AsyncMock()), \
             patch("src.sessions.scheduler.q.insert_topic", new=AsyncMock()):
            await svc._tick()

        # Verify pinned_thread_id was saved to DB
        tasks = await q.get_all_scheduled_tasks()
        assert tasks[0]["pinned_thread_id"] == 77777

        # Verify enqueue was called with the prompt
        fresh_runner.enqueue.assert_called_once_with("run fresh")
    finally:
        schema_mod.DB_PATH = original
        conn_mod.DB_PATH = original


async def test_scheduler_create_task_validates_cron(tmp_db):
    """create_task raises ValueError for invalid cron expression."""
    import src.db.schema as schema_mod
    import src.db.connection as conn_mod
    original = conn_mod.DB_PATH
    try:
        svc, _, _ = _make_scheduler(tmp_db)

        with pytest.raises(ValueError, match="Invalid cron"):
            await svc.create_task(
                name="bad",
                cron_expr="not-a-cron",
                prompt="x",
                target_thread_id=1,
            )
    finally:
        schema_mod.DB_PATH = original
        conn_mod.DB_PATH = original


async def test_scheduler_create_task_requires_target(tmp_db):
    """create_task raises ValueError when neither target_thread_id nor new_session_workdir is set."""
    import src.db.schema as schema_mod
    import src.db.connection as conn_mod
    original = conn_mod.DB_PATH
    try:
        svc, _, _ = _make_scheduler(tmp_db)

        with pytest.raises(ValueError, match="Either target_thread_id or new_session_workdir"):
            await svc.create_task(
                name="no-target",
                cron_expr="0 9 * * *",
                prompt="x",
            )
    finally:
        schema_mod.DB_PATH = original
        conn_mod.DB_PATH = original


async def test_scheduler_crud_lifecycle(tmp_db):
    """Full CRUD lifecycle: create, list, update, pause, resume, delete."""
    import src.db.schema as schema_mod
    import src.db.connection as conn_mod
    original = conn_mod.DB_PATH
    try:
        svc, _, _ = _make_scheduler(tmp_db)

        # Create
        task = await svc.create_task(
            name="lifecycle",
            cron_expr="0 12 * * *",
            prompt="hello world",
            target_thread_id=99,
        )
        assert task.id > 0
        assert task.name == "lifecycle"
        assert task.cron_expr == "0 12 * * *"
        assert task.prompt == "hello world"
        assert task.target_thread_id == 99
        assert task.enabled is True
        assert task.next_run_at is not None

        # List
        tasks = await svc.list_tasks()
        assert len(tasks) == 1
        assert tasks[0].id == task.id

        # Update
        await svc.update_task(task.id, name="updated-lifecycle", prompt="updated prompt")
        tasks = await svc.list_tasks()
        assert tasks[0].name == "updated-lifecycle"
        assert tasks[0].prompt == "updated prompt"

        # Pause (disable)
        await svc.set_task_enabled(task.id, False)
        tasks = await svc.list_tasks()
        assert tasks[0].enabled is False

        # Resume (enable)
        await svc.set_task_enabled(task.id, True)
        tasks = await svc.list_tasks()
        assert tasks[0].enabled is True

        # Delete
        await svc.delete_task(task.id)
        tasks = await svc.list_tasks()
        assert len(tasks) == 0
    finally:
        schema_mod.DB_PATH = original
        conn_mod.DB_PATH = original
