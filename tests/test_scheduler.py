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
