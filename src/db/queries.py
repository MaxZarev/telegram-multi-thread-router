"""Named SQL query functions for session and topic CRUD."""

from src.db.connection import get_connection
from src.sessions.backend import normalize_provider


async def insert_topic(thread_id: int, name: str, is_orchestrator: bool = False) -> None:
    """Insert a new topic record."""
    async with get_connection() as conn:
        await conn.execute(
            "INSERT INTO topics (thread_id, name, is_orchestrator) VALUES (?, ?, ?)",
            (thread_id, name, int(is_orchestrator)),
        )
        await conn.commit()


async def get_orchestrator_topic() -> dict | None:
    """Return the orchestrator topic row, or None."""
    async with get_connection() as conn:
        cursor = await conn.execute(
            "SELECT thread_id, name FROM topics WHERE is_orchestrator=1 LIMIT 1"
        )
        row = await cursor.fetchone()
        return dict(row) if row else None


async def insert_session(
    thread_id: int,
    workdir: str,
    model: str | None = None,
    server: str = "local",
    provider: str | None = None,
    backend_session_id: str | None = None,
) -> None:
    """Insert a new session record with state='idle'."""
    provider = normalize_provider(provider)
    async with get_connection() as conn:
        await conn.execute(
            "INSERT INTO sessions (thread_id, workdir, model, state, server, provider, backend_session_id) "
            "VALUES (?, ?, ?, 'idle', ?, ?, ?)",
            (thread_id, workdir, model, server, provider, backend_session_id),
        )
        await conn.commit()


async def update_session_id(thread_id: int, session_id: str) -> None:
    """Update the Claude session_id for a thread after first ResultMessage."""
    async with get_connection() as conn:
        await conn.execute(
            "UPDATE sessions SET session_id=?, backend_session_id=?, updated_at=datetime('now') "
            "WHERE thread_id=?",
            (session_id, session_id, thread_id),
        )
        await conn.commit()


async def update_backend_session_id(thread_id: int, backend_session_id: str) -> None:
    """Update the provider-specific backend session identifier for a thread."""
    async with get_connection() as conn:
        await conn.execute(
            "UPDATE sessions SET backend_session_id=?, updated_at=datetime('now') WHERE thread_id=?",
            (backend_session_id, thread_id),
        )
        await conn.commit()


async def clear_session_id(thread_id: int) -> None:
    """Clear session_id so the next runner starts a fresh conversation (no resume)."""
    async with get_connection() as conn:
        await conn.execute(
            "UPDATE sessions SET session_id=NULL, backend_session_id=NULL, "
            "state='idle', updated_at=datetime('now') WHERE thread_id=?",
            (thread_id,),
        )
        await conn.commit()


async def update_session_state(thread_id: int, state: str) -> None:
    """Update the state of the most recent session for a thread."""
    async with get_connection() as conn:
        await conn.execute(
            "UPDATE sessions SET state=?, updated_at=datetime('now') WHERE thread_id=?",
            (state, thread_id),
        )
        await conn.commit()


async def update_session_provider(thread_id: int, provider: str, model: str | None = None) -> None:
    """Update provider/model and clear backend IDs after a provider switch."""
    provider = normalize_provider(provider)
    async with get_connection() as conn:
        await conn.execute(
            "UPDATE sessions SET provider=?, model=?, session_id=NULL, backend_session_id=NULL, "
            "state='idle', updated_at=datetime('now') WHERE thread_id=?",
            (provider, model, thread_id),
        )
        await conn.commit()


async def get_resumable_sessions() -> list[dict]:
    """Return all local sessions that were running or idle and have a session_id."""
    async with get_connection() as conn:
        try:
            cursor = await conn.execute(
                "SELECT thread_id, session_id, backend_session_id, workdir, model, state, server, "
                "provider, auto_mode, goal_text FROM sessions "
                "WHERE state IN ('running', 'idle') AND ("
                "(provider='claude' AND session_id IS NOT NULL) OR "
                "(provider!='claude' AND backend_session_id IS NOT NULL)"
                ")"
            )
        except Exception as e:
            if "no such column" not in str(e).lower():
                raise
            cursor = await conn.execute(
                "SELECT thread_id, session_id, session_id AS backend_session_id, workdir, "
                "NULL AS model, state, server, 'claude' AS provider, 0 AS auto_mode FROM sessions "
                "WHERE state IN ('running', 'idle') AND session_id IS NOT NULL"
            )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def get_worker_sessions(worker_id: str) -> list[dict]:
    """Return all idle/running sessions for a specific remote worker (session_id not required)."""
    async with get_connection() as conn:
        try:
            cursor = await conn.execute(
                "SELECT thread_id, session_id, backend_session_id, workdir, model, state, server, "
                "provider, auto_mode FROM sessions "
                "WHERE state IN ('running', 'idle') AND server=?",
                (worker_id,),
            )
        except Exception as e:
            if "no such column" not in str(e).lower():
                raise
            cursor = await conn.execute(
                "SELECT thread_id, session_id, session_id AS backend_session_id, workdir, "
                "NULL AS model, state, server, 'claude' AS provider, 0 AS auto_mode FROM sessions "
                "WHERE state IN ('running', 'idle') AND server=?",
                (worker_id,),
            )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def get_session_by_thread(thread_id: int) -> dict | None:
    """Return the most recent session row for a thread, or None."""
    async with get_connection() as conn:
        cursor = await conn.execute(
            "SELECT * FROM sessions WHERE thread_id=? ORDER BY id DESC LIMIT 1",
            (thread_id,),
        )
        row = await cursor.fetchone()
        return dict(row) if row else None


async def update_auto_mode(thread_id: int, enabled: bool) -> None:
    """Update auto_mode flag for a session."""
    async with get_connection() as conn:
        await conn.execute(
            "UPDATE sessions SET auto_mode=?, updated_at=datetime('now') WHERE thread_id=?",
            (int(enabled), thread_id),
        )
        await conn.commit()


async def update_goal_text(thread_id: int, goal_text: str | None) -> None:
    """Update goal_text for a session (None to clear)."""
    async with get_connection() as conn:
        await conn.execute(
            "UPDATE sessions SET goal_text=?, updated_at=datetime('now') WHERE thread_id=?",
            (goal_text, thread_id),
        )
        await conn.commit()


async def delete_session_and_topic(thread_id: int) -> None:
    """Delete all DB records for a closed session (sessions, topics)."""
    async with get_connection() as conn:
        await conn.execute("DELETE FROM sessions WHERE thread_id=?", (thread_id,))
        await conn.execute("DELETE FROM topics WHERE thread_id=?", (thread_id,))
        await conn.commit()


def normalize_session_model(model: str | None) -> str | None:
    """Normalize model names before persisting them.

    Some providers/SDK paths can emit synthetic placeholder labels like
    `<synthetic>`, which are not real model names and should not be stored.
    """
    if model is None:
        return None
    normalized = str(model).strip()
    if not normalized:
        return None
    if normalized.startswith("<") and normalized.endswith(">"):
        return None
    return normalized


async def update_session_model(thread_id: int, model: str | None) -> None:
    """Update model when Claude switches models mid-session."""
    normalized = normalize_session_model(model)
    async with get_connection() as conn:
        await conn.execute(
            "UPDATE sessions SET model=?, updated_at=datetime('now') WHERE thread_id=?",
            (normalized, thread_id),
        )
        await conn.commit()


async def get_all_active_sessions() -> list[dict]:
    """Return all sessions in idle or running state, joined with topic name."""
    async with get_connection() as conn:
        try:
            cursor = await conn.execute(
                "SELECT s.id, s.thread_id, s.session_id, s.backend_session_id, s.provider, "
                "s.workdir, s.model, s.state, s.server, "
                "s.created_at, s.updated_at, t.name FROM sessions s "
                "JOIN topics t ON s.thread_id=t.thread_id "
                "WHERE s.state IN ('idle', 'running')"
            )
        except Exception as e:
            if "no such column" not in str(e).lower():
                raise
            cursor = await conn.execute(
                "SELECT s.id, s.thread_id, s.session_id, s.session_id AS backend_session_id, "
                "'claude' AS provider, s.workdir, NULL AS model, s.state, s.server, "
                "s.created_at, s.updated_at, t.name FROM sessions s "
                "JOIN topics t ON s.thread_id=t.thread_id "
                "WHERE s.state IN ('idle', 'running')"
            )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


# ---- Bot settings ----

async def get_bot_setting(key: str) -> str | None:
    """Load a bot setting value by key."""
    async with get_connection() as conn:
        cursor = await conn.execute("SELECT value FROM bot_settings WHERE key=?", (key,))
        row = await cursor.fetchone()
        return row[0] if row else None


async def set_bot_setting(key: str, value: str) -> None:
    """Save a bot setting (insert or replace)."""
    async with get_connection() as conn:
        await conn.execute(
            "INSERT OR REPLACE INTO bot_settings (key, value) VALUES (?, ?)",
            (key, value),
        )
        await conn.commit()


# ---- Global permissions ----

async def get_global_permissions() -> set[str]:
    """Load all globally allowed tool names from DB."""
    async with get_connection() as conn:
        cursor = await conn.execute("SELECT tool_name FROM global_permissions")
        rows = await cursor.fetchall()
        return {row[0] for row in rows}


async def save_global_permission(tool_name: str) -> None:
    """Persist a globally allowed tool to DB."""
    async with get_connection() as conn:
        await conn.execute(
            "INSERT OR IGNORE INTO global_permissions (tool_name) VALUES (?)",
            (tool_name,),
        )
        await conn.commit()


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
