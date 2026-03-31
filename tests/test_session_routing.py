"""Tests for bot-side session routing: RemoteSession, SessionManager, DB server field."""

import asyncio
import os

import pytest
from unittest.mock import AsyncMock, MagicMock

from src.sessions.remote import RemoteSession
from src.sessions.manager import SessionManager
from src.ipc.server import WorkerRegistry


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_registry():
    """WorkerRegistry with a mocked connected worker."""
    registry = WorkerRegistry()
    mock_writer = MagicMock()
    mock_writer.is_closing.return_value = False
    mock_writer.drain = AsyncMock()
    registry.register("myserver", mock_writer)
    return registry


@pytest.fixture
def mock_bot():
    bot = AsyncMock()
    bot.send_message = AsyncMock()
    return bot


@pytest.fixture
def permission_manager():
    from src.sessions.permissions import PermissionManager
    return PermissionManager()


# ---------------------------------------------------------------------------
# MSRV-07: Remote routing
# ---------------------------------------------------------------------------

async def test_remote_routing(mock_registry):
    """MSRV-07: create_remote() stores RemoteSession with correct worker_id."""
    manager = SessionManager()
    session = await manager.create_remote(
        thread_id=100,
        workdir="/tmp/remote",
        worker_id="myserver",
        worker_registry=mock_registry,
    )

    assert isinstance(session, RemoteSession)
    assert session.worker_id == "myserver"
    assert session.thread_id == 100

    stored = manager.get(100)
    assert stored is session
    assert isinstance(stored, RemoteSession)
    assert stored.provider == "claude"


async def test_create_remote_raises_if_duplicate(mock_registry):
    """create_remote() raises ValueError if session for thread_id already exists."""
    manager = SessionManager()
    await manager.create_remote(
        thread_id=200,
        workdir="/tmp",
        worker_id="myserver",
        worker_registry=mock_registry,
    )
    with pytest.raises(ValueError, match="already exists"):
        await manager.create_remote(
            thread_id=200,
            workdir="/tmp",
            worker_id="myserver",
            worker_registry=mock_registry,
        )


def test_normalize_server_name_maps_personal_alias():
    """Known human aliases normalize to the connected worker ID."""
    from src.sessions.backend import normalize_server_name

    assert normalize_server_name("personal-server") == "personal"
    assert normalize_server_name("personal") == "personal"
    assert normalize_server_name("mac") == "local"


def test_resolve_workdir_for_personal_maps_agent_repo():
    """Known local repo paths are rewritten to server paths for remote workers."""
    from src.sessions.backend import resolve_workdir_for_server

    assert resolve_workdir_for_server("personal", "/Users/knyaz/agent") == "/root/agent"
    assert resolve_workdir_for_server("personal", "agent") == "/root/agent"


# ---------------------------------------------------------------------------
# MSRV-08: Local default
# ---------------------------------------------------------------------------

async def test_local_default(mock_bot, permission_manager):
    """MSRV-08: create() returns SessionRunner (not RemoteSession)."""
    from src.sessions.runner import SessionRunner

    manager = SessionManager()
    runner = await manager.create(
        thread_id=300,
        workdir="/tmp/local",
        bot=mock_bot,
        chat_id=-100999,
        permission_manager=permission_manager,
    )

    assert isinstance(runner, SessionRunner)
    assert not isinstance(runner, RemoteSession)
    stored = manager.get(300)
    assert isinstance(stored, SessionRunner)


async def test_local_codex_loads_repo_local_instructions(tmp_path, mock_bot, permission_manager):
    """Local Codex sessions inherit repo-local AGENTS.md/CLAUDE.md instructions."""
    instructions_path = tmp_path / "AGENTS.md"
    instructions_path.write_text("local codex instructions")

    manager = SessionManager()
    fake_runner = MagicMock()
    fake_runner.start = AsyncMock()
    codex_ctor = MagicMock(return_value=fake_runner)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("src.sessions.manager.CodexRunner", codex_ctor)
        runner = await manager.create(
            thread_id=301,
            workdir=str(tmp_path),
            bot=mock_bot,
            chat_id=-100999,
            permission_manager=permission_manager,
            provider="codex",
        )

    assert runner is fake_runner
    _, kwargs = codex_ctor.call_args
    assert kwargs["base_instructions"] == "local codex instructions"


# ---------------------------------------------------------------------------
# MSRV-07: get_server helper
# ---------------------------------------------------------------------------

async def test_get_server_returns_worker_id_for_remote(mock_registry):
    """MSRV-07: get_server() returns worker_id for RemoteSession."""
    manager = SessionManager()
    await manager.create_remote(
        thread_id=400,
        workdir="/tmp",
        worker_id="myserver",
        worker_registry=mock_registry,
    )
    assert manager.get_server(400) == "myserver"


async def test_get_server_returns_local_for_runner(mock_bot, permission_manager):
    """MSRV-08: get_server() returns 'local' for SessionRunner."""
    manager = SessionManager()
    await manager.create(
        thread_id=500,
        workdir="/tmp/local",
        bot=mock_bot,
        chat_id=-100999,
        permission_manager=permission_manager,
    )
    assert manager.get_server(500) == "local"


async def test_get_server_returns_local_for_unknown():
    """get_server() returns 'local' for unknown thread_id."""
    manager = SessionManager()
    assert manager.get_server(9999) == "local"


# ---------------------------------------------------------------------------
# MSRV-07: list_all shows server
# ---------------------------------------------------------------------------

async def test_list_shows_server(mock_bot, permission_manager, mock_registry):
    """MSRV-07: list_all() returns both local and remote sessions."""
    from src.sessions.runner import SessionRunner

    manager = SessionManager()
    await manager.create(
        thread_id=600,
        workdir="/tmp/local",
        bot=mock_bot,
        chat_id=-100999,
        permission_manager=permission_manager,
    )
    await manager.create_remote(
        thread_id=700,
        workdir="/tmp/remote",
        worker_id="myserver",
        worker_registry=mock_registry,
    )

    sessions = dict(manager.list_all())
    assert 600 in sessions
    assert 700 in sessions
    assert isinstance(sessions[600], SessionRunner)
    assert isinstance(sessions[700], RemoteSession)

    assert manager.get_server(600) == "local"
    assert manager.get_server(700) == "myserver"


# ---------------------------------------------------------------------------
# MSRV-07: RemoteSession interface shape
# ---------------------------------------------------------------------------

async def test_remote_session_enqueue(mock_registry):
    """RemoteSession.enqueue sends UserMessageMsg to worker."""
    from src.ipc.protocol import UserMessageMsg

    # Track send_to calls
    sent = []

    async def fake_send_to(worker_id, msg):
        sent.append((worker_id, msg))
        return True

    mock_registry.send_to = fake_send_to

    session = RemoteSession(
        thread_id=10,
        workdir="/tmp",
        worker_id="myserver",
        worker_registry=mock_registry,
    )
    await session.enqueue("hello from user")

    assert len(sent) == 1
    _, msg = sent[0]
    assert isinstance(msg, UserMessageMsg)
    assert msg.topic_id == 10
    assert msg.text == "hello from user"


async def test_remote_session_stop(mock_registry):
    """RemoteSession.stop sends StopSessionMsg and sets state=STOPPED."""
    from src.ipc.protocol import StopSessionMsg
    from src.sessions.state import SessionState

    sent = []

    async def fake_send_to(worker_id, msg):
        sent.append((worker_id, msg))
        return True

    mock_registry.send_to = fake_send_to

    session = RemoteSession(
        thread_id=20,
        workdir="/tmp",
        worker_id="myserver",
        worker_registry=mock_registry,
    )
    await session.stop()

    assert len(sent) == 1
    _, msg = sent[0]
    assert isinstance(msg, StopSessionMsg)
    assert msg.topic_id == 20
    assert session.state == SessionState.STOPPED


async def test_remote_session_interrupt(mock_registry):
    """RemoteSession.interrupt sends InterruptMsg to the worker."""
    from src.ipc.protocol import InterruptMsg

    sent = []

    async def fake_send_to(worker_id, msg):
        sent.append((worker_id, msg))
        return True

    mock_registry.send_to = fake_send_to

    session = RemoteSession(
        thread_id=25,
        workdir="/tmp",
        worker_id="myserver",
        worker_registry=mock_registry,
    )
    result = await session.interrupt()

    assert result is True
    assert len(sent) == 1
    _, msg = sent[0]
    assert isinstance(msg, InterruptMsg)
    assert msg.topic_id == 25


async def test_remote_session_is_alive(mock_registry):
    """RemoteSession.is_alive reflects WorkerRegistry.is_connected."""
    session = RemoteSession(
        thread_id=30,
        workdir="/tmp",
        worker_id="myserver",
        worker_registry=mock_registry,
    )
    assert session.is_alive is True

    # Unregister the worker
    mock_registry.unregister("myserver")
    assert session.is_alive is False


# ---------------------------------------------------------------------------
# MSRV-07: DB server field
# ---------------------------------------------------------------------------

@pytest.fixture
async def tmp_db(tmp_path, monkeypatch):
    """Initialize a temp DB and patch DB_PATH for queries."""
    db_path = tmp_path / "test.db"
    from src.db.schema import init_db
    await init_db(db_path)

    # Patch DB_PATH in both schema and connection modules
    monkeypatch.setattr("src.db.schema.DB_PATH", db_path)
    monkeypatch.setattr("src.db.connection.DB_PATH", db_path)
    return db_path


async def test_insert_session_with_server(tmp_db):
    """MSRV-07: insert_session stores server field correctly."""
    import aiosqlite
    from src.db.queries import insert_session
    from src.db.connection import get_connection

    # Need a topic first (FK constraint)
    async with get_connection() as conn:
        await conn.execute(
            "INSERT INTO topics (thread_id, name) VALUES (?, ?)",
            (1, "test-topic"),
        )
        await conn.commit()

    await insert_session(thread_id=1, workdir="/tmp", server="myserver")

    async with aiosqlite.connect(str(tmp_db)) as db:
        cursor = await db.execute("SELECT server FROM sessions WHERE thread_id=1")
        row = await cursor.fetchone()
        assert row[0] == "myserver"


async def test_insert_session_default_server(tmp_db):
    """MSRV-07: insert_session defaults server to 'local' when not specified."""
    import aiosqlite
    from src.db.queries import insert_session
    from src.db.connection import get_connection

    async with get_connection() as conn:
        await conn.execute(
            "INSERT INTO topics (thread_id, name) VALUES (?, ?)",
            (2, "local-topic"),
        )
        await conn.commit()

    await insert_session(thread_id=2, workdir="/tmp")  # no server arg

    async with aiosqlite.connect(str(tmp_db)) as db:
        cursor = await db.execute("SELECT server FROM sessions WHERE thread_id=2")
        row = await cursor.fetchone()
        assert row[0] == "local"


async def test_insert_session_with_provider_and_backend_id(tmp_db):
    """insert_session stores provider and backend_session_id for non-Claude backends."""
    import aiosqlite
    from src.db.queries import insert_session
    from src.db.connection import get_connection

    async with get_connection() as conn:
        await conn.execute(
            "INSERT INTO topics (thread_id, name) VALUES (?, ?)",
            (4, "codex-topic"),
        )
        await conn.commit()

    await insert_session(
        thread_id=4,
        workdir="/tmp/codex",
        server="local",
        provider="codex",
        backend_session_id="thread-123",
    )

    async with aiosqlite.connect(str(tmp_db)) as db:
        cursor = await db.execute(
            "SELECT provider, backend_session_id FROM sessions WHERE thread_id=4"
        )
        row = await cursor.fetchone()
        assert row[0] == "codex"
        assert row[1] == "thread-123"


async def test_get_resumable_sessions_includes_server(tmp_db):
    """get_resumable_sessions() returns server field in result rows."""
    from src.db.queries import insert_session, get_resumable_sessions
    from src.db.connection import get_connection

    async with get_connection() as conn:
        await conn.execute(
            "INSERT INTO topics (thread_id, name) VALUES (?, ?)",
            (3, "topic"),
        )
        await conn.commit()

    await insert_session(thread_id=3, workdir="/tmp", server="remotehost")
    # Must have session_id to appear in get_resumable_sessions
    from src.db.queries import update_session_id
    await update_session_id(3, "sess-abc")

    rows = await get_resumable_sessions()
    assert len(rows) >= 1
    matching = [r for r in rows if r["thread_id"] == 3]
    assert len(matching) == 1
    assert matching[0]["server"] == "remotehost"


async def test_get_resumable_sessions_uses_backend_session_id_for_codex(tmp_db):
    """Non-Claude resumable sessions are selected by backend_session_id."""
    from src.db.queries import insert_session, get_resumable_sessions
    from src.db.connection import get_connection

    async with get_connection() as conn:
        await conn.execute(
            "INSERT INTO topics (thread_id, name) VALUES (?, ?)",
            (5, "codex-topic"),
        )
        await conn.commit()

    await insert_session(
        thread_id=5,
        workdir="/tmp",
        provider="codex",
        backend_session_id="thread-xyz",
    )

    rows = await get_resumable_sessions()
    matching = [r for r in rows if r["thread_id"] == 5]
    assert len(matching) == 1
    assert matching[0]["provider"] == "codex"
    assert matching[0]["backend_session_id"] == "thread-xyz"
