"""Tests for forum topic routing and session commands."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.bot.routers.session import (
    handle_new,
    handle_list_in_session,
    handle_session_message,
    handle_stop,
)
from src.sessions.manager import SessionManager
from src.sessions.state import SessionState


def _make_message(thread_id, text="hello"):
    """Create a mock Message with given thread_id."""
    msg = MagicMock()
    msg.message_thread_id = thread_id
    msg.message_id = 1
    msg.from_user = MagicMock()
    msg.from_user.id = 12345
    msg.text = text
    msg.reply = AsyncMock()
    msg.react = AsyncMock()
    return msg


# ---------------------------------------------------------------------------
# /new command
# ---------------------------------------------------------------------------

async def test_handle_new_missing_args():
    """/new with too few args replies with usage."""
    from src.ipc.server import WorkerRegistry
    msg = _make_message(thread_id=42, text="/new")
    bot = AsyncMock()
    session_manager = MagicMock(spec=SessionManager)
    permission_manager = MagicMock()
    worker_registry = WorkerRegistry()
    await handle_new(msg, bot, session_manager, permission_manager, worker_registry)
    msg.reply.assert_called_once()
    assert "Usage" in msg.reply.call_args[0][0]


async def test_handle_new_codex_disabled(monkeypatch):
    """/new ... codex is rejected when ENABLE_CODEX is false."""
    from src.ipc.server import WorkerRegistry

    monkeypatch.setattr("src.bot.routers.session.settings.enable_codex", False)
    msg = _make_message(thread_id=42, text="/new demo /tmp codex")
    bot = AsyncMock()
    session_manager = MagicMock(spec=SessionManager)
    permission_manager = MagicMock()
    worker_registry = WorkerRegistry()

    await handle_new(msg, bot, session_manager, permission_manager, worker_registry)

    msg.reply.assert_called_once()
    assert "disabled" in msg.reply.call_args[0][0].lower()


async def test_handle_new_codex_provider(monkeypatch):
    """/new passes provider='codex' into DB and SessionManager when enabled."""
    from src.ipc.server import WorkerRegistry

    monkeypatch.setattr("src.bot.routers.session.settings.enable_codex", True)
    monkeypatch.setattr("src.bot.routers.session.settings.chat_id", -100999)
    insert_topic = AsyncMock()
    insert_session = AsyncMock()
    monkeypatch.setattr("src.bot.routers.session.insert_topic", insert_topic)
    monkeypatch.setattr("src.bot.routers.session.insert_session", insert_session)

    topic = MagicMock(message_thread_id=77)
    bot = AsyncMock(return_value=topic)
    bot.send_message = AsyncMock()
    msg = _make_message(thread_id=42, text="/new demo /tmp local codex")
    session_manager = MagicMock(spec=SessionManager)
    session_manager.create = AsyncMock()
    permission_manager = MagicMock()
    worker_registry = WorkerRegistry()

    await handle_new(msg, bot, session_manager, permission_manager, worker_registry)

    insert_session.assert_called_once()
    assert insert_session.call_args.kwargs["provider"] == "codex"
    session_manager.create.assert_called_once()
    assert session_manager.create.call_args.kwargs["provider"] == "codex"


async def test_handle_new_rewrites_personal_agent_path(monkeypatch):
    """/new rewrites known Mac repo paths to the server path on personal."""
    from src.ipc.server import WorkerRegistry

    monkeypatch.setattr("src.bot.routers.session.settings.enable_codex", True)
    monkeypatch.setattr("src.bot.routers.session.settings.chat_id", -100999)
    insert_topic = AsyncMock()
    insert_session = AsyncMock()
    monkeypatch.setattr("src.bot.routers.session.insert_topic", insert_topic)
    monkeypatch.setattr("src.bot.routers.session.insert_session", insert_session)

    topic = MagicMock(message_thread_id=88)
    bot = AsyncMock(return_value=topic)
    bot.send_message = AsyncMock()
    msg = _make_message(thread_id=42, text="/new demo /Users/knyaz/agent personal codex")
    session_manager = MagicMock(spec=SessionManager)
    session_manager.create_remote = AsyncMock()
    permission_manager = MagicMock()
    worker_registry = WorkerRegistry()
    mock_writer = MagicMock()
    mock_writer.is_closing.return_value = False
    mock_writer.drain = AsyncMock()
    worker_registry.register("personal", mock_writer)

    await handle_new(msg, bot, session_manager, permission_manager, worker_registry)

    assert insert_session.call_args.args[1] == "/root/agent"
    assert session_manager.create_remote.call_args.kwargs["workdir"] == "/root/agent"


# ---------------------------------------------------------------------------
# /list command
# ---------------------------------------------------------------------------

async def test_handle_list_no_sessions():
    """/list with no active sessions replies accordingly."""
    from src.ipc.server import WorkerRegistry
    msg = _make_message(thread_id=42, text="/list")
    session_manager = MagicMock(spec=SessionManager)
    session_manager.list_all.return_value = []
    worker_registry = WorkerRegistry()
    await handle_list_in_session(msg, session_manager, worker_registry)
    msg.reply.assert_called_once()
    assert "No active sessions" in msg.reply.call_args[0][0]


async def test_handle_list_with_sessions():
    """/list with sessions shows thread_id and workdir."""
    from src.ipc.server import WorkerRegistry
    msg = _make_message(thread_id=42, text="/list")
    runner = MagicMock()
    runner.workdir = "/home/user/proj"
    runner.state = SessionState.IDLE
    runner.auto_mode = False
    session_manager = MagicMock(spec=SessionManager)
    session_manager.list_all.return_value = [(42, runner)]
    worker_registry = WorkerRegistry()
    await handle_list_in_session(msg, session_manager, worker_registry)
    msg.reply.assert_called_once()
    call_text = msg.reply.call_args[0][0]
    assert "42" in call_text
    assert "/home/user/proj" in call_text
    assert "IDLE" in call_text


async def test_handle_list_escapes_html_sensitive_fields():
    """/list should escape workdir/provider/server values before sending HTML."""
    from src.ipc.server import WorkerRegistry

    msg = _make_message(thread_id=42, text="/list")
    runner = MagicMock()
    runner.workdir = "/tmp/<15%>"
    runner.state = SessionState.IDLE
    runner.auto_mode = False
    runner.provider = 'codex<script>'
    session_manager = MagicMock(spec=SessionManager)
    session_manager.list_all.return_value = [(42, runner)]
    worker_registry = WorkerRegistry()
    worker_registry.register("srv<bad>", MagicMock(is_closing=MagicMock(return_value=False)))

    await handle_list_in_session(msg, session_manager, worker_registry)

    text = msg.reply.call_args[0][0]
    assert "&lt;15%&gt;" in text
    assert "codex&lt;script&gt;" in text


# ---------------------------------------------------------------------------
# Session router
# ---------------------------------------------------------------------------

async def test_handle_stop_no_session():
    """/stop in topic without session replies with error."""
    msg = _make_message(thread_id=42, text="/stop")
    session_manager = MagicMock(spec=SessionManager)
    session_manager.get.return_value = None
    await handle_stop(msg, session_manager)
    msg.reply.assert_called_once()
    assert "No active session" in msg.reply.call_args[0][0]


async def test_handle_stop_active_session():
    """/stop calls runner.interrupt() and replies."""
    msg = _make_message(thread_id=42, text="/stop")
    session_manager = MagicMock(spec=SessionManager)
    runner = MagicMock()
    runner.interrupt = AsyncMock(return_value=True)
    session_manager.get.return_value = runner
    await handle_stop(msg, session_manager)
    runner.interrupt.assert_called_once()
    msg.reply.assert_called_once()
    assert "interrupt" in msg.reply.call_args[0][0].lower() or "Interrupted" in msg.reply.call_args[0][0]


async def test_handle_session_message_no_runner():
    """Messages to topic with no session are silently ignored."""
    msg = _make_message(thread_id=42, text="hello claude")
    session_manager = MagicMock(spec=SessionManager)
    session_manager.get.return_value = None
    await handle_session_message(msg, session_manager)
    msg.reply.assert_not_called()
    msg.react.assert_not_called()


async def test_handle_session_message_stopped():
    """Messages to a stopped session prompt restart hint."""
    msg = _make_message(thread_id=42, text="hello")
    runner = MagicMock()
    runner.state = SessionState.STOPPED
    session_manager = MagicMock(spec=SessionManager)
    session_manager.get.return_value = runner
    await handle_session_message(msg, session_manager)
    msg.reply.assert_called_once()
    assert "/new" in msg.reply.call_args[0][0]


async def test_handle_session_message_enqueues_and_reacts():
    """Text messages in active session are reacted to and enqueued."""
    msg = _make_message(thread_id=42, text="do something")
    runner = MagicMock()
    runner.state = SessionState.IDLE
    runner.enqueue = AsyncMock()
    session_manager = MagicMock(spec=SessionManager)
    session_manager.get.return_value = runner
    await handle_session_message(msg, session_manager)
    msg.react.assert_called_once()
    runner.enqueue.assert_called_once_with("do something", reply_to_message_id=msg.message_id)


async def test_handle_session_message_forwards_slash_commands():
    """/clear, /compact, /reset are forwarded as raw text (not intercepted)."""
    for cmd in ["/clear", "/compact", "/reset"]:
        msg = _make_message(thread_id=42, text=cmd)
        runner = MagicMock()
        runner.state = SessionState.IDLE
        runner.enqueue = AsyncMock()
        session_manager = MagicMock(spec=SessionManager)
        session_manager.get.return_value = runner
        await handle_session_message(msg, session_manager)
        runner.enqueue.assert_called_once_with(cmd, reply_to_message_id=msg.message_id)
        runner.enqueue.reset_mock()


# ---------------------------------------------------------------------------
# Dispatcher wiring
# ---------------------------------------------------------------------------

@patch.dict("os.environ", {
    "BOT_TOKEN": "test",
    "OWNER_USER_ID": "12345",
    "CHAT_ID": "-100999",
    "AUTH_TOKEN": "test",
})
def test_build_dispatcher_has_middleware():
    """build_dispatcher registers OwnerAuthMiddleware on dp.message."""
    import importlib
    import src.config
    importlib.reload(src.config)

    from src.bot.dispatcher import build_dispatcher
    dp = build_dispatcher()

    # Check that outer middleware is registered on message observer
    assert len(dp.message.outer_middleware) > 0 or hasattr(dp, '_middlewares')
    # Verify routers are included
    assert len(dp.sub_routers) >= 2  # general + session
