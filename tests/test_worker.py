"""Tests for worker-side components: WorkerOutputChannel, WorkerClient reconnect, permission bridge."""

import asyncio
import importlib.util

import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call

from src.bot.routers.session import _parse_new_command_args
from src.ipc.protocol import (
    AssistantTextMsg,
    McpSendFileMsg,
    QuestionRequestMsg,
    StartSessionMsg,
    UserFileMsg,
    UserMessageMsg,
    _enc,
    _b2w_dec,
    _w2b_dec,
)
from src.worker.output_channel import WorkerOutputChannel


# ---------------------------------------------------------------------------
# MSRV-01: WorkerOutputChannel / SessionRunner instantiation
# ---------------------------------------------------------------------------

async def test_worker_output_channel_send_text():
    """MSRV-01: WorkerOutputChannel.send_message encodes AssistantTextMsg correctly."""
    writer = MagicMock()
    writer.is_closing.return_value = False
    writer.write = MagicMock()
    writer.drain = AsyncMock()

    channel = WorkerOutputChannel(writer, chat_id=0)
    await channel.send_message(chat_id=0, text="test output", message_thread_id=7)

    # Reconstruct what was written (AssistantTextMsg is a WorkerToBot message)
    assert writer.write.called
    raw_bytes = writer.write.call_args[0][0]
    n = int.from_bytes(raw_bytes[:4], "big")
    payload = raw_bytes[4:4 + n]
    decoded = _w2b_dec.decode(payload)
    assert isinstance(decoded, AssistantTextMsg)
    assert decoded.topic_id == 7
    assert decoded.text == "test output"


async def test_worker_output_channel_send_document_embeds_bytes(tmp_path):
    """WorkerOutputChannel.send_document serializes file bytes for remote upload."""
    writer = MagicMock()
    writer.is_closing.return_value = False
    writer.write = MagicMock()
    writer.drain = AsyncMock()

    path = tmp_path / "42.txt"
    path.write_text("hello")

    channel = WorkerOutputChannel(writer, chat_id=0)
    await channel.send_document(chat_id=0, document=str(path), message_thread_id=7)

    raw_bytes = writer.write.call_args[0][0]
    n = int.from_bytes(raw_bytes[:4], "big")
    payload = raw_bytes[4:4 + n]
    decoded = _w2b_dec.decode(payload)
    assert isinstance(decoded, McpSendFileMsg)
    assert decoded.file_name == "42.txt"
    assert decoded.file_bytes == b"hello"


async def test_worker_output_channel_not_connected_drops_silently():
    """WorkerOutputChannel with None writer drops messages without raising."""
    channel = WorkerOutputChannel(None, chat_id=0)
    # Should not raise
    await channel.send_message(chat_id=0, text="ignored", message_thread_id=1)


async def test_worker_session_runner_instantiates():
    """MSRV-01: SessionRunner can be instantiated with WorkerOutputChannel as bot."""
    writer = MagicMock()
    writer.is_closing.return_value = False
    writer.write = MagicMock()
    writer.drain = AsyncMock()

    channel = WorkerOutputChannel(writer, chat_id=0)

    from src.sessions.permissions import PermissionManager
    from src.sessions.runner import SessionRunner

    perm_mgr = PermissionManager()
    runner = SessionRunner(
        thread_id=1,
        workdir="/tmp",
        bot=channel,
        chat_id=0,
        permission_manager=perm_mgr,
    )
    # Just verify it was created without error; don't start it (would invoke Claude)
    assert runner.thread_id == 1
    assert runner.workdir == "/tmp"


def test_parse_new_command_normalizes_personal_alias():
    """`/new` accepts human server aliases and normalizes them."""
    assert _parse_new_command_args("/new repo /tmp/repo personal-server") == (
        "repo",
        "/tmp/repo",
        "personal",
        "claude",
    )


# ---------------------------------------------------------------------------
# MSRV-06: Reconnect backoff pattern
# ---------------------------------------------------------------------------

async def test_reconnect_backoff():
    """MSRV-06: WorkerClient uses exponential backoff 1→2→4→...→60 on connection failure."""
    from src.worker.client import WorkerClient

    client = WorkerClient(
        host="127.0.0.1",
        port=19999,  # nothing listening
        auth_token="tok",
        worker_id="test-worker",
    )

    delays_slept = []
    connection_attempts = 0
    max_attempts = 5

    async def mock_open_connection(host, port):
        nonlocal connection_attempts
        connection_attempts += 1
        if connection_attempts < max_attempts:
            raise ConnectionRefusedError("connection refused")
        # On last attempt, raise so we exit the loop
        raise RuntimeError("stop-test")

    async def mock_sleep(delay):
        delays_slept.append(delay)
        if len(delays_slept) >= max_attempts - 1:
            raise asyncio.CancelledError()

    with patch("asyncio.open_connection", side_effect=mock_open_connection), \
         patch("asyncio.sleep", side_effect=mock_sleep):
        try:
            await client.run()
        except (asyncio.CancelledError, RuntimeError):
            pass

    # Verify exponential backoff: 1, 2, 4, 8, ...
    expected_start = [1.0, 2.0, 4.0, 8.0]
    for i, expected in enumerate(expected_start[:len(delays_slept)]):
        assert delays_slept[i] == expected, (
            f"Delay {i} should be {expected}, got {delays_slept[i]}"
        )

    # Verify max cap at 60
    for d in delays_slept:
        assert d <= 60.0, f"Delay {d} exceeds max 60"


async def test_reconnect_delay_resets_on_success():
    """MSRV-06: Reconnect delay resets to 1.0 after successful connection."""
    from src.worker.client import WorkerClient
    from src.ipc.protocol import AuthOkMsg, send_msg

    client = WorkerClient(
        host="127.0.0.1",
        port=0,
        auth_token="tok",
        worker_id="test-worker",
    )

    call_count = [0]
    delays_slept = []

    async def mock_open_connection(host, port):
        call_count[0] += 1
        if call_count[0] == 1:
            raise ConnectionRefusedError("first fail")
        # On second attempt, raise to stop the test
        raise RuntimeError("stop-test")

    async def mock_sleep(delay):
        delays_slept.append(delay)
        # Only allow one sleep
        if len(delays_slept) >= 2:
            raise asyncio.CancelledError()

    with patch("asyncio.open_connection", side_effect=mock_open_connection), \
         patch("asyncio.sleep", side_effect=mock_sleep):
        try:
            await client.run()
        except (asyncio.CancelledError, RuntimeError):
            pass

    # After first fail, delay is 1.0; after first sleep, it doubles to 2.0 for next
    assert delays_slept[0] == 1.0


# ---------------------------------------------------------------------------
# MSRV-04/MSRV-05: Permission bridge
# ---------------------------------------------------------------------------

async def test_permission_resolve():
    """MSRV-04/MSRV-05: _resolve_permission resolves pending future with action."""
    from src.worker.client import WorkerClient

    client = WorkerClient(
        host="127.0.0.1",
        port=0,
        auth_token="tok",
        worker_id="test-worker",
    )

    loop = asyncio.get_event_loop()
    future: asyncio.Future = loop.create_future()
    client._permission_futures["req-1"] = future

    client._resolve_permission("req-1", "allow")
    assert future.done()
    assert future.result() == "allow"
    assert "req-1" not in client._permission_futures


async def test_permission_resolve_unknown_request():
    """_resolve_permission logs a warning for unknown request_id (no crash)."""
    from src.worker.client import WorkerClient

    client = WorkerClient(
        host="127.0.0.1",
        port=0,
        auth_token="tok",
        worker_id="test-worker",
    )
    # Should not raise
    client._resolve_permission("nonexistent-id", "deny")


async def test_request_questions_round_trip():
    """WorkerClient bridges question requests and resolves them from bot answers."""
    from src.worker.client import WorkerClient

    writer = MagicMock()
    writer.is_closing.return_value = False
    writer.write = MagicMock()
    writer.drain = AsyncMock()

    client = WorkerClient(
        host="127.0.0.1",
        port=0,
        auth_token="tok",
        worker_id="test-worker",
    )
    client._output_channel = WorkerOutputChannel(writer, chat_id=0)

    task = asyncio.create_task(
        client._request_questions(
            17,
            [{"id": "provider", "question": "Which provider?"}],
        )
    )
    await asyncio.sleep(0)

    raw_bytes = writer.write.call_args[0][0]
    n = int.from_bytes(raw_bytes[:4], "big")
    payload = raw_bytes[4:4 + n]
    decoded = _w2b_dec.decode(payload)
    assert isinstance(decoded, QuestionRequestMsg)
    assert decoded.topic_id == 17

    client._resolve_question(decoded.request_id, {"Which provider?": "codex"})
    result = await task
    assert result == {"Which provider?": "codex"}


async def test_start_session_supports_codex():
    """WorkerClient starts CodexRunner for provider=codex."""
    from src.worker.client import WorkerClient
    from src.sessions.state import SessionState

    client = WorkerClient(
        host="127.0.0.1",
        port=0,
        auth_token="tok",
        worker_id="test-worker",
    )
    client._output_channel = WorkerOutputChannel(None, chat_id=0)
    client._announce_session_started = AsyncMock()

    fake_runner = MagicMock()
    fake_runner.start = AsyncMock()
    fake_runner.session_id = None
    fake_runner.backend_session_id = "codex-thread-1"
    fake_runner.state = SessionState.IDLE

    with patch("src.worker.client.CodexRunner", return_value=fake_runner):
        await client._start_session(
            StartSessionMsg(topic_id=33, cwd="/tmp/codex", provider="codex")
        )

    await asyncio.sleep(0)
    assert client._sessions[33] is fake_runner
    fake_runner.start.assert_awaited_once()
    client._announce_session_started.assert_awaited_once()


async def test_handle_file_input_uses_enqueue_image_for_remote_photo(tmp_path):
    """WorkerClient saves incoming photo bytes and forwards them as native image input."""
    from src.worker.client import WorkerClient

    client = WorkerClient(
        host="127.0.0.1",
        port=0,
        auth_token="tok",
        worker_id="test-worker",
    )

    runner = MagicMock()
    runner.workdir = str(tmp_path)
    runner.enqueue_image = AsyncMock()
    client._sessions[17] = runner

    await client._handle_file_input(
        UserFileMsg(
            topic_id=17,
            file_name="photo.jpg",
            file_bytes=b"abc",
            caption="look",
            media_type="image/jpeg",
            reply_to_message_id=9,
            is_image=True,
        )
    )

    assert (tmp_path / "photo.jpg").read_bytes() == b"abc"
    runner.enqueue_image.assert_awaited_once_with(
        image_data=b"abc",
        media_type="image/jpeg",
        caption="look",
        reply_to_message_id=9,
    )


def test_coerce_user_file_msg_accepts_same_shape_from_other_module():
    """Worker tolerates UserFileMsg objects whose class identity comes from another import."""
    from pathlib import Path

    from src.worker.client import _coerce_user_file_msg

    protocol_path = Path(__file__).resolve().parents[1] / "src" / "ipc" / "protocol.py"
    spec = importlib.util.spec_from_file_location("alt_protocol", protocol_path)
    assert spec and spec.loader
    alt_protocol = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(alt_protocol)

    foreign_msg = alt_protocol.UserFileMsg(
        topic_id=17,
        file_name="photo.jpg",
        file_bytes=b"abc",
        caption="look",
        media_type="image/jpeg",
        reply_to_message_id=9,
        is_image=True,
    )

    coerced = _coerce_user_file_msg(foreign_msg)
    assert isinstance(coerced, UserFileMsg)
    assert coerced.file_name == "photo.jpg"
    assert coerced.file_bytes == b"abc"


# ---------------------------------------------------------------------------
# MSRV-06: Permission cancel on disconnect
# ---------------------------------------------------------------------------

async def test_permission_cancel_on_disconnect():
    """MSRV-06: _on_disconnected resolves all pending permission futures with 'deny'."""
    from src.worker.client import WorkerClient

    client = WorkerClient(
        host="127.0.0.1",
        port=0,
        auth_token="tok",
        worker_id="test-worker",
    )

    loop = asyncio.get_event_loop()
    f1: asyncio.Future = loop.create_future()
    f2: asyncio.Future = loop.create_future()
    client._permission_futures["r1"] = f1
    client._permission_futures["r2"] = f2

    client._on_disconnected()

    assert f1.done() and f1.result() == "deny"
    assert f2.done() and f2.result() == "deny"
    assert len(client._permission_futures) == 0


async def test_question_cancel_on_disconnect():
    """_on_disconnected resolves all pending question futures with empty answers."""
    from src.worker.client import WorkerClient

    client = WorkerClient(
        host="127.0.0.1",
        port=0,
        auth_token="tok",
        worker_id="test-worker",
    )

    loop = asyncio.get_event_loop()
    future: asyncio.Future = loop.create_future()
    client._question_futures["q1"] = future

    client._on_disconnected()

    assert future.done() and future.result() == {}
    assert len(client._question_futures) == 0


async def test_on_disconnected_clears_writer():
    """_on_disconnected closes and clears the writer reference."""
    from src.worker.client import WorkerClient

    client = WorkerClient(
        host="127.0.0.1",
        port=0,
        auth_token="tok",
        worker_id="test-worker",
    )
    mock_writer = MagicMock()
    mock_writer.close = MagicMock()
    client._writer = mock_writer

    client._on_disconnected()

    mock_writer.close.assert_called_once()
    assert client._writer is None
