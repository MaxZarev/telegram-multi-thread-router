"""Unit tests for the local Codex app-server runner."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

from src.sessions.codex_runner import CodexRunner
from src.sessions.state import SessionState


class _FakeClient:
    def __init__(self, messages: list[dict] | None = None) -> None:
        self.messages = list(messages or [])
        self.requests: list[tuple[str, dict]] = []
        self.responses: list[tuple[int | str, dict]] = []

    async def request(self, method: str, params: dict) -> dict:
        self.requests.append((method, params))
        if method == "thread/start":
            return {"thread": {"id": "thread-123"}}
        if method == "thread/resume":
            return {"thread": {"id": params["threadId"]}}
        if method == "turn/start":
            return {"turn": {"id": "turn-123"}}
        if method == "turn/interrupt":
            return {}
        return {}

    async def next_message(self) -> dict:
        if not self.messages:
            await asyncio.sleep(3600)
        return self.messages.pop(0)

    async def respond(self, request_id: int | str, result: dict) -> None:
        self.responses.append((request_id, result))


async def test_build_config_overrides_includes_mcp_urls():
    runner = CodexRunner(
        thread_id=1,
        workdir="/tmp",
        bot=AsyncMock(),
        chat_id=1,
        mcp_server_urls={
            "orchestrator": "http://127.0.0.1:8765/sse",
            "files": "http://127.0.0.1:8766/sse",
        },
    )

    overrides = runner._build_config_overrides()

    assert overrides == [
        'mcp_servers.files.url="http://127.0.0.1:8766/sse"',
        'mcp_servers.orchestrator.url="http://127.0.0.1:8765/sse"',
    ]


async def test_ensure_thread_starts_new_thread_with_prompt_overrides(monkeypatch):
    client = _FakeClient()
    runner = CodexRunner(
        thread_id=10,
        workdir="/tmp",
        bot=AsyncMock(),
        chat_id=5,
        model="gpt-5.4",
        base_instructions="base",
        developer_instructions="dev",
    )
    runner._client = client
    update_backend_session_id = AsyncMock()
    monkeypatch.setattr("src.sessions.codex_runner.update_backend_session_id", update_backend_session_id)

    await runner._ensure_thread()

    assert client.requests == [
        (
            "thread/start",
            {
                "cwd": "/tmp",
                "approvalPolicy": "on-request",
                "approvalsReviewer": "user",
                "personality": "pragmatic",
                "baseInstructions": "base",
                "developerInstructions": "dev",
                "model": "gpt-5.4",
            },
        )
    ]
    assert runner.backend_session_id == "thread-123"
    update_backend_session_id.assert_called_once_with(10, "thread-123")


async def test_run_turn_updates_backend_session_and_sends_text(monkeypatch):
    sent = []

    async def _send_message(**kwargs):
        sent.append(kwargs)
        return MagicMock(message_id=1)

    bot = AsyncMock()
    bot.send_message = AsyncMock(side_effect=_send_message)
    runner = CodexRunner(thread_id=10, workdir="/tmp", bot=bot, chat_id=5)
    runner.backend_session_id = "thread-123"
    runner._current_reply_to = 99
    runner._status = MagicMock()
    runner._status.track_usage = MagicMock()
    runner._status.finalize = AsyncMock()
    status = runner._status
    runner._client = _FakeClient(
        messages=[
            {"method": "thread/started", "params": {"thread": {"id": "thread-123"}}},
            {
                "method": "item/completed",
                "params": {
                    "item": {
                        "type": "agentMessage",
                        "content": [{"type": "outputText", "text": "hello from codex"}],
                    }
                },
            },
            {"method": "turn/completed", "params": {"turn": {"status": "completed"}}},
        ]
    )
    update_backend_session_id = AsyncMock()
    monkeypatch.setattr("src.sessions.codex_runner.update_backend_session_id", update_backend_session_id)

    await runner._run_turn("hello")

    assert runner._client.requests[0] == (
        "turn/start",
        {
            "threadId": "thread-123",
            "input": [{"type": "text", "text": "hello"}],
            "cwd": "/tmp",
            "approvalPolicy": "on-request",
            "approvalsReviewer": "user",
        },
    )
    assert sent[-1]["text"] == "hello from codex"
    assert sent[-1]["reply_to_message_id"] == 99
    status.finalize.assert_called_once()
    update_backend_session_id.assert_not_called()


async def test_run_turn_streams_codex_delta_when_enabled(monkeypatch):
    sent = []

    async def _send_message(**kwargs):
        sent.append(kwargs)
        return MagicMock(message_id=len(sent))

    monkeypatch.setattr("src.sessions.codex_runner.settings.stream_intermediate_messages", True)

    bot = AsyncMock()
    bot.send_message = AsyncMock(side_effect=_send_message)
    runner = CodexRunner(thread_id=10, workdir="/tmp", bot=bot, chat_id=5)
    runner.backend_session_id = "thread-123"
    runner._current_reply_to = 99
    runner._status = MagicMock()
    runner._status.track_usage = MagicMock()
    runner._status.finalize = AsyncMock()
    runner._client = _FakeClient(
        messages=[
            {
                "method": "item/agentMessage/delta",
                "params": {"itemId": "msg-1", "delta": "hello from codex.\n"},
            },
            {
                "method": "item/completed",
                "params": {"item": {"id": "msg-1", "type": "agentMessage"}},
            },
            {"method": "turn/completed", "params": {"turn": {"status": "completed"}}},
        ]
    )

    await runner._run_turn("hello")

    assert len(sent) == 1
    assert sent[0]["text"] == "hello from codex.\n"
    assert sent[0]["reply_to_message_id"] == 99


async def test_run_turn_buffers_codex_delta_when_streaming_disabled(monkeypatch):
    sent = []

    async def _send_message(**kwargs):
        sent.append(kwargs)
        return MagicMock(message_id=len(sent))

    monkeypatch.setattr("src.sessions.codex_runner.settings.stream_intermediate_messages", False)

    bot = AsyncMock()
    bot.send_message = AsyncMock(side_effect=_send_message)
    runner = CodexRunner(thread_id=10, workdir="/tmp", bot=bot, chat_id=5)
    runner.backend_session_id = "thread-123"
    runner._current_reply_to = 99
    runner._status = MagicMock()
    runner._status.track_usage = MagicMock()
    runner._status.finalize = AsyncMock()
    runner._client = _FakeClient(
        messages=[
            {
                "method": "item/agentMessage/delta",
                "params": {"itemId": "msg-1", "delta": "hello"},
            },
            {
                "method": "item/completed",
                "params": {
                    "item": {
                        "id": "msg-1",
                        "type": "agentMessage",
                        "content": [{"type": "outputText", "text": "hello from codex"}],
                    }
                },
            },
            {"method": "turn/completed", "params": {"turn": {"status": "completed"}}},
        ]
    )

    await runner._run_turn("hello")

    assert len(sent) == 1
    assert sent[0]["text"] == "hello from codex"
    assert sent[0]["reply_to_message_id"] == 99


async def test_interrupt_requests_turn_interrupt():
    runner = CodexRunner(thread_id=1, workdir="/tmp", bot=AsyncMock(), chat_id=1)
    runner.state = SessionState.RUNNING
    runner.backend_session_id = "thread-123"
    runner._current_turn_id = "turn-123"
    runner._client = _FakeClient()

    interrupted = await runner.interrupt()

    assert interrupted is True
    assert runner._client.requests[-1] == (
        "turn/interrupt",
        {"threadId": "thread-123", "turnId": "turn-123"},
    )


async def test_enqueue_image_starts_turn_with_native_image_input():
    bot = AsyncMock()
    runner = CodexRunner(thread_id=10, workdir="/tmp", bot=bot, chat_id=5)
    runner.backend_session_id = "thread-123"
    runner._current_reply_to = 99
    runner._status = MagicMock()
    runner._status.track_usage = MagicMock()
    runner._status.finalize = AsyncMock()
    runner._client = _FakeClient(
        messages=[
            {"method": "turn/completed", "params": {"turn": {"status": "completed"}}},
        ]
    )

    await runner.enqueue_image(b"abc", caption="see this", reply_to_message_id=99)
    item = await runner._message_queue.get()
    await runner._run_turn(prompt=item.text, input_items=item.input_items)

    method, params = runner._client.requests[0]
    assert method == "turn/start"
    assert params["input"][0]["type"] == "image"
    assert params["input"][0]["url"].startswith("data:image/jpeg;base64,")
    assert params["input"][1] == {"type": "text", "text": "see this"}


async def test_run_starts_session_telegram_mcp_server(monkeypatch):
    bot = AsyncMock()
    started = []
    stopped = []

    class _FakeTelegramMcpServer:
        def __init__(self, bot_arg, chat_id: int, thread_id: int) -> None:
            assert bot_arg is bot
            assert chat_id == 5
            assert thread_id == 10

        async def start(self) -> str:
            started.append(True)
            return "http://127.0.0.1:8767/mcp"

        async def stop(self) -> None:
            stopped.append(True)

    fake_client = MagicMock()
    fake_client.start = AsyncMock()
    fake_client.close = AsyncMock()

    async def _next_message():
        await asyncio.sleep(0)
        raise asyncio.CancelledError()

    fake_client.next_message = AsyncMock(side_effect=_next_message)

    monkeypatch.setattr(
        "src.sessions.codex_runner.LocalTelegramOutputMcpServer",
        _FakeTelegramMcpServer,
    )
    client_ctor = MagicMock(return_value=fake_client)
    monkeypatch.setattr("src.sessions.codex_runner.CodexAppServerClient", client_ctor)

    runner = CodexRunner(thread_id=10, workdir="/tmp", bot=bot, chat_id=5)
    task = asyncio.create_task(runner._run())
    await asyncio.sleep(0)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert started == [True]
    assert stopped == [True]
    _, kwargs = client_ctor.call_args
    assert kwargs["config_overrides"] == ['mcp_servers.telegram.url="http://127.0.0.1:8767/mcp"']


async def test_codex_permission_always_persists_globally():
    perm_mgr = MagicMock()
    perm_mgr.is_globally_allowed.return_value = False
    perm_mgr.create_request.return_value = ("req-1", asyncio.get_running_loop().create_future())
    perm_mgr.create_request.return_value[1].set_result("always")
    perm_mgr.allow_globally = AsyncMock()

    bot = AsyncMock()
    bot.send_message = AsyncMock(return_value=MagicMock(message_id=1))
    runner = CodexRunner(thread_id=10, workdir="/tmp", bot=bot, chat_id=5, permission_manager=perm_mgr)

    result = await runner._ask_telegram_permission(
        "item/commandExecution/requestApproval",
        {"command": "ls"},
    )

    assert result == {"decision": "acceptForSession"}
    perm_mgr.allow_globally.assert_awaited_once_with("Bash")


async def test_codex_permission_profile_uses_global_allow():
    perm_mgr = MagicMock()
    perm_mgr.is_globally_allowed.side_effect = lambda tool: tool == "request_permissions"
    perm_mgr.allow_globally = AsyncMock()

    runner = CodexRunner(
        thread_id=10,
        workdir="/tmp",
        bot=AsyncMock(),
        chat_id=5,
        permission_manager=perm_mgr,
    )

    result = await runner._ask_telegram_permissions_profile({"permissions": {"network": "on"}})

    assert result == {"scope": "session", "permissions": {"network": "on"}}
