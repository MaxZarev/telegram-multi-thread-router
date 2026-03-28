"""Tests for idle message draining in SessionRunner."""

from unittest.mock import AsyncMock, MagicMock

from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

from src.sessions.permissions import PermissionManager
from src.sessions.runner import SessionRunner


async def test_runner_drain_non_turn_messages_flushes_delayed_output():
    """Late Claude messages should be emitted during idle, not on the next prompt."""
    bot = AsyncMock()
    bot.send_message = AsyncMock(return_value=MagicMock(message_id=1))

    runner = SessionRunner(
        thread_id=10,
        workdir="/tmp",
        bot=bot,
        chat_id=5,
        permission_manager=PermissionManager(),
    )

    async def _stream():
        yield AssistantMessage(
            content=[TextBlock("late background result")],
            model="claude-sonnet-4-6",
        )

    runner._message_stream = _stream()

    await runner._drain_non_turn_messages()

    bot.send_message.assert_awaited_once()
    assert bot.send_message.await_args.kwargs["text"] == "late background result"
    assert bot.send_message.await_args.kwargs["parse_mode"] == "Markdown"


async def test_runner_drain_non_turn_messages_suppresses_output_after_telegram_mcp():
    """Idle Claude output should not duplicate text after MCP already replied in that turn."""
    bot = AsyncMock()
    bot.send_message = AsyncMock(return_value=MagicMock(message_id=1))

    runner = SessionRunner(
        thread_id=10,
        workdir="/tmp",
        bot=bot,
        chat_id=5,
        permission_manager=PermissionManager(),
    )
    runner._turn_used_telegram_output = True

    async def _stream():
        yield AssistantMessage(
            content=[TextBlock("late duplicate result")],
            model="claude-sonnet-4-6",
        )

    runner._message_stream = _stream()

    await runner._drain_non_turn_messages()

    bot.send_message.assert_not_awaited()


async def test_runner_drain_response_suppresses_final_text_after_telegram_mcp(monkeypatch):
    """Final AssistantMessage text should not be sent if MCP already replied in the same turn."""
    monkeypatch.setattr("src.sessions.runner.settings.stream_intermediate_messages", False)

    bot = AsyncMock()
    bot.send_message = AsyncMock(return_value=MagicMock(message_id=1))

    runner = SessionRunner(
        thread_id=10,
        workdir="/tmp",
        bot=bot,
        chat_id=5,
        permission_manager=PermissionManager(),
    )
    runner._turn_used_telegram_output = True
    runner._current_reply_to = 77
    runner._status = MagicMock()
    runner._status.track_usage = MagicMock()
    runner._status.finalize = AsyncMock()
    status = runner._status

    async def _stream():
        yield AssistantMessage(
            content=[TextBlock("duplicate final text")],
            model="claude-sonnet-4-6",
        )
        yield ResultMessage(
            subtype="success",
            duration_ms=123,
            duration_api_ms=120,
            is_error=False,
            num_turns=1,
            session_id="sess-1",
            total_cost_usd=0.01,
        )

    runner._message_stream = _stream()

    await runner._drain_response(client=None)

    bot.send_message.assert_not_awaited()
    status.finalize.assert_awaited_once()
