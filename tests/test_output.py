"""Tests for split_message and TypingIndicator (STAT-03, STAT-04, STAT-05)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, call

import pytest
from aiogram.exceptions import TelegramBadRequest

from src.bot.output import (
    TypingIndicator,
    edit_html_message,
    send_html_message,
    split_message,
    strip_html_markup,
)
from src.sessions.questions import format_question_message


# ---------------------------------------------------------------------------
# split_message tests (STAT-03, STAT-04)
# ---------------------------------------------------------------------------


def test_short_message_unchanged():
    """Text shorter than max_len is returned as a single-element list."""
    text = "Hello, world!"
    result = split_message(text)
    assert result == [text]


def test_empty_message():
    """Empty string returns ['']."""
    result = split_message("")
    assert result == [""]


def test_exact_boundary():
    """Text of exactly max_len characters is NOT split."""
    text = "a" * 4096
    result = split_message(text)
    assert result == [text]


def test_split_at_newline():
    """Text with a newline before the boundary splits at the last newline."""
    # Build a string: 4000 'a' chars + newline + 999 'b' chars = 5000 total
    text = "a" * 4000 + "\n" + "b" * 999
    result = split_message(text)
    assert len(result) == 2
    # First chunk ends at the newline boundary (newline not included)
    assert result[0] == "a" * 4000
    assert result[1] == "b" * 999
    # Both chunks within limit
    assert all(len(chunk) <= 4096 for chunk in result)


def test_split_preserves_code_block():
    """Code-block boundary (\\n```) is preferred over a bare newline split."""
    # Construct: content before code block, \n``` marker, then more content.
    # Keep total > 4096 so a split happens.
    before_block = "x" * 3000
    code_block_end = "\n```"
    after_block = "y" * 2000  # pushes total over 4096
    text = before_block + code_block_end + after_block

    result = split_message(text)
    # First chunk should end with the \n``` boundary
    assert result[0].endswith("\n```")
    # No chunk exceeds max_len
    assert all(len(chunk) <= 4096 for chunk in result)


def test_split_multiple_chunks():
    """10 000-char string (no newlines) splits into 3+ chunks all <= 4096."""
    text = "z" * 10_000
    result = split_message(text)
    assert len(result) >= 3
    assert all(len(chunk) <= 4096 for chunk in result)
    # Reassembled text equals original
    assert "".join(result) == text


def test_hard_split_no_newline():
    """Single 5000-char string with no newlines splits at hard boundary (4096)."""
    text = "q" * 5000
    result = split_message(text)
    assert len(result) == 2
    assert result[0] == "q" * 4096
    assert result[1] == "q" * 904
    assert all(len(chunk) <= 4096 for chunk in result)


# ---------------------------------------------------------------------------
# TypingIndicator tests (STAT-05)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_typing_sends_action():
    """TypingIndicator calls send_chat_action with correct parameters."""
    bot = AsyncMock()
    bot.send_chat_action = AsyncMock()

    indicator = TypingIndicator(bot=bot, chat_id=100, thread_id=5)
    await indicator.start()

    # Let the loop tick at least once (it sends immediately on first iteration)
    await asyncio.sleep(0.05)

    await indicator.stop()

    # Verify send_chat_action was called with expected keyword arguments
    bot.send_chat_action.assert_called_with(
        chat_id=100,
        action="typing",
        message_thread_id=5,
    )


@pytest.mark.asyncio
async def test_typing_stop_cancels_task():
    """After stop(), the internal asyncio task is done (cancelled)."""
    bot = AsyncMock()
    bot.send_chat_action = AsyncMock()

    indicator = TypingIndicator(bot=bot, chat_id=200, thread_id=10)
    await indicator.start()

    # Grab a reference to the task before stop clears it
    task = indicator._task
    assert task is not None

    await indicator.stop()

    # Task should be done and internal reference cleared
    assert task.done()
    assert indicator._task is None


@pytest.mark.asyncio
async def test_send_html_message_falls_back_to_plain_text():
    """Malformed HTML should retry as plain text instead of bubbling TelegramBadRequest."""
    bot = AsyncMock()
    sent = AsyncMock()
    bot.send_message = AsyncMock(
        side_effect=[
            TelegramBadRequest(
                method=AsyncMock(),
                message='Bad Request: can\'t parse entities: Unsupported start tag "15%" at byte offset 2048',
            ),
            sent,
        ]
    )

    result = await send_html_message(bot, chat_id=1, text="Value: <15%>")

    assert result is sent
    assert bot.send_message.await_count == 2
    assert bot.send_message.await_args_list[0].kwargs["parse_mode"] == "HTML"
    assert bot.send_message.await_args_list[1].kwargs["text"] == "Value: <15%>"
    assert "parse_mode" not in bot.send_message.await_args_list[1].kwargs


@pytest.mark.asyncio
async def test_edit_html_message_falls_back_to_plain_text():
    """Malformed HTML edits should also retry without parse_mode."""
    bot = AsyncMock()
    edited = AsyncMock()
    bot.edit_message_text = AsyncMock(
        side_effect=[
            TelegramBadRequest(
                method=AsyncMock(),
                message='Bad Request: can\'t parse entities: Unsupported start tag "15%" at byte offset 2048',
            ),
            edited,
        ]
    )

    result = await edit_html_message(bot, chat_id=1, message_id=2, text="CPU <15%>")

    assert result is edited
    assert bot.edit_message_text.await_count == 2
    assert bot.edit_message_text.await_args_list[0].kwargs["parse_mode"] == "HTML"
    assert bot.edit_message_text.await_args_list[1].kwargs["text"] == "CPU <15%>"


def test_strip_html_markup_unescapes_visible_text():
    """Fallback plain-text rendering should keep the user-visible content."""
    assert strip_html_markup("Session <b>demo</b> in <code>/tmp</code>") == "Session demo in /tmp"
    assert strip_html_markup("Value: <15%>") == "Value: <15%>"


def test_question_message_escapes_dynamic_html():
    """Question UI must escape headers, text, labels, and descriptions."""
    rendered = format_question_message(
        {
            "header": "CPU <15%>",
            "question": "Pick <mode>",
            "options": [
                {"label": "safe <ok>", "description": "use <low>"},
            ],
        }
    )
    assert "&lt;15%&gt;" in rendered
    assert "Pick &lt;mode&gt;" in rendered
    assert "safe &lt;ok&gt;" in rendered
    assert "use &lt;low&gt;" in rendered
