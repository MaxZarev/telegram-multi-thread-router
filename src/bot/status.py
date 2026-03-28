"""StatusUpdater — manages one editable status message per session turn."""

from __future__ import annotations

import asyncio
import html
import logging
import time

from aiogram import Bot
from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest

from src.usage import fetch_usage, format_usage

logger = logging.getLogger(__name__)

# Minimum interval between status edits (Telegram counts edits as messages for rate limits)
MIN_EDIT_INTERVAL = 30.0


def _truncate(text: str, max_len: int = 60) -> str:
    """Truncate text and HTML-escape it for safe display."""
    t = text.replace("\n", " ").strip()
    if len(t) > max_len:
        t = t[:max_len] + "…"
    return html.escape(t)


def _format_tokens(n: int) -> str:
    """Format token count: 1234 → '1.2K', 123456 → '123K', 1234567 → '1.2M'."""
    if n < 1000:
        return str(n)
    elif n < 100_000:
        return f"{n / 1000:.1f}K"
    elif n < 1_000_000:
        return f"{n // 1000}K"
    else:
        return f"{n / 1_000_000:.1f}M"


def _short_model(model: str) -> str:
    """Shorten model name: 'claude-opus-4-6' → 'opus-4-6', 'claude-sonnet-4-6' → 'sonnet-4-6'."""
    return html.escape(model.replace("claude-", ""))


# All current Claude models have 1M context window (GA since March 2026)
_CONTEXT_LIMITS: dict[str, int] = {}
_DEFAULT_CONTEXT_LIMIT = 1_000_000


def _get_context_limit(model: str | None) -> int:
    """Get context window size for a model. Defaults to 1M."""
    if not model:
        return _DEFAULT_CONTEXT_LIMIT
    return _CONTEXT_LIMITS.get(model, _DEFAULT_CONTEXT_LIMIT)


class StatusUpdater:
    """Manages one editable Telegram status message per session turn.

    Uses event-driven updates with throttling instead of fixed interval polling.
    First tool call triggers an immediate update, subsequent updates are throttled
    to MIN_EDIT_INTERVAL seconds to avoid Telegram rate limits.

    Lifecycle:
      start_turn()    — sends initial "Working..." message
      track_tool()    — updates current tool info, triggers throttled edit
      track_usage()   — updates token/model/effort from AssistantMessage
      finalize()      — edits message to cost/duration summary, schedules 30s deletion
      stop()          — cancels pending edits, clears message_id
    """

    def __init__(
        self,
        bot: Bot,
        chat_id: int,
        thread_id: int,
        session_id: str | None = None,
        model: str | None = None,
        effort: str | None = None,
    ) -> None:
        self._bot = bot
        self._chat_id = chat_id
        self._thread_id = thread_id

        self._message_id: int | None = None
        self._current_tool: str | None = None
        self._tool_summary: str | None = None
        self._tool_count: int = 0
        self._start_time: float | None = None
        self._last_edit_time: float = 0.0
        self._pending_edit: asyncio.TimerHandle | None = None
        self._edit_lock = asyncio.Lock()
        self._watchdog_note: str | None = None

        # Session metadata
        self._session_id = session_id
        self._model = model
        self._effort = effort

        # Token usage (cumulative within turn from latest AssistantMessage)
        self._input_tokens: int = 0
        self._output_tokens: int = 0
        self._cache_read_tokens: int = 0
        self._cache_creation_tokens: int = 0

    async def start_turn(self) -> None:
        """Send initial status message."""
        self._start_time = time.monotonic()
        self._tool_count = 0
        self._current_tool = None
        self._tool_summary = None
        self._last_edit_time = 0.0
        self._watchdog_note = None
        self._input_tokens = 0
        self._output_tokens = 0
        self._cache_read_tokens = 0
        self._cache_creation_tokens = 0

        # Build initial message with session info
        lines = ["⚡ Working..."]
        if self._model:
            meta = f"🤖 {_short_model(self._model)}"
            if self._effort:
                meta += f" · {self._effort}"
            lines.append(meta)

        sent = await self._bot.send_message(
            chat_id=self._chat_id,
            message_thread_id=self._thread_id,
            text="\n".join(lines),
            parse_mode="HTML",
        )
        self._message_id = sent.message_id

    def track_tool(self, tool_name: str, input_data: dict | None = None) -> None:
        """Record the currently executing tool and schedule a status edit."""
        self._current_tool = tool_name
        self._tool_count += 1

        if input_data:
            self._tool_summary = _build_tool_summary(tool_name, input_data)
        else:
            self._tool_summary = None

        self._schedule_edit()

    def track_usage(
        self,
        usage: dict | None = None,
        model: str | None = None,
    ) -> None:
        """Update token usage and model from an AssistantMessage."""
        if model and model != self._model:
            self._model = model

        if usage:
            self._input_tokens = usage.get("input_tokens", self._input_tokens)
            self._output_tokens = usage.get("output_tokens", self._output_tokens)
            self._cache_read_tokens = usage.get(
                "cache_read_input_tokens", self._cache_read_tokens
            )
            self._cache_creation_tokens = usage.get(
                "cache_creation_input_tokens", self._cache_creation_tokens
            )

    async def show_watchdog_notice(self, silent_for_s: int) -> None:
        """Update the status message to reflect prolonged silence without alarming the user."""
        if self._message_id is None:
            return

        minutes = max(1, round(silent_for_s / 60))
        self._watchdog_note = (
            f"⏳ No updates for {minutes}m. Long-running tools can be quiet."
        )

        if self._pending_edit is not None:
            self._pending_edit.cancel()
            self._pending_edit = None

        async with self._edit_lock:
            await self._edit_status()
            self._last_edit_time = time.monotonic()

    def clear_watchdog_notice(self) -> None:
        """Clear any previously shown watchdog note."""
        self._watchdog_note = None

    def _schedule_edit(self) -> None:
        """Schedule a status edit, respecting the throttle interval."""
        now = time.monotonic()
        elapsed_since_edit = now - self._last_edit_time

        if elapsed_since_edit >= MIN_EDIT_INTERVAL:
            asyncio.get_running_loop().call_soon(
                lambda: asyncio.create_task(self._do_edit())
            )
        elif self._pending_edit is None:
            delay = MIN_EDIT_INTERVAL - elapsed_since_edit
            loop = asyncio.get_running_loop()
            self._pending_edit = loop.call_later(
                delay, lambda: asyncio.create_task(self._do_edit())
            )

    async def _do_edit(self) -> None:
        """Actually perform the status edit."""
        self._pending_edit = None
        async with self._edit_lock:
            await self._edit_status()
            self._last_edit_time = time.monotonic()

    async def _edit_status(self) -> None:
        """Edit the status message with current progress info."""
        if self._message_id is None:
            return

        elapsed = self._format_elapsed()

        lines = [f"⚡ Working... <b>{elapsed}</b>"]

        # Model + effort line
        if self._model:
            meta = f"🤖 {_short_model(self._model)}"
            if self._effort:
                meta += f" · {self._effort}"
            lines.append(meta)

        # Current tool
        if self._current_tool:
            tool_line = f"🔧 {self._current_tool}"
            if self._tool_summary:
                tool_line += f": {self._tool_summary}"
            lines.append(tool_line)

        # Stats: tool calls + context usage with %
        stats = f"📊 {self._tool_count} tools"
        total_ctx = self._input_tokens + self._cache_read_tokens + self._cache_creation_tokens + self._output_tokens
        if total_ctx:
            limit = _get_context_limit(self._model)
            pct = total_ctx / limit * 100
            stats += f" · {_format_tokens(total_ctx)} ctx ({pct:.0f}%)"
        lines.append(stats)

        # Rate limits from API (cached, non-blocking)
        try:
            usage = await fetch_usage()
            if usage:
                usage_str = format_usage(usage)
                if usage_str:
                    lines.append(f"📉 {usage_str}")
        except Exception:
            pass

        if self._watchdog_note:
            lines.append(self._watchdog_note)

        status_text = "\n".join(lines)

        for attempt in range(2):
            try:
                await self._bot.edit_message_text(
                    text=status_text,
                    chat_id=self._chat_id,
                    message_id=self._message_id,
                    parse_mode="HTML",
                )
                return
            except TelegramRetryAfter as e:
                if attempt == 0:
                    await asyncio.sleep(e.retry_after)
                else:
                    logger.warning("StatusUpdater: retry after flood control failed, skipping edit")
                    return
            except TelegramBadRequest:
                return
            except Exception as e:
                logger.warning("StatusUpdater: unexpected error editing status: %s", e)
                return

    def _format_elapsed(self) -> str:
        """Format elapsed time as 'Xm Ys'."""
        if self._start_time is None:
            return "0s"
        elapsed_s = int(time.monotonic() - self._start_time)
        if elapsed_s < 60:
            return f"{elapsed_s}s"
        minutes = elapsed_s // 60
        seconds = elapsed_s % 60
        return f"{minutes}m {seconds}s"

    async def finalize(self, cost_usd: float | None, duration_ms: int, tool_count: int) -> None:
        """Show cost/duration summary with context info, schedule deletion in 30s."""
        if self._pending_edit is not None:
            self._pending_edit.cancel()
            self._pending_edit = None
        self._watchdog_note = None

        if self._message_id is not None:
            cost_str = f"${cost_usd:.4f}" if cost_usd is not None else "n/a"
            duration_str = f"{duration_ms / 1000:.1f}s"

            lines = ["✅ Done"]

            # Model + effort
            if self._model:
                meta = f"🤖 {_short_model(self._model)}"
                if self._effort:
                    meta += f" · {self._effort}"
                lines.append(meta)

            # Cost, duration, tools
            lines.append(f"💰 {cost_str} | ⏱ {duration_str} | 🔧 {tool_count} tools")

            # Context usage with %
            total_ctx = self._input_tokens + self._cache_read_tokens + self._cache_creation_tokens + self._output_tokens
            if total_ctx:
                limit = _get_context_limit(self._model)
                pct = total_ctx / limit * 100
                ctx_line = f"📝 {_format_tokens(total_ctx)} ctx ({pct:.0f}%)"
                if self._cache_read_tokens:
                    ctx_line += f" · {_format_tokens(self._cache_read_tokens)} cached"
                lines.append(ctx_line)

            # Rate limits from API (always available, not just on SDK events)
            try:
                usage = await fetch_usage()
                if usage:
                    usage_str = format_usage(usage)
                    if usage_str:
                        lines.append(f"📉 {usage_str}")
            except Exception:
                pass

            summary_text = "\n".join(lines)
            try:
                await self._bot.edit_message_text(
                    text=summary_text,
                    chat_id=self._chat_id,
                    message_id=self._message_id,
                )
            except TelegramBadRequest:
                pass
            except Exception as e:
                logger.warning("StatusUpdater: failed to finalize status message: %s", e)

            asyncio.get_running_loop().call_later(
                30, lambda: asyncio.create_task(self._delete_status())
            )

    async def _delete_status(self) -> None:
        """Delete the status message, ignoring any errors."""
        if self._message_id is not None:
            try:
                await self._bot.delete_message(self._chat_id, self._message_id)
            except Exception:
                pass
            self._message_id = None

    async def stop(self) -> None:
        """Cancel any pending edits and clear the tracked message id."""
        if self._pending_edit is not None:
            self._pending_edit.cancel()
            self._pending_edit = None
        self._watchdog_note = None
        self._message_id = None


def _build_tool_summary(tool_name: str, input_data: dict) -> str:
    """Build a short human-readable summary of a tool call."""
    if tool_name == "Bash":
        cmd = input_data.get("command", "")
        return f"<code>{_truncate(cmd, 80)}</code>"
    elif tool_name == "Read":
        path = input_data.get("file_path", "")
        return f"<code>{_truncate(path, 80)}</code>"
    elif tool_name in ("Edit", "Write"):
        path = input_data.get("file_path", "")
        return f"<code>{_truncate(path, 80)}</code>"
    elif tool_name == "Grep":
        pattern = input_data.get("pattern", "")
        return f"<code>{_truncate(pattern, 40)}</code>"
    elif tool_name == "Glob":
        pattern = input_data.get("pattern", "")
        return f"<code>{_truncate(pattern, 40)}</code>"
    elif tool_name == "Agent":
        desc = input_data.get("description", input_data.get("prompt", ""))
        return _truncate(str(desc), 60)
    elif tool_name == "WebSearch":
        query = input_data.get("query", "")
        return _truncate(str(query), 60)
    elif tool_name == "WebFetch":
        url = input_data.get("url", "")
        return f"<code>{_truncate(str(url), 60)}</code>"
    else:
        for key in ("prompt", "query", "command", "pattern", "file_path", "description"):
            if key in input_data:
                return _truncate(str(input_data[key]), 60)
        return ""
