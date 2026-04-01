"""SessionRunner — owns one ClaudeSDKClient per session with state machine and message queue."""

from __future__ import annotations

import asyncio
import contextlib
import html
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable

from claude_agent_sdk import (
    ClaudeSDKClient,
    ClaudeAgentOptions,
    AssistantMessage,
    ResultMessage,
    SystemMessage,
    TextBlock,
    ToolUseBlock,
    PermissionResultAllow,
    PermissionResultDeny,
    PermissionUpdate,
    HookMatcher,
)
from claude_agent_sdk.types import (
    PermissionRuleValue,
    RateLimitEvent,
    TaskStartedMessage,
    TaskProgressMessage,
    TaskNotificationMessage,
)
from aiogram import Bot
from aiogram.exceptions import TelegramRetryAfter

from src.config import settings
from src.sessions.state import SessionState
from src.sessions.backend import SessionProvider, looks_like_provider_limit_error
from src.sessions.permissions import PermissionManager, build_permission_keyboard, format_permission_message
from src.sessions.questions import QuestionManager, build_question_keyboard, format_question_message
from src.sessions.mcp_tools import create_telegram_mcp_server
from src.db.queries import update_session_id, update_session_model, update_session_state
from src.bot.status import StatusUpdater
from src.bot.output import TypingIndicator, escape_markdown_html, send_html_message, split_message

logger = logging.getLogger(__name__)


@dataclass
class _QueueItem:
    """A queued user message with optional reply tracking."""
    text: str | None  # None = stop sentinel
    reply_to_message_id: int | None = None
    content_blocks: list | None = None  # For image messages: list of Anthropic content blocks


async def _dummy_pretool_hook(input_data, tool_use_id, context):
    """Required PreToolUse hook — without this, can_use_tool never fires (SDK issue #18735)."""
    tool_name = context.get("tool_name", "unknown") if isinstance(context, dict) else getattr(context, "tool_name", "unknown")
    logger.debug("PreToolUse hook fired for tool: %s", tool_name)
    return {"continue_": True}


async def _make_ask_user_hook(runner: SessionRunner):
    """Create a PreToolUse hook that intercepts AskUserQuestion and proxies it to Telegram."""

    async def _hook(input_data, tool_use_id, context):
        logger.info("AskUserQuestion hook fired! thread=%d input_keys=%s", runner.thread_id, list(input_data.keys()))
        if runner._question_manager is None:
            logger.warning("AskUserQuestion hook: no question_manager, passing through")
            return {"continue_": True}

        questions = input_data.get("questions", [])
        if not questions:
            logger.warning("AskUserQuestion hook: no questions in input_data")
            return {"continue_": True}

        request_id, future = runner._question_manager.create_request(questions)

        # Send each question as a separate Telegram message with inline buttons
        for i, question in enumerate(questions):
            text = format_question_message(question)
            keyboard = build_question_keyboard(request_id, i, question)
            try:
                sent = await send_html_message(
                    runner._bot,
                    chat_id=runner._chat_id,
                    message_thread_id=runner.thread_id,
                    text=text,
                    reply_markup=keyboard,
                )
                runner._question_manager.add_message_id(request_id, sent.message_id)
            except Exception as e:
                logger.error("Failed to send question %d: %s", i, e)

        # Wait for user to answer all questions
        try:
            answers = await asyncio.wait_for(future, timeout=300.0)
        except asyncio.TimeoutError:
            runner._question_manager.expire(request_id)
            await runner._bot.send_message(
                chat_id=runner._chat_id,
                message_thread_id=runner.thread_id,
                text="⏱ Questions timed out",
            )
            return {"decision": "block", "reason": "User did not respond within 5 minutes"}

        # Block the tool and return answers via reason — Claude sees this as the tool output
        import json
        return {"decision": "block", "reason": json.dumps({"answers": answers})}

    return _hook


def _build_system_prompt(workdir: str) -> dict:
    """Build an append-style system prompt so the CLI keeps its default (with ToolSearch/deferred tools).

    Returns a dict ``{"type": "preset", "append": "..."}`` which the SDK translates
    to ``--append-system-prompt`` instead of ``--system-prompt``.
    """
    claude_md = Path(workdir) / "CLAUDE.md"
    base = ""
    try:
        base = claude_md.read_text()
    except (FileNotFoundError, PermissionError):
        pass
    append_text = f"{base}\n\nYou are helping in directory {workdir}".strip()
    return {"type": "preset", "append": append_text}


class SessionRunner:
    """Owns one ClaudeSDKClient per session. Manages query/response lifecycle via asyncio task."""

    def __init__(
        self,
        thread_id: int,
        workdir: str,
        bot: Bot,
        chat_id: int,
        permission_manager: PermissionManager,
        question_manager: QuestionManager | None = None,
        session_id: str | None = None,
        model: str | None = None,
    ) -> None:
        self.thread_id = thread_id
        self.workdir = str(Path(workdir).expanduser())
        self.provider: SessionProvider = "claude"
        self.session_id = session_id
        self.model = model
        self._bot = bot
        self._chat_id = chat_id
        self._permission_manager = permission_manager
        self._question_manager = question_manager
        self._allowed_tools: set[str] = set()  # Session-local allowed tools (populated from global + local)
        self.state = SessionState.IDLE
        self._client: ClaudeSDKClient | None = None
        self._message_queue: asyncio.Queue[_QueueItem] = asyncio.Queue()
        self._task: asyncio.Task | None = None
        self._status: StatusUpdater | None = None
        self._typing: TypingIndicator | None = None
        self._extra_mcp: dict | None = None  # Additional MCP servers (e.g. orchestrator tools)
        self._system_prompt_override: str | dict | None = None  # Override default system prompt
        self._last_seen_model: str | None = None  # Track model changes across turns
        self._current_reply_to: int | None = None  # message_id to reply to for current turn
        self._effort: str | None = None  # Track effort level (updated from SDK system messages)
        self.auto_mode: bool = False  # Auto-approve all permissions (no prompts)
        self.goal_text: str | None = None  # Goal mode target (None = disabled)
        self._on_turn_complete: Callable[[], Awaitable[None]] | None = None
        self._provider_exhausted_callback: Callable[[str], Awaitable[None]] | None = None
        self._provider_exhausted_notified = False
        self._turn_started_at: float | None = None
        self._last_tool_name: str | None = None
        self._sdk_msg_count: int = 0
        self._last_tool_count: int = 0
        # (removed _consecutive_perm_timeouts — abort on first timeout now)

        # Initialize allowed tools from global permissions
        self._allowed_tools.update(self._permission_manager.get_global_allowed())

    async def start(self) -> None:
        """Launch the runner asyncio task."""
        self._task = asyncio.create_task(self._run())

    def _schedule_provider_exhausted(self, reason: str) -> None:
        """Notify the orchestrator supervisor once when this provider is exhausted."""
        if self._provider_exhausted_notified or self._provider_exhausted_callback is None:
            return
        self._provider_exhausted_notified = True
        asyncio.create_task(self._provider_exhausted_callback(reason))

    async def _run(self) -> None:
        """Main loop: own the ClaudeSDKClient context, process messages from queue.

        Messages arriving during a turn are injected mid-turn via client.query()
        (the SDK supports bidirectional concurrent query + receive_response).
        """
        if self._system_prompt_override:
            if isinstance(self._system_prompt_override, str):
                system_prompt = {"type": "preset", "append": self._system_prompt_override}
            else:
                system_prompt = self._system_prompt_override
        else:
            system_prompt = _build_system_prompt(self.workdir)
        mcp_server = create_telegram_mcp_server(self._bot, self._chat_id, self.thread_id)
        mcp_servers = {"telegram": mcp_server}
        if self._extra_mcp:
            mcp_servers.update(self._extra_mcp)
        # Build hooks — dummy PreToolUse hook is required for can_use_tool to fire (SDK issue #18735)
        # AskUserQuestion is handled in _can_use_tool, not via hooks
        hooks = {
            "PreToolUse": [
                HookMatcher(matcher=None, hooks=[_dummy_pretool_hook]),
            ],
        }
        options = ClaudeAgentOptions(
            cwd=self.workdir,
            model=self.model,
            system_prompt=system_prompt,
            setting_sources=["user", "project"],
            can_use_tool=self._can_use_tool,
            hooks=hooks,
            resume=self.session_id,
            include_partial_messages=True,
            mcp_servers=mcp_servers,
        )
        try:
            async with ClaudeSDKClient(options=options) as client:
                self._client = client

                # Start mid-turn injector task
                inject_task = asyncio.create_task(self._inject_loop(client))

                while self.state != SessionState.STOPPED:
                    item = await self._message_queue.get()
                    if item.text is None and item.content_blocks is None:  # stop sentinel
                        break
                    self.state = SessionState.RUNNING
                    self._turn_started_at = time.monotonic()
                    self._last_tool_name = None
                    self._sdk_msg_count = 0
                    self._current_reply_to = item.reply_to_message_id
                    query_preview = (item.text or "(image)")[:100]
                    logger.info(
                        "Turn started thread=%d session=%s query=%s",
                        self.thread_id, self.session_id, query_preview,
                        extra={"thread_id": self.thread_id, "session_id": self.session_id},
                    )
                    # Create per-turn UX helpers with session metadata
                    self._status = StatusUpdater(
                        self._bot, self._chat_id, self.thread_id,
                        session_id=self.session_id,
                        model=self._last_seen_model or self.model,
                        effort=self._effort,
                    )
                    self._typing = TypingIndicator(self._bot, self._chat_id, self.thread_id)
                    await self._status.start_turn()
                    await self._typing.start()
                    await self._drain_stale_messages(client)
                    await self._send_query(client, item)
                    await self._drain_response(client)
                    # Mark idle IMMEDIATELY so _inject_loop doesn't steal the next message
                    if self.state == SessionState.INTERRUPTING:
                        self.state = SessionState.STOPPED
                    else:
                        self.state = SessionState.IDLE
                    # Stop typing indicator (status finalized inside _drain_response on ResultMessage)
                    if self._typing:
                        await self._typing.stop()
                        self._typing = None
                    if self._turn_started_at is not None:
                        elapsed = time.monotonic() - self._turn_started_at
                        duration_ms = int(elapsed * 1000)
                        if elapsed >= settings.turn_error_seconds:
                            level = logging.ERROR
                        elif elapsed >= settings.turn_warn_seconds:
                            level = logging.WARNING
                        else:
                            level = logging.INFO
                        logger.log(
                            level,
                            "Turn completed thread=%d duration=%dms tool_count=%d",
                            self.thread_id, duration_ms, self._last_tool_count,
                            extra={
                                "thread_id": self.thread_id,
                                "session_id": self.session_id,
                                "duration_ms": duration_ms,
                                "tool_count": self._last_tool_count,
                            },
                        )
                    self._turn_started_at = None
                    self._current_reply_to = None
                    if self.state == SessionState.STOPPED:
                        break
                    await update_session_state(self.thread_id, "idle")
                    if self.goal_text and self._on_turn_complete:
                        try:
                            await self._on_turn_complete()
                        except Exception as e:
                            logger.warning("Goal turn-complete callback failed for thread %d: %s", self.thread_id, e)

                inject_task.cancel()
                try:
                    await inject_task
                except asyncio.CancelledError:
                    pass
        except Exception as e:
            logger.error("Session error for thread %d: %s", self.thread_id, e)
            self.state = SessionState.STOPPED
            if looks_like_provider_limit_error(str(e)):
                self._schedule_provider_exhausted(str(e))
            if self._typing:
                await self._typing.stop()
                self._typing = None
            if self._status:
                await self._status.stop()
                self._status = None
            # Don't send error for expected shutdown errors (SIGTERM = exit 143)
            if self.state != SessionState.INTERRUPTING and "terminated process" not in str(e).lower():
                try:
                    error_text = f"❌ Error: {type(e).__name__}\n{e}"
                    await send_html_message(
                        self._bot,
                        chat_id=self._chat_id,
                        message_thread_id=self.thread_id,
                        text=html.escape(error_text),
                    )
                except Exception:
                    logger.warning("Failed to send error message to thread %d", self.thread_id)
        finally:
            self._client = None

    async def _inject_loop(self, client: ClaudeSDKClient) -> None:
        """Background task: inject queued messages mid-turn via client.query().

        While the main loop is draining a response, new messages from the user
        are picked up here and injected directly into the running session.
        """
        try:
            while True:
                # Only inject when we're in RUNNING state (mid-turn)
                # When IDLE, the main loop will pick up from the queue normally
                if self.state != SessionState.RUNNING:
                    await asyncio.sleep(0.1)
                    continue

                # Non-blocking check for queued messages
                try:
                    item = self._message_queue.get_nowait()
                except asyncio.QueueEmpty:
                    await asyncio.sleep(0.1)
                    continue

                if item.text is None and item.content_blocks is None:
                    # Stop sentinel — put it back for the main loop
                    await self._message_queue.put(item)
                    break

                # Inject mid-turn
                logger.info("Injecting mid-turn message in thread %d", self.thread_id)
                self._current_reply_to = item.reply_to_message_id
                await self._send_query(client, item)
        except asyncio.CancelledError:
            pass

    async def _can_use_tool(
        self,
        tool_name: str,
        input_data: dict,
        context,
    ) -> PermissionResultAllow | PermissionResultDeny:
        """Permission callback — auto-approves safe tools, prompts user for others via Telegram.

        Special handling for AskUserQuestion: shows questions in Telegram, collects answers,
        and returns them as updated_input (mimicking the CLI's onAllow with answers).

        Checks global allowed tools first, then session-local.
        "Allow always" saves to global persistent set via PermissionManager.
        """
        # Auto mode — approve everything without prompting
        if self.auto_mode and tool_name != "AskUserQuestion":
            return PermissionResultAllow(updated_input=input_data)

        # AskUserQuestion — proxy questions to Telegram inline buttons
        if tool_name == "AskUserQuestion" and self._question_manager is not None:
            return await self._handle_ask_user_question(input_data)

        # Check global + session-local allowed tools
        if tool_name in self._allowed_tools:
            return PermissionResultAllow(updated_input=input_data)

        # Also check global (may have been updated by another session)
        if self._permission_manager.is_globally_allowed(tool_name):
            self._allowed_tools.add(tool_name)
            return PermissionResultAllow(updated_input=input_data)

        # Transition to WAITING_PERMISSION state
        prev_state = self.state
        self.state = SessionState.WAITING_PERMISSION

        request_id, future = self._permission_manager.create_request()
        perm_msg = await send_html_message(
            self._bot,
            chat_id=self._chat_id,
            message_thread_id=self.thread_id,
            text=format_permission_message(tool_name, input_data),
            reply_markup=build_permission_keyboard(request_id),
        )

        logger.info(
            "Permission requested thread=%d tool=%s",
            self.thread_id, tool_name,
            extra={"thread_id": self.thread_id, "tool_name": tool_name},
        )
        perm_start = time.monotonic()

        try:
            action = await self._wait_permission_with_reminder(
                future, request_id, perm_msg.message_id,
            )
        except asyncio.TimeoutError:
            perm_duration_ms = int((time.monotonic() - perm_start) * 1000)
            logger.error(
                "Permission timeout thread=%d tool=%s duration=%dms",
                self.thread_id, tool_name, perm_duration_ms,
                extra={
                    "thread_id": self.thread_id,
                    "tool_name": tool_name,
                    "duration_ms": perm_duration_ms,
                },
            )
            self._permission_manager.expire(request_id)
            # Abort the turn immediately on timeout — prevents stale context
            # when user sends a new message later
            await self._bot.send_message(
                chat_id=self._chat_id,
                message_thread_id=self.thread_id,
                text="⏱ Permission timed out — turn aborted.\n"
                     "Send your next message to start fresh.",
            )
            # Interrupt the SDK turn so _drain_response finishes cleanly
            if self._client:
                try:
                    await self._client.interrupt()
                except Exception:
                    pass
            return PermissionResultDeny(message="Turn aborted — permission timeout")
        finally:
            self.state = prev_state  # always restore state (Pitfall 4)

        perm_duration_ms = int((time.monotonic() - perm_start) * 1000)
        logger.info(
            "Permission resolved thread=%d tool=%s result=%s duration=%dms",
            self.thread_id, tool_name, action, perm_duration_ms,
            extra={
                "thread_id": self.thread_id,
                "tool_name": tool_name,
                "result": action,
                "duration_ms": perm_duration_ms,
            },
        )

        if action == "allow":
            return PermissionResultAllow(updated_input=input_data)
        elif action == "always":
            # Add to global persistent set
            await self._permission_manager.allow_globally(tool_name)
            self._allowed_tools.add(tool_name)
            return PermissionResultAllow(
                updated_input=input_data,
                updated_permissions=[
                    PermissionUpdate(
                        type="addRules",
                        rules=[PermissionRuleValue(tool_name=tool_name)],
                        behavior="allow",
                    )
                ],
            )
        else:  # "deny"
            return PermissionResultDeny(message="Denied by user")

    async def _wait_permission_with_reminder(
        self,
        future: asyncio.Future,
        request_id: str,
        perm_message_id: int,
        timeout: float = 120.0,
        reminder_after: float = 45.0,
    ) -> str:
        """Wait for permission response with a reminder nudge.

        After `reminder_after` seconds, replies to the permission message to draw
        attention. Raises asyncio.TimeoutError if no response within `timeout`.
        """
        reminder_sent = False

        async def _send_reminder() -> None:
            nonlocal reminder_sent
            await asyncio.sleep(reminder_after)
            if not future.done():
                reminder_sent = True
                try:
                    await self._bot.send_message(
                        chat_id=self._chat_id,
                        message_thread_id=self.thread_id,
                        text="⏳ Waiting for permission... session is paused",
                        reply_to_message_id=perm_message_id,
                    )
                except Exception:
                    pass

        reminder_task = asyncio.create_task(_send_reminder())
        try:
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            reminder_task.cancel()
            try:
                await reminder_task
            except asyncio.CancelledError:
                pass

    async def _handle_ask_user_question(self, input_data: dict) -> PermissionResultAllow | PermissionResultDeny:
        """Handle AskUserQuestion by showing questions in Telegram and collecting answers.

        Returns PermissionResultAllow with updated_input containing the user's answers,
        mimicking the CLI's onAllow({...input, answers: {...}}) behavior.
        """
        questions = input_data.get("questions", [])
        if not questions:
            return PermissionResultAllow(updated_input=input_data)

        prev_state = self.state
        self.state = SessionState.WAITING_PERMISSION

        request_id, future = self._question_manager.create_request(questions)

        # Send each question as a Telegram message with inline buttons
        for i, question in enumerate(questions):
            text = format_question_message(question)
            keyboard = build_question_keyboard(request_id, i, question)
            try:
                sent = await send_html_message(
                    self._bot,
                    chat_id=self._chat_id,
                    message_thread_id=self.thread_id,
                    text=text,
                    reply_markup=keyboard,
                )
                self._question_manager.add_message_id(request_id, sent.message_id)
            except Exception as e:
                logger.error("Failed to send question %d to thread %d: %s", i, self.thread_id, e)

        try:
            answers = await asyncio.wait_for(future, timeout=300.0)
        except asyncio.TimeoutError:
            self._question_manager.expire(request_id)
            await self._bot.send_message(
                chat_id=self._chat_id,
                message_thread_id=self.thread_id,
                text="⏱ Questions timed out",
            )
            self.state = prev_state
            return PermissionResultDeny(message="User did not answer within 5 minutes")
        finally:
            self.state = prev_state

        # Return allow with answers merged into input (like CLI's onAllow)
        updated = {**input_data, "answers": answers}
        logger.info("AskUserQuestion answered in thread %d: %s", self.thread_id, answers)
        return PermissionResultAllow(updated_input=updated)

    async def _handle_system_message(self, msg: SystemMessage) -> None:
        """Handle SDK system messages and notify user in Telegram.

        Known subtypes:
          - compact_summary / compact_complete — conversation was compacted
          - Any other subtype — log and optionally notify
        """
        subtype = msg.subtype
        data = msg.data
        logger.info("SystemMessage in thread %d: subtype=%s data=%s", self.thread_id, subtype, data)

        # Map known subtypes to user-friendly notifications
        notification = None
        if "compact" in subtype:
            # Compact completed — extract summary if available
            summary = data.get("summary", data.get("message", ""))
            if summary:
                notification = f"📦 Compact: {html.escape(str(summary))}"
            else:
                notification = "📦 Conversation compacted"
        elif subtype == "model_change":
            model = data.get("model", "unknown")
            effort = data.get("effort")
            if effort:
                self._effort = effort
            notification = f"🔄 Model: <code>{html.escape(str(model))}</code>"
            if effort:
                notification += f" · {html.escape(str(effort))}"

        if notification:
            try:
                await send_html_message(
                    self._bot,
                    chat_id=self._chat_id,
                    message_thread_id=self.thread_id,
                    text=notification,
                )
            except Exception as e:
                logger.warning("Failed to send system notification: %s", e)

    async def _drain_stale_messages(self, client: ClaudeSDKClient) -> None:
        """Drain any buffered messages from background tasks that completed between turns.

        Background sub-agents can finish after the main turn's ResultMessage.
        Their messages remain in the CLI stdout buffer and would be read by the
        next receive_response() call, confusing it with stale data.
        """
        while True:
            try:
                msg = await asyncio.wait_for(client.receive_messages().__anext__(), timeout=0.1)
            except (asyncio.TimeoutError, StopAsyncIteration):
                return

            # Forward useful notifications to Telegram, discard the rest
            if isinstance(msg, TaskNotificationMessage):
                status = getattr(msg, "status", "")
                summary = getattr(msg, "summary", "")
                if status == "failed":
                    with contextlib.suppress(Exception):
                        await self._bot.send_message(
                            chat_id=self._chat_id,
                            message_thread_id=self.thread_id,
                            text=f"⚠️ Background task failed: {summary[:200]}",
                        )
                elif summary and status in {"completed", "succeeded", "done"}:
                    with contextlib.suppress(Exception):
                        await self._bot.send_message(
                            chat_id=self._chat_id,
                            message_thread_id=self.thread_id,
                            text=f"ℹ️ Background task {status}: {summary[:200]}",
                        )
            elif isinstance(msg, AssistantMessage):
                # Track model changes from stale messages
                if hasattr(msg, "model") and msg.model and self._last_seen_model != msg.model:
                    self._last_seen_model = msg.model
                    with contextlib.suppress(Exception):
                        await update_session_model(self.thread_id, msg.model)
            elif isinstance(msg, SystemMessage):
                await self._handle_system_message(msg)
            # ResultMessage, TaskStartedMessage, TaskProgressMessage — silently discard

    async def _drain_response(self, client: ClaudeSDKClient) -> None:
        """Receive all messages from the current turn, forwarding text to Telegram with status tracking.

        First text message in each turn is sent as a reply to the user's original message.
        Includes a watchdog: if no SDK message arrives for 3 minutes, notify user.
        """
        tool_count = 0
        first_text_sent = False
        buffered_text_parts: list[str] = []
        soft_watchdog_notified = False
        hard_watchdog_notified = False
        soft_watchdog_timeout = 180.0  # 3 minutes
        hard_watchdog_timeout = 600.0  # 10 minutes

        async def _send_assistant_text(text: str) -> None:
            nonlocal first_text_sent
            for part in split_message(text):
                reply_to = None
                if not first_text_sent and self._current_reply_to:
                    reply_to = self._current_reply_to
                    first_text_sent = True
                escaped_part = escape_markdown_html(part)
                try:
                    await self._bot.send_message(
                        chat_id=self._chat_id,
                        message_thread_id=self.thread_id,
                        text=escaped_part,
                        parse_mode="Markdown",
                        reply_to_message_id=reply_to,
                    )
                except TelegramRetryAfter as e:
                    await asyncio.sleep(e.retry_after)
                    try:
                        await self._bot.send_message(
                            chat_id=self._chat_id,
                            message_thread_id=self.thread_id,
                            text=escaped_part,
                            parse_mode="Markdown",
                            reply_to_message_id=reply_to,
                        )
                    except Exception:
                        await self._bot.send_message(
                            chat_id=self._chat_id,
                            message_thread_id=self.thread_id,
                            text=part,
                            reply_to_message_id=reply_to,
                        )
                except Exception:
                    try:
                        await self._bot.send_message(
                            chat_id=self._chat_id,
                            message_thread_id=self.thread_id,
                            text=part,
                            reply_to_message_id=reply_to,
                        )
                    except Exception:
                        logger.warning("Failed to send text to thread %d (even plain)", self.thread_id)

        async def _watchdog_notify() -> None:
            """Escalate from a soft status note to a hard warning if silence persists."""
            nonlocal soft_watchdog_notified, hard_watchdog_notified
            while True:
                timeout = soft_watchdog_timeout if not soft_watchdog_notified else hard_watchdog_timeout - soft_watchdog_timeout
                await asyncio.sleep(timeout)
                if self.state != SessionState.RUNNING:
                    continue

                if not soft_watchdog_notified:
                    soft_watchdog_notified = True
                    logger.warning(
                        "SDK silence thread=%d silence_duration=%ds",
                        self.thread_id, int(soft_watchdog_timeout),
                        extra={
                            "thread_id": self.thread_id,
                            "silence_duration_ms": int(soft_watchdog_timeout * 1000),
                            "last_msg_type": "unknown",
                        },
                    )
                    if self._status:
                        try:
                            await self._status.show_watchdog_notice(int(soft_watchdog_timeout))
                        except Exception:
                            pass
                    continue

                if not hard_watchdog_notified:
                    hard_watchdog_notified = True
                    logger.error(
                        "SDK hard silence thread=%d silence_duration=%ds",
                        self.thread_id, int(hard_watchdog_timeout),
                        extra={
                            "thread_id": self.thread_id,
                            "silence_duration_ms": int(hard_watchdog_timeout * 1000),
                            "last_msg_type": "unknown",
                        },
                    )
                    try:
                        await self._bot.send_message(
                            chat_id=self._chat_id,
                            message_thread_id=self.thread_id,
                            text="⚠️ No SDK updates for 10 min. The task may be stuck. "
                                 "Use /stop only if you want to interrupt it.",
                        )
                    except Exception:
                        pass

        watchdog_task = asyncio.create_task(_watchdog_notify())
        try:
            async for msg in client.receive_response():
                # Reset watchdog on each message by cancelling and restarting
                watchdog_task.cancel()
                soft_watchdog_notified = False
                hard_watchdog_notified = False
                if self._status:
                    self._status.clear_watchdog_notice()
                watchdog_task = asyncio.create_task(_watchdog_notify())

                self._sdk_msg_count += 1
                msg_type = type(msg).__name__
                logger.debug(
                    "SDK msg thread=%d type=%s index=%d",
                    self.thread_id, msg_type, self._sdk_msg_count,
                    extra={
                        "thread_id": self.thread_id,
                        "msg_type": msg_type,
                        "msg_index": self._sdk_msg_count,
                    },
                )

                if isinstance(msg, AssistantMessage):
                    # Track model
                    if hasattr(msg, "model") and msg.model:
                        if self._last_seen_model != msg.model:
                            self._last_seen_model = msg.model
                            try:
                                await update_session_model(self.thread_id, msg.model)
                            except Exception:
                                pass

                    # Feed usage/model data to status updater
                    msg_usage = getattr(msg, "usage", None)
                    if self._status:
                        self._status.track_usage(
                            usage=msg_usage,
                            model=getattr(msg, "model", None),
                        )

                    for block in msg.content:
                        if isinstance(block, TextBlock) and block.text:
                            if settings.stream_intermediate_messages:
                                await _send_assistant_text(block.text)
                            else:
                                buffered_text_parts.append(block.text)
                            logger.info(
                                "Assistant response thread=%d text_length=%d",
                                self.thread_id, len(block.text),
                                extra={
                                    "thread_id": self.thread_id,
                                    "text_length": len(block.text),
                                },
                            )
                        elif isinstance(block, ToolUseBlock):
                            tool_count += 1
                            self._last_tool_name = block.name
                            logger.info(
                                "Tool use thread=%d tool=%s tool_index=%d",
                                self.thread_id, block.name, tool_count,
                                extra={
                                    "thread_id": self.thread_id,
                                    "tool_name": block.name,
                                    "tool_index": tool_count,
                                },
                            )
                            if self._status:
                                self._status.track_tool(block.name, block.input if hasattr(block, "input") else None)

                elif isinstance(msg, TaskProgressMessage):
                    if self._status:
                        last_tool = getattr(msg, "last_tool_name", None)
                        usage = getattr(msg, "usage", None)
                        if last_tool:
                            tools_n = usage.get("tool_uses", 0) if usage else 0
                            self._status.track_tool(f"Agent/{last_tool}", {"description": f"sub-agent ({tools_n} tools)"})

                elif isinstance(msg, TaskStartedMessage):
                    logger.info("Sub-agent started in thread %d: %s", self.thread_id, getattr(msg, "description", ""))

                elif isinstance(msg, TaskNotificationMessage):
                    status = getattr(msg, "status", "")
                    summary = getattr(msg, "summary", "")
                    if status == "failed":
                        try:
                            await self._bot.send_message(
                                chat_id=self._chat_id,
                                message_thread_id=self.thread_id,
                                text=f"⚠️ Sub-agent failed: {summary[:200]}",
                            )
                        except Exception:
                            pass
                    logger.info("Sub-agent %s in thread %d: %s", status, self.thread_id, summary[:100])

                elif isinstance(msg, RateLimitEvent):
                    info = msg.rate_limit_info
                    if info.status == "rejected":
                        resets = ""
                        if info.resets_at:
                            import datetime
                            dt = datetime.datetime.fromtimestamp(info.resets_at / 1000)
                            resets = f" Resets at {dt.strftime('%H:%M')}"
                        try:
                            await self._bot.send_message(
                                chat_id=self._chat_id,
                                message_thread_id=self.thread_id,
                                text=f"🚫 Rate limited!{resets}",
                            )
                        except Exception:
                            pass
                        self._schedule_provider_exhausted(
                            f"Claude rate limited{resets}".strip()
                        )
                    elif info.status == "allowed_warning" and info.utilization:
                        try:
                            await self._bot.send_message(
                                chat_id=self._chat_id,
                                message_thread_id=self.thread_id,
                                text=f"⚠️ Rate limit: {info.utilization * 100:.0f}% used",
                            )
                        except Exception:
                            pass

                elif isinstance(msg, SystemMessage):
                    await self._handle_system_message(msg)

                elif isinstance(msg, ResultMessage):
                    if self.session_id is None and msg.session_id:
                        self.session_id = msg.session_id
                        await update_session_id(self.thread_id, msg.session_id)

                    if not settings.stream_intermediate_messages and buffered_text_parts:
                        await _send_assistant_text("".join(buffered_text_parts))
                        buffered_text_parts.clear()

                    result_usage = getattr(msg, "usage", None)
                    if self._status and result_usage:
                        self._status.track_usage(usage=result_usage)

                    if self._status:
                        await self._status.finalize(
                            cost_usd=msg.total_cost_usd,
                            duration_ms=msg.duration_ms,
                            tool_count=tool_count,
                        )
                        self._status = None

                    if msg.is_error:
                        try:
                            await send_html_message(
                                self._bot,
                                chat_id=self._chat_id,
                                message_thread_id=self.thread_id,
                                text=html.escape(f"❌ Error: SDK\n{msg.session_id or 'no session_id'}"),
                            )
                        except Exception:
                            pass

                    logger.info(
                        "Turn complete for thread %d: cost=$%s, duration=%dms, tools=%d",
                        self.thread_id, msg.total_cost_usd, msg.duration_ms, tool_count,
                    )
        finally:
            self._last_tool_count = tool_count
            watchdog_task.cancel()
            try:
                await watchdog_task
            except asyncio.CancelledError:
                pass

    async def _send_query(self, client: ClaudeSDKClient, item: _QueueItem) -> None:
        """Send a query to the SDK — text or multimodal (image + text)."""
        import json

        if item.content_blocks:
            # Multimodal: send raw message dict with content blocks
            message = {
                "type": "user",
                "message": {"role": "user", "content": item.content_blocks},
                "parent_tool_use_id": None,
                "session_id": "default",
            }
            await client._transport.write(json.dumps(message) + "\n")
        else:
            await client.query(item.text)

    async def enqueue(self, text: str, reply_to_message_id: int | None = None) -> None:
        """Queue a user message with optional reply tracking."""
        await self._message_queue.put(_QueueItem(text=text, reply_to_message_id=reply_to_message_id))
        qsize = self._message_queue.qsize()
        log_level = logging.WARNING if qsize > 5 else logging.INFO
        logger.log(
            log_level,
            "Message enqueued thread=%d queue_size=%d",
            self.thread_id, qsize,
            extra={"thread_id": self.thread_id, "queue_size": qsize},
        )

    async def enqueue_image(
        self,
        image_data: bytes,
        media_type: str = "image/jpeg",
        caption: str = "",
        reply_to_message_id: int | None = None,
    ) -> None:
        """Queue an image message. Claude sees the image directly (no file path)."""
        import base64

        b64 = base64.b64encode(image_data).decode()
        blocks = [
            {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": b64}},
        ]
        if caption:
            blocks.append({"type": "text", "text": caption})
        else:
            blocks.append({"type": "text", "text": "User sent a photo. Describe what you see."})
        await self._message_queue.put(_QueueItem(
            text=None,
            reply_to_message_id=reply_to_message_id,
            content_blocks=blocks,
        ))

    async def interrupt(self) -> bool:
        """Interrupt the current turn (like Escape in CLI). Session stays alive for next message.

        Returns True if there was a running turn to interrupt, False otherwise.
        """
        if self.state != SessionState.RUNNING or not self._client:
            return False
        try:
            await self._client.interrupt()
            logger.info("Interrupted running turn in thread %d", self.thread_id)
            return True
        except Exception as e:
            logger.warning("Failed to interrupt thread %d: %s", self.thread_id, e)
            return False

    async def stop(self) -> None:
        """Interrupt the running turn (if any) and stop the runner."""
        prev_state = self.state
        self.state = SessionState.INTERRUPTING
        if self._client and prev_state == SessionState.RUNNING:
            try:
                await self._client.interrupt()
            except Exception as e:
                logger.warning("Failed to interrupt client for thread %d: %s", self.thread_id, e)
        await self._message_queue.put(_QueueItem(text=None))  # sentinel to unblock queue.get()
        try:
            await update_session_state(self.thread_id, "stopped")
        except Exception:
            pass
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=10.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()
            except Exception as e:
                logger.warning("Error waiting for task %d: %s", self.thread_id, e)

    @property
    def is_alive(self) -> bool:
        """True if the runner task is still running."""
        return self._task is not None and not self._task.done()
