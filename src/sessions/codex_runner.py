"""CodexRunner — local session backend backed by `codex app-server`."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import html
import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable

from aiogram import Bot
from aiogram.exceptions import TelegramRetryAfter

from src.bot.output import (
    TypingIndicator,
    escape_markdown_html,
    html_code,
    send_html_message,
    split_message,
)
from src.bot.status import StatusUpdater
from src.config import settings
from src.db.queries import (
    update_backend_session_id,
    update_session_model,
    update_session_state,
)
from src.sessions.backend import SessionProvider, looks_like_provider_limit_error
from src.sessions.codex_app_server import (
    CodexAppServerClient,
    CodexAppServerError,
    THREAD_ARCHIVE_METHOD,
    THREAD_COMPACT_START_METHOD,
    THREAD_RESUME_METHOD,
    THREAD_START_METHOD,
    TURN_INTERRUPT_METHOD,
    TURN_START_METHOD,
    TURN_STEER_METHOD,
)
from src.sessions.permissions import (
    PermissionManager,
    build_permission_keyboard,
    format_permission_message,
)
from src.sessions.questions import (
    QuestionManager,
    build_question_keyboard,
    format_question_message,
)
from src.sessions.state import SessionState
from src.sessions.telegram_output_mcp import LocalTelegramOutputMcpServer

logger = logging.getLogger(__name__)


@dataclass
class _QueueItem:
    """A queued user message with optional reply tracking."""

    text: str | None
    input_items: list[dict[str, Any]] | None = None
    reply_to_message_id: int | None = None


class CodexRunner:
    """Owns one persistent Codex app-server thread per Telegram topic."""

    provider: SessionProvider = "codex"

    def __init__(
        self,
        thread_id: int,
        workdir: str,
        bot: Bot,
        chat_id: int,
        permission_manager: PermissionManager | None = None,
        question_manager: QuestionManager | None = None,
        backend_session_id: str | None = None,
        model: str | None = None,
        base_instructions: str | None = None,
        developer_instructions: str | None = None,
        mcp_server_urls: dict[str, str] | None = None,
    ) -> None:
        self.thread_id = thread_id
        self.workdir = str(Path(workdir).expanduser())
        self.session_id: str | None = None
        self.backend_session_id = backend_session_id
        self.model = model
        self.state = SessionState.IDLE
        self.auto_mode = False

        self._bot = bot
        self._chat_id = chat_id
        self._permission_manager = permission_manager or PermissionManager()
        self._question_manager = question_manager
        self._base_instructions = base_instructions
        self._developer_instructions = developer_instructions
        self._mcp_server_urls = mcp_server_urls or {}
        self._telegram_mcp_server: LocalTelegramOutputMcpServer | None = None
        self._telegram_mcp_url: str | None = None
        self._message_queue: asyncio.Queue[_QueueItem] = asyncio.Queue()
        self._task: asyncio.Task | None = None
        self._inject_task: asyncio.Task | None = None
        self._status: StatusUpdater | None = None
        self._typing: TypingIndicator | None = None
        self._current_reply_to: int | None = None
        self._current_turn_id: str | None = None
        self._client: CodexAppServerClient | None = None
        self._interrupted = False
        self._active_user_wait: asyncio.Future | None = None
        self._active_user_wait_cancel_value: str | dict[str, Any] | None = None
        self._agent_message_buffers: dict[str, str] = {}
        self._agent_message_sent_lengths: dict[str, int] = {}
        self._task_items: dict[str, str] = {}
        self._provider_exhausted_callback: Callable[[str], Awaitable[None]] | None = None
        self._provider_exhausted_notified = False
        self._turn_first_text_sent = False
        self.goal_text: str | None = None
        self._on_turn_complete: Callable[[], Awaitable[None]] | None = None

    def _build_config_overrides(self) -> list[str]:
        """Return codex CLI config overrides for this runner."""
        mcp_urls = dict(self._mcp_server_urls)
        if self._telegram_mcp_url and "telegram" not in mcp_urls:
            mcp_urls["telegram"] = self._telegram_mcp_url
        overrides: list[str] = []
        for name, url in sorted(mcp_urls.items()):
            overrides.append(f"mcp_servers.{name}.url={json.dumps(url)}")
        return overrides

    async def start(self) -> None:
        """Launch the runner task."""
        self._task = asyncio.create_task(self._run())

    def _schedule_provider_exhausted(self, reason: str) -> None:
        """Notify the orchestrator supervisor once when this provider is exhausted."""
        if self._provider_exhausted_notified or self._provider_exhausted_callback is None:
            return
        self._provider_exhausted_notified = True
        asyncio.create_task(self._provider_exhausted_callback(reason))

    async def _run(self) -> None:
        """Start app-server, ensure thread exists, then process queued turns."""
        try:
            if "telegram" not in self._mcp_server_urls:
                self._telegram_mcp_server = LocalTelegramOutputMcpServer(
                    self._bot,
                    self._chat_id,
                    self.thread_id,
                )
                self._telegram_mcp_url = await self._telegram_mcp_server.start()
            self._client = CodexAppServerClient(
                cwd=self.workdir,
                config_overrides=self._build_config_overrides(),
            )
            await self._client.start()
            await self._ensure_thread()
            self._inject_task = asyncio.create_task(self._inject_loop())

            while self.state != SessionState.STOPPED:
                item = await self._message_queue.get()
                if item.text is None and item.input_items is None:
                    break

                if item.input_items is None and item.text is not None and await self._handle_compat_command(item.text):
                    continue

                self.state = SessionState.RUNNING
                await update_session_state(self.thread_id, "running")
                self._interrupted = False
                self._current_reply_to = item.reply_to_message_id
                self._status = StatusUpdater(
                    self._bot,
                    self._chat_id,
                    self.thread_id,
                    session_id=self.backend_session_id,
                    model=self.model,
                )
                self._typing = TypingIndicator(self._bot, self._chat_id, self.thread_id)
                await self._status.start_turn()
                await self._typing.start()

                try:
                    await self._run_turn(prompt=item.text, input_items=item.input_items)
                finally:
                    if self._typing:
                        await self._typing.stop()
                        self._typing = None
                    self._current_reply_to = None
                    self._current_turn_id = None
                    self._agent_message_buffers.clear()
                    self._agent_message_sent_lengths.clear()
                    self._turn_first_text_sent = False

                if self.state == SessionState.INTERRUPTING:
                    self.state = SessionState.STOPPED
                    break

                self.state = SessionState.IDLE
                await update_session_state(self.thread_id, "idle")
                if self.goal_text and self._on_turn_complete:
                    try:
                        await self._on_turn_complete()
                    except Exception as e_cb:
                        logger.warning("Goal turn-complete callback failed for thread %d: %s", self.thread_id, e_cb)
        except Exception as e:
            logger.error("Codex session error for thread %d: %s", self.thread_id, e)
            self.state = SessionState.STOPPED
            if looks_like_provider_limit_error(str(e)):
                self._schedule_provider_exhausted(str(e))
            if self._typing:
                await self._typing.stop()
                self._typing = None
            if self._status:
                await self._status.stop()
                self._status = None
            try:
                await self._bot.send_message(
                    chat_id=self._chat_id,
                    message_thread_id=self.thread_id,
                    text=f"❌ Codex error: {type(e).__name__}\n{e}",
                )
            except Exception:
                logger.warning("Failed to send Codex error message to thread %d", self.thread_id)
        finally:
            if self._inject_task:
                self._inject_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._inject_task
                self._inject_task = None
            if self._client:
                await self._client.close()
                self._client = None
            if self._telegram_mcp_server:
                await self._telegram_mcp_server.stop()
                self._telegram_mcp_server = None
            self._telegram_mcp_url = None

    async def _ensure_thread(self, *, force_new: bool = False) -> None:
        """Start or resume the persistent Codex thread for this Telegram topic."""
        if self._client is None:
            raise CodexAppServerError("Codex app-server client is not ready")

        params = {
            "cwd": self.workdir,
            "approvalPolicy": "on-request",
            "approvalsReviewer": "user",
            "personality": "pragmatic",
        }
        if self._base_instructions:
            params["baseInstructions"] = self._base_instructions
        if self._developer_instructions:
            params["developerInstructions"] = self._developer_instructions
        if self.model:
            params["model"] = self.model

        if self.backend_session_id and not force_new:
            result = await self._client.request(
                THREAD_RESUME_METHOD,
                {
                    "threadId": self.backend_session_id,
                    **params,
                },
            )
        else:
            result = await self._client.request(THREAD_START_METHOD, params)

        thread = result.get("thread", {}) if isinstance(result, dict) else {}
        thread_id = thread.get("id")
        if thread_id and thread_id != self.backend_session_id:
            self.backend_session_id = thread_id
            await update_backend_session_id(self.thread_id, thread_id)

        await self._drain_non_turn_messages()

    async def _run_turn(
        self,
        prompt: str | None = None,
        input_items: list[dict[str, Any]] | None = None,
    ) -> None:
        """Start one Codex turn and process notifications until completion."""
        if self._client is None:
            raise CodexAppServerError("Codex app-server client is not ready")
        if self.backend_session_id is None:
            await self._ensure_thread()
        if self.backend_session_id is None:
            raise CodexAppServerError("Codex thread id is missing")

        if input_items is None:
            if prompt is None:
                raise CodexAppServerError("Codex turn input is missing")
            input_items = [{"type": "text", "text": prompt}]

        result = await self._client.request(
            TURN_START_METHOD,
            {
                "threadId": self.backend_session_id,
                "input": input_items,
                "cwd": self.workdir,
                "approvalPolicy": "on-request",
                "approvalsReviewer": "user",
                **({"model": self.model} if self.model else {}),
            },
        )
        turn = result.get("turn", {}) if isinstance(result, dict) else {}
        self._current_turn_id = turn.get("id")
        turn_started_at = time.monotonic()
        tool_count = 0
        stall_notified = False
        watchdog_timeout = 180.0
        self._turn_first_text_sent = False
        self._agent_message_sent_lengths.clear()

        async def _watchdog_notify() -> None:
            nonlocal stall_notified
            while True:
                await asyncio.sleep(watchdog_timeout)
                if not stall_notified and self.state == SessionState.RUNNING:
                    stall_notified = True
                    with contextlib.suppress(Exception):
                        await self._bot.send_message(
                            chat_id=self._chat_id,
                            message_thread_id=self.thread_id,
                            text=(
                                "⚠️ Codex session appears stalled (no response for 3 min). "
                                "Use /stop to interrupt or send a message to nudge."
                            ),
                        )

        watchdog_task = asyncio.create_task(_watchdog_notify())

        try:
            while True:
                message = await self._client.next_message()
                watchdog_task.cancel()
                stall_notified = False
                watchdog_task = asyncio.create_task(_watchdog_notify())
                method = message.get("method")
                params = message.get("params", {})

                if "id" in message and method:
                    await self._handle_server_request(message)
                    continue

                if method == "thread/started":
                    thread = params.get("thread", {})
                    thread_id = thread.get("id")
                    if thread_id and thread_id != self.backend_session_id:
                        self.backend_session_id = thread_id
                        await update_backend_session_id(self.thread_id, thread_id)
                    continue

                if method == "thread/tokenUsage/updated":
                    usage = params.get("tokenUsage", {})
                    if self._status:
                        self._status.track_usage(
                            usage={
                                "input_tokens": usage.get("inputTokens", 0),
                                "output_tokens": usage.get("outputTokens", 0),
                                "cache_read_input_tokens": usage.get("cacheReadInputTokens", 0),
                                "cache_creation_input_tokens": usage.get("cacheCreationInputTokens", 0),
                            },
                            model=self.model,
                        )
                    continue

                if method == "model/rerouted":
                    model = params.get("model")
                    if model and model != self.model:
                        self.model = model
                        await update_session_model(self.thread_id, model)
                        await send_html_message(
                            self._bot,
                            chat_id=self._chat_id,
                            message_thread_id=self.thread_id,
                            text=f"🔄 Model: {html_code(model)}",
                        )
                    continue

                if method == "item/started":
                    tool_count += self._track_item_started(params.get("item", {}))
                    continue

                if method == "item/agentMessage/delta":
                    item_id = params.get("itemId")
                    delta = params.get("delta", "")
                    if item_id and delta:
                        self._agent_message_buffers[item_id] = (
                            self._agent_message_buffers.get(item_id, "") + delta
                        )
                        if settings.stream_intermediate_messages:
                            await self._flush_streaming_agent_text(item_id)
                    continue

                if method == "item/completed":
                    await self._handle_item_completed(params.get("item", {}))
                    continue

                if method == "turn/plan/updated":
                    if self._status:
                        self._status.track_tool(
                            "Agent",
                            {"description": params.get("explanation", "plan update")},
                        )
                    continue

                if method == "error":
                    logger.warning("Codex turn error event in thread %d: %s", self.thread_id, params)
                    continue

                if method == "turn/completed":
                    completed_turn = params.get("turn", {})
                    if self._status:
                        await self._status.finalize(
                            cost_usd=None,
                            duration_ms=int((time.monotonic() - turn_started_at) * 1000),
                            tool_count=tool_count,
                        )
                        self._status = None

                    status_value = completed_turn.get("status")
                    error = completed_turn.get("error")
                    if status_value == "failed" and error:
                        message = error.get("message") if isinstance(error, dict) else str(error)
                        if looks_like_provider_limit_error(message):
                            self._schedule_provider_exhausted(message)
                            with contextlib.suppress(Exception):
                                await self._bot.send_message(
                                    chat_id=self._chat_id,
                                    message_thread_id=self.thread_id,
                                    text=f"🚫 Codex rate limited or quota exhausted\n{message or 'Unknown error'}",
                                )
                        else:
                            with contextlib.suppress(Exception):
                                await self._bot.send_message(
                                    chat_id=self._chat_id,
                                    message_thread_id=self.thread_id,
                                    text=f"❌ Codex turn failed\n{message or 'Unknown error'}",
                                )
                    return
        finally:
            watchdog_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await watchdog_task

    async def _inject_loop(self) -> None:
        """Inject queued messages into the currently active turn via turn/steer."""
        try:
            while True:
                if self.state != SessionState.RUNNING or not self._current_turn_id or not self._client:
                    await asyncio.sleep(0.1)
                    continue

                try:
                    item = self._message_queue.get_nowait()
                except asyncio.QueueEmpty:
                    await asyncio.sleep(0.1)
                    continue

                if item.text is None and item.input_items is None:
                    await self._message_queue.put(item)
                    break

                if item.input_items is None and item.text and item.text.startswith("/"):
                    await self._message_queue.put(item)
                    await asyncio.sleep(0.1)
                    continue

                logger.info("Steering active Codex turn in thread %d", self.thread_id)
                self._current_reply_to = item.reply_to_message_id
                try:
                    await self._client.request(
                        TURN_STEER_METHOD,
                        {
                            "threadId": self.backend_session_id,
                            "expectedTurnId": self._current_turn_id,
                            "input": item.input_items or [{"type": "text", "text": item.text}],
                        },
                    )
                except Exception as e:
                    logger.warning("Failed to steer Codex turn in thread %d: %s", self.thread_id, e)
        except asyncio.CancelledError:
            pass

    async def _handle_server_request(self, message: dict[str, Any]) -> None:
        """Handle server-initiated approval or user-input requests."""
        request_id = message["id"]
        method = message["method"]
        params = message.get("params", {})

        if method in {
            "item/commandExecution/requestApproval",
            "item/fileChange/requestApproval",
        }:
            result = await self._ask_telegram_permission(method, params)
            await self._client.respond(request_id, result)
            return

        if method == "item/permissions/requestApproval":
            result = await self._ask_telegram_permissions_profile(params)
            await self._client.respond(request_id, result)
            return

        if method == "item/tool/requestUserInput":
            result = await self._ask_telegram_user_input(params)
            await self._client.respond(request_id, result)
            return

        logger.warning("Unhandled Codex server request %s in thread %d", method, self.thread_id)
        await self._client.respond(request_id, {})

    async def _ask_telegram_permission(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        """Bridge a command/file approval request into Telegram inline buttons."""
        if self.auto_mode:
            return {"decision": "accept"}

        tool_name = "Bash" if "commandExecution" in method else "Edit"
        if self._permission_manager.is_globally_allowed(tool_name):
            return {"decision": "acceptForSession"}
        input_data = {
            "command": params.get("command", ""),
            "file_path": params.get("grantRoot", ""),
            "reason": params.get("reason", ""),
        }

        request_id, future = self._permission_manager.create_request()
        self._active_user_wait = future
        self._active_user_wait_cancel_value = "deny"

        sent = await send_html_message(
            self._bot,
            chat_id=self._chat_id,
            message_thread_id=self.thread_id,
            text=format_permission_message(tool_name, input_data),
            reply_markup=build_permission_keyboard(request_id),
        )
        try:
            action = await asyncio.wait_for(future, timeout=300.0)
        except asyncio.TimeoutError:
            self._permission_manager.expire(request_id)
            action = "deny"
            await self._bot.send_message(
                chat_id=self._chat_id,
                message_thread_id=self.thread_id,
                text="⏱ Codex permission timed out — declining request.",
                reply_to_message_id=sent.message_id,
            )
        finally:
            self._active_user_wait = None
            self._active_user_wait_cancel_value = None

        if action == "always":
            await self._permission_manager.allow_globally(tool_name)
            return {"decision": "acceptForSession"}
        if action == "allow":
            return {"decision": "accept"}
        return {"decision": "decline"}

    async def _ask_telegram_permissions_profile(self, params: dict[str, Any]) -> dict[str, Any]:
        """Bridge a `request_permissions` request into Telegram approvals."""
        permissions = params.get("permissions", {})
        if self.auto_mode:
            return {"scope": "session", "permissions": permissions}
        if self._permission_manager.is_globally_allowed("request_permissions"):
            return {"scope": "session", "permissions": permissions}

        request_id, future = self._permission_manager.create_request()
        self._active_user_wait = future
        self._active_user_wait_cancel_value = "deny"
        sent = await send_html_message(
            self._bot,
            chat_id=self._chat_id,
            message_thread_id=self.thread_id,
            text=format_permission_message("request_permissions", permissions),
            reply_markup=build_permission_keyboard(request_id),
        )
        try:
            action = await asyncio.wait_for(future, timeout=300.0)
        except asyncio.TimeoutError:
            self._permission_manager.expire(request_id)
            action = "deny"
            await self._bot.send_message(
                chat_id=self._chat_id,
                message_thread_id=self.thread_id,
                text="⏱ Permission request timed out — declining request.",
                reply_to_message_id=sent.message_id,
            )
        finally:
            self._active_user_wait = None
            self._active_user_wait_cancel_value = None

        if action in {"allow", "always"}:
            if action == "always":
                await self._permission_manager.allow_globally("request_permissions")
            return {
                "scope": "session" if action == "always" else "turn",
                "permissions": permissions,
            }
        return {"scope": "turn", "permissions": {}}

    async def _ask_telegram_user_input(self, params: dict[str, Any]) -> dict[str, Any]:
        """Bridge Codex `request_user_input` into the existing Telegram question flow."""
        questions = params.get("questions", [])
        if not questions or self._question_manager is None:
            return {"answers": {}}

        request_id, future = self._question_manager.create_request(questions)
        self._active_user_wait = future
        self._active_user_wait_cancel_value = {}

        for i, question in enumerate(questions):
            sent = await send_html_message(
                self._bot,
                chat_id=self._chat_id,
                message_thread_id=self.thread_id,
                text=format_question_message(question),
                reply_markup=build_question_keyboard(request_id, i, question),
            )
            self._question_manager.add_message_id(request_id, sent.message_id)

        try:
            answers = await asyncio.wait_for(future, timeout=300.0)
        except asyncio.TimeoutError:
            self._question_manager.expire(request_id)
            answers = {}
            await self._bot.send_message(
                chat_id=self._chat_id,
                message_thread_id=self.thread_id,
                text="⏱ Questions timed out",
            )
        except asyncio.CancelledError:
            answers = {}
        finally:
            self._active_user_wait = None
            self._active_user_wait_cancel_value = None

        payload: dict[str, Any] = {"answers": {}}
        for question in questions:
            qid = question.get("id")
            qtext = question.get("question", "")
            if not qid:
                continue
            raw_answer = answers.get(qtext)
            if raw_answer in (None, "(no selection)"):
                selected = []
            else:
                selected = [part.strip() for part in str(raw_answer).split(",") if part.strip()]
            payload["answers"][qid] = {"answers": selected}
        return payload

    def _track_item_started(self, item: dict[str, Any]) -> int:
        """Map Codex items to the shared status updater."""
        if not self._status or not isinstance(item, dict):
            return 0
        item_type = item.get("type")
        if item_type in {"task", "agentTask", "subtask", "agent"}:
            item_id = item.get("id")
            description = self._describe_task_item(item)
            if item_id and description:
                self._task_items[item_id] = description
                asyncio.create_task(
                    self._bot.send_message(
                        chat_id=self._chat_id,
                        message_thread_id=self.thread_id,
                        text=f"🧠 Sub-agent started: {description[:200]}",
                    )
                )
            self._status.track_tool("Agent", {"description": description or item_type})
            return 1
        if item_type == "commandExecution":
            self._status.track_tool("Bash", {"command": item.get("command", "")})
            return 1
        if item_type == "fileChange":
            first_change = ""
            changes = item.get("changes") or []
            if changes and isinstance(changes[0], dict):
                first_change = changes[0].get("path", "")
            self._status.track_tool("Edit", {"file_path": first_change})
            return 1
        if item_type == "mcpToolCall":
            self._status.track_tool(item.get("tool", "MCP"), {"description": item.get("tool", "")})
            return 1
        return 0

    async def _handle_item_completed(self, item: dict[str, Any]) -> None:
        """Handle completed items, including final assistant messages."""
        if not isinstance(item, dict):
            return
        item_type = item.get("type")
        if item_type in {"task", "agentTask", "subtask", "agent"}:
            item_id = item.get("id")
            description = self._task_items.pop(item_id, self._describe_task_item(item))
            status = str(item.get("status", "")).lower()
            summary = self._describe_task_summary(item)
            if status in {"failed", "error"}:
                with contextlib.suppress(Exception):
                    await self._bot.send_message(
                        chat_id=self._chat_id,
                        message_thread_id=self.thread_id,
                        text=f"⚠️ Sub-agent failed: {(summary or description or item_type)[:200]}",
                    )
            return
        if item_type == "agentMessage":
            item_id = item.get("id")
            text = self._agent_message_buffers.pop(item_id, "")
            sent_length = self._agent_message_sent_lengths.pop(item_id, 0)
            completed_text = self._extract_agent_message_text(item)
            if completed_text and len(completed_text) >= len(text):
                text = completed_text
            if text:
                if settings.stream_intermediate_messages:
                    remainder = text[sent_length:]
                    if remainder:
                        await self._send_assistant_text(remainder)
                else:
                    await self._send_assistant_text(text)

    async def _flush_streaming_agent_text(self, item_id: str, force: bool = False) -> None:
        """Flush accumulated agent text when there is enough visible progress."""
        text = self._agent_message_buffers.get(item_id, "")
        sent_length = self._agent_message_sent_lengths.get(item_id, 0)
        if sent_length >= len(text):
            return
        unsent = text[sent_length:]
        if not unsent:
            return
        if not force and len(unsent) < 120 and not any(ch in unsent for ch in ("\n", ".", "!", "?", ":")):
            return
        await self._send_assistant_text(unsent)
        self._agent_message_sent_lengths[item_id] = len(text)

    @staticmethod
    def _describe_task_item(item: dict[str, Any]) -> str:
        """Return a compact human-readable task description when available."""
        for key in ("title", "description", "summary", "name", "prompt"):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    @staticmethod
    def _describe_task_summary(item: dict[str, Any]) -> str:
        """Return a compact summary/result string from a completed task item."""
        for key in ("summary", "result", "message"):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    def _extract_agent_message_text(self, item: dict[str, Any]) -> str:
        """Best-effort extract text from a completed agentMessage item."""
        content = item.get("content")
        if isinstance(content, list):
            parts = []
            for block in content:
                if isinstance(block, dict) and block.get("type") == "outputText":
                    parts.append(block.get("text", ""))
            return "".join(parts)
        return ""

    async def _drain_non_turn_messages(self) -> None:
        """Best-effort drain a few startup messages that are not tied to an active turn."""
        if self._client is None:
            return
        while True:
            try:
                message = await asyncio.wait_for(self._client.next_message(), timeout=0.05)
            except asyncio.TimeoutError:
                return
            method = message.get("method")
            params = message.get("params", {})
            if method == "thread/started":
                thread = params.get("thread", {})
                thread_id = thread.get("id")
                if thread_id and thread_id != self.backend_session_id:
                    self.backend_session_id = thread_id
                    await update_backend_session_id(self.thread_id, thread_id)

    async def _handle_compat_command(self, text: str) -> bool:
        """Implement the highest-value Claude-like slash commands for Codex."""
        if not text.startswith("/"):
            return False

        if text in {"/clear", "/reset"}:
            old_thread_id = self.backend_session_id
            self.backend_session_id = None
            if old_thread_id and self._client:
                with contextlib.suppress(Exception):
                    await self._client.request(THREAD_ARCHIVE_METHOD, {"threadId": old_thread_id})
            await self._ensure_thread(force_new=True)
            await self._bot.send_message(
                chat_id=self._chat_id,
                message_thread_id=self.thread_id,
                text="🧼 Codex context cleared. Started a fresh thread.",
            )
            return True

        if text == "/compact":
            if self._client and self.backend_session_id:
                await self._client.request(
                    THREAD_COMPACT_START_METHOD,
                    {"threadId": self.backend_session_id},
                )
                await self._bot.send_message(
                    chat_id=self._chat_id,
                    message_thread_id=self.thread_id,
                    text="📦 Codex compaction requested.",
                )
            return True

        if text.startswith("/model "):
            model = text.split(maxsplit=1)[1].strip()
            self.model = model or None
            if self.model:
                await update_session_model(self.thread_id, self.model)
            await send_html_message(
                self._bot,
                chat_id=self._chat_id,
                message_thread_id=self.thread_id,
                text=f"🤖 Codex model set to {html_code(self.model or 'default')}",
            )
            return True

        if text == "/help":
            await send_html_message(
                self._bot,
                chat_id=self._chat_id,
                message_thread_id=self.thread_id,
                text=(
                    "Codex session commands:\n"
                    "<code>/status</code> — show current session state\n"
                    "<code>/config</code> — show Codex config for this thread\n"
                    "<code>/model &lt;name&gt;</code> — set default model\n"
                    "<code>/compact</code> — compact Codex thread\n"
                    "<code>/clear</code> / <code>/reset</code> — fresh Codex thread"
                ),
            )
            return True

        if text == "/status":
            await send_html_message(
                self._bot,
                chat_id=self._chat_id,
                message_thread_id=self.thread_id,
                text=(
                    f"Provider: {html_code(self.provider)}\n"
                    f"State: {html_code(self.state.name.lower())}\n"
                    f"Model: {html_code(self.model or 'default')}\n"
                    f"Workdir: {html_code(self.workdir)}\n"
                    f"Backend thread: {html_code(self.backend_session_id or 'pending')}\n"
                    f"Auto-mode: {html_code('on' if self.auto_mode else 'off')}"
                ),
            )
            return True

        if text == "/config":
            active_mcp = ", ".join(sorted({"telegram", *self._mcp_server_urls.keys()}))
            await send_html_message(
                self._bot,
                chat_id=self._chat_id,
                message_thread_id=self.thread_id,
                text=(
                    "Codex config:\n"
                    f"Model: {html_code(self.model or 'default')}\n"
                    f"Workdir: {html_code(self.workdir)}\n"
                    f"MCP: {html_code(active_mcp)}\n"
                    f"Intermediate messages: {html_code('stream' if settings.stream_intermediate_messages else 'final-only')}\n"
                    f"Base instructions: {html_code('yes' if self._base_instructions else 'no')}\n"
                    f"Developer instructions: {html_code('yes' if self._developer_instructions else 'no')}"
                ),
            )
            return True

        if text == "/agents":
            await self._bot.send_message(
                chat_id=self._chat_id,
                message_thread_id=self.thread_id,
                text="Codex sub-agent lifecycle notifications are enabled for this session.",
            )
            return True

        return False

    async def _send_assistant_text(self, text: str) -> None:
        """Forward assistant text to Telegram, replying to the triggering message once."""
        for part in split_message(text):
            reply_to = self._current_reply_to if not self._turn_first_text_sent else None
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
                    with contextlib.suppress(Exception):
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
                    logger.warning("Failed to send codex text to thread %d (even plain)", self.thread_id)
            self._turn_first_text_sent = True

    async def enqueue(self, text: str, reply_to_message_id: int | None = None) -> None:
        """Queue a prompt for the next or active Codex turn."""
        await self._message_queue.put(
            _QueueItem(text=text, input_items=None, reply_to_message_id=reply_to_message_id)
        )

    async def enqueue_image(
        self,
        image_data: bytes,
        media_type: str = "image/jpeg",
        caption: str = "",
        reply_to_message_id: int | None = None,
    ) -> None:
        """Queue an image message so Codex sees the image directly."""
        data_url = f"data:{media_type};base64,{base64.b64encode(image_data).decode()}"
        input_items: list[dict[str, Any]] = [{"type": "image", "url": data_url}]
        input_items.append(
            {
                "type": "text",
                "text": caption or "User sent a photo. Describe what you see.",
            }
        )
        await self._message_queue.put(
            _QueueItem(
                text=None,
                input_items=input_items,
                reply_to_message_id=reply_to_message_id,
            )
        )

    async def interrupt(self) -> bool:
        """Interrupt the active turn or pending approval/question wait."""
        if self.state != SessionState.RUNNING or not self._current_turn_id or not self._client:
            return False
        self._interrupted = True
        if self._active_user_wait is not None and not self._active_user_wait.done():
            self._active_user_wait.set_result(self._active_user_wait_cancel_value)
        with contextlib.suppress(Exception):
            await self._client.request(
                TURN_INTERRUPT_METHOD,
                {"threadId": self.backend_session_id, "turnId": self._current_turn_id},
            )
        return True

    async def stop(self) -> None:
        """Stop the runner and close the app-server session."""
        self.state = SessionState.INTERRUPTING
        if self._active_user_wait is not None and not self._active_user_wait.done():
            self._active_user_wait.set_result(self._active_user_wait_cancel_value)
        if self._current_turn_id and self._client:
            with contextlib.suppress(Exception):
                await self._client.request(
                    TURN_INTERRUPT_METHOD,
                    {"threadId": self.backend_session_id, "turnId": self._current_turn_id},
                )
        await self._message_queue.put(_QueueItem(text=None, input_items=None))
        try:
            await update_session_state(self.thread_id, "stopped")
        except Exception:
            pass
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=10.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()

    @property
    def is_alive(self) -> bool:
        """True if the runner task is still active."""
        return self._task is not None and not self._task.done()
