"""Bot-side IPC server — accepts worker TCP connections and dispatches protocol messages."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from aiogram import Bot
from aiogram.types import BufferedInputFile, FSInputFile, ReactionTypeEmoji

from src.config import settings
from src.ipc.protocol import (
    AuthFailMsg,
    AuthMsg,
    AuthOkMsg,
    AssistantTextMsg,
    McpEditMessageMsg,
    McpReactMsg,
    McpSendFileMsg,
    McpSendMessageMsg,
    PermissionRequestMsg,
    PermissionResponseMsg,
    PingMsg,
    PongMsg,
    QuestionRequestMsg,
    QuestionResponseMsg,
    RateLimitMsg,
    SessionEndedMsg,
    SessionStartedMsg,
    StatusUpdateMsg,
    SystemNotificationMsg,
    TurnCompletedMsg,
    UsageUpdateMsg,
    recv_w2b,
    send_msg,
)
from src.bot.output import escape_markdown_html, send_html_message, split_message
from src.sessions.permissions import build_permission_keyboard, format_permission_message
from src.sessions.questions import build_question_keyboard, format_question_message

logger = logging.getLogger(__name__)


class WorkerRegistry:
    """Tracks live worker TCP connections keyed by worker_id."""

    def __init__(self) -> None:
        self._workers: dict[str, asyncio.StreamWriter] = {}

    def register(self, worker_id: str, writer: asyncio.StreamWriter) -> None:
        """Record a newly authenticated worker connection."""
        self._workers[worker_id] = writer

    def unregister(self, worker_id: str) -> None:
        """Remove a worker connection (called on disconnect)."""
        self._workers.pop(worker_id, None)

    async def send_to(self, worker_id: str, msg) -> bool:
        """Send a message to a specific worker. Returns False if not connected or closing."""
        writer = self._workers.get(worker_id)
        if writer is None or writer.is_closing():
            return False
        await send_msg(writer, msg)
        return True

    def is_connected(self, worker_id: str) -> bool:
        """Return True if the worker has a live (non-closing) connection."""
        w = self._workers.get(worker_id)
        return w is not None and not w.is_closing()

    def list_workers(self) -> list[str]:
        """Return a list of all connected worker IDs."""
        return list(self._workers.keys())


async def start_ipc_server(
    host: str,
    port: int,
    auth_token: str,
    bot: Bot,
    session_manager,
    permission_manager,
    worker_registry: WorkerRegistry,
    question_manager=None,
) -> asyncio.Server:
    """Start the TCP IPC server. Returns the asyncio.Server object."""

    async def handle_connection(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        # Spawn a task so the accept loop is never blocked.
        asyncio.create_task(
            _handle_worker(
                reader,
                writer,
                auth_token,
                bot,
                session_manager,
                permission_manager,
                worker_registry,
                question_manager,
            )
        )

    server = await asyncio.start_server(
        handle_connection, host, port,
        reuse_address=True,
    )
    logger.info("IPC server listening on %s:%d", host, port)
    return server


async def _resume_worker_sessions(
    worker_id: str,
    bot: Bot,
    session_manager,
    worker_registry: WorkerRegistry,
) -> None:
    """Resume remote sessions for a reconnected worker."""
    from src.db.queries import get_worker_sessions

    rows = await get_worker_sessions(worker_id)
    for row in rows:
        thread_id = row["thread_id"]
        # Skip if already registered in session manager
        if session_manager.get(thread_id):
            continue
        try:
            remote = await session_manager.create_remote(
                thread_id=thread_id,
                workdir=row["workdir"],
                worker_id=worker_id,
                worker_registry=worker_registry,
                session_id=row.get("session_id"),
                backend_session_id=row.get("backend_session_id"),
                model=row.get("model"),
                provider=row.get("provider", "claude"),
            )
            if row.get("auto_mode"):
                remote.auto_mode = True
            await bot.send_message(
                chat_id=settings.chat_id,
                message_thread_id=thread_id,
                text=f"Session resumed on {worker_id}.",
            )
            logger.info("Resumed remote session topic %d on worker %s", thread_id, worker_id)
        except Exception as e:
            logger.error("Failed to resume remote session %d on %s: %s", thread_id, worker_id, e)


async def _handle_worker(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    auth_token: str,
    bot: Bot,
    session_manager,
    permission_manager,
    worker_registry: WorkerRegistry,
    question_manager,
) -> None:
    """Handle a single worker connection: authenticate, then dispatch messages."""
    worker_id: str | None = None
    try:
        # --- Auth handshake ---
        first_msg = await recv_w2b(reader)
        if not isinstance(first_msg, AuthMsg) or first_msg.token != auth_token:
            await send_msg(writer, AuthFailMsg(reason="invalid token"))
            writer.close()
            return

        worker_id = first_msg.worker_id
        await send_msg(writer, AuthOkMsg(worker_id=worker_id))
        worker_registry.register(worker_id, writer)
        logger.info("Worker %s authenticated and registered", worker_id)

        # Guard: chat_id must be set before processing worker messages
        if settings.chat_id is None:
            logger.error(
                "Worker %s connected but chat_id is not set yet. "
                "Send a message to the bot first. Closing connection.",
                worker_id,
            )
            worker_registry.unregister(worker_id)
            writer.close()
            return

        # --- Resume remote sessions for this worker ---
        await _resume_worker_sessions(
            worker_id, bot, session_manager, worker_registry,
        )

        # --- Status trackers for remote sessions ---
        from src.bot.status import StatusUpdater
        from src.db.queries import update_backend_session_id, update_session_id
        from aiogram.exceptions import TelegramRetryAfter

        _status_updaters: dict[int, StatusUpdater] = {}

        async def _get_or_create_status(topic_id: int) -> StatusUpdater:
            if topic_id not in _status_updaters:
                s = StatusUpdater(bot, settings.chat_id, topic_id)
                await s.start_turn()
                _status_updaters[topic_id] = s
            return _status_updaters[topic_id]

        async def _finalize_status(topic_id: int, cost_usd=None, duration_ms=0, tool_count=0) -> None:
            s = _status_updaters.pop(topic_id, None)
            if s:
                if cost_usd is not None:
                    await s.finalize(cost_usd=cost_usd, duration_ms=duration_ms, tool_count=tool_count)
                else:
                    await s.stop()

        # --- Message dispatch loop ---
        while True:
            msg = await recv_w2b(reader)
            if msg is None:
                break  # EOF / clean disconnect

            if isinstance(msg, AssistantTextMsg):
                parts = split_message(msg.text)
                for part in parts:
                    # Escape angle brackets so Telegram's Markdown parser does not
                    # raise "Unsupported start tag" on text like <идея>.
                    escaped_part = escape_markdown_html(part)
                    try:
                        await bot.send_message(
                            chat_id=settings.chat_id,
                            message_thread_id=msg.topic_id,
                            text=escaped_part,
                            parse_mode="Markdown",
                        )
                    except TelegramRetryAfter as e:
                        await asyncio.sleep(e.retry_after)
                        await bot.send_message(
                            chat_id=settings.chat_id,
                            message_thread_id=msg.topic_id,
                            text=escaped_part,
                            parse_mode="Markdown",
                        )
                    except Exception:
                        try:
                            await bot.send_message(
                                chat_id=settings.chat_id,
                                message_thread_id=msg.topic_id,
                                text=part,
                            )
                        except Exception as e:
                            logger.error(
                                "Failed to send assistant text to topic %d: %s",
                                msg.topic_id, e,
                            )

            elif isinstance(msg, PermissionRequestMsg):
                # Auto-mode: approve immediately without prompting
                remote_session = session_manager.get(msg.topic_id)
                if remote_session and getattr(remote_session, 'auto_mode', False):
                    response = PermissionResponseMsg(
                        request_id=msg.request_id, action="allow"
                    )
                    await worker_registry.send_to(worker_id, response)
                    continue

                _request_id, future = permission_manager.create_request()
                perm_text = format_permission_message(msg.tool_name, msg.input_data)
                keyboard = build_permission_keyboard(_request_id)
                perm_msg_id = None
                try:
                    perm_sent = await send_html_message(
                        bot,
                        chat_id=settings.chat_id,
                        message_thread_id=msg.topic_id,
                        text=perm_text,
                        reply_markup=keyboard,
                    )
                    perm_msg_id = perm_sent.message_id
                except Exception as e:
                    logger.error(
                        "Failed to send permission request to topic %d: %s",
                        msg.topic_id, e,
                    )

                async def _await_permission(
                    orig_request_id: str,
                    perm_future: asyncio.Future,
                    w_id: str,
                    worker_request_id: str,
                    topic_id: int,
                    reply_to_msg_id: int | None,
                ) -> None:
                    # Send a reminder after 45 seconds if no response
                    async def _reminder():
                        await asyncio.sleep(45)
                        if not perm_future.done():
                            try:
                                await bot.send_message(
                                    chat_id=settings.chat_id,
                                    message_thread_id=topic_id,
                                    text="⏳ Waiting for permission... session is paused",
                                    reply_to_message_id=reply_to_msg_id,
                                )
                            except Exception:
                                pass

                    reminder_task = asyncio.create_task(_reminder())
                    try:
                        result = await asyncio.wait_for(perm_future, timeout=120.0)
                    except asyncio.TimeoutError:
                        result = "deny"
                        permission_manager.expire(orig_request_id)
                        try:
                            await bot.send_message(
                                chat_id=settings.chat_id,
                                message_thread_id=topic_id,
                                text="⏱ Permission timed out (2 min) — denied",
                            )
                        except Exception:
                            pass
                    finally:
                        reminder_task.cancel()
                    response = PermissionResponseMsg(
                        request_id=worker_request_id, action=result
                    )
                    await worker_registry.send_to(w_id, response)

                asyncio.create_task(
                    _await_permission(
                        _request_id, future, worker_id, msg.request_id,
                        msg.topic_id, perm_msg_id,
                    )
                )

            elif isinstance(msg, QuestionRequestMsg):
                if question_manager is None:
                    await worker_registry.send_to(
                        worker_id,
                        QuestionResponseMsg(request_id=msg.request_id, answers={}),
                    )
                    continue

                local_request_id, future = question_manager.create_request(msg.questions)

                for i, question in enumerate(msg.questions):
                    try:
                        sent = await send_html_message(
                            bot,
                            chat_id=settings.chat_id,
                            message_thread_id=msg.topic_id,
                            text=format_question_message(question),
                            reply_markup=build_question_keyboard(local_request_id, i, question),
                        )
                        question_manager.add_message_id(local_request_id, sent.message_id)
                    except Exception as e:
                        logger.error(
                            "Failed to send question %d to topic %d: %s",
                            i,
                            msg.topic_id,
                            e,
                        )

                async def _await_questions(
                    local_id: str,
                    question_future: asyncio.Future,
                    w_id: str,
                    worker_request_id: str,
                    topic_id: int,
                ) -> None:
                    try:
                        answers = await asyncio.wait_for(question_future, timeout=300.0)
                    except asyncio.TimeoutError:
                        question_manager.expire(local_id)
                        answers = {}
                        try:
                            await bot.send_message(
                                chat_id=settings.chat_id,
                                message_thread_id=topic_id,
                                text="⏱ Questions timed out",
                            )
                        except Exception:
                            pass
                    except asyncio.CancelledError:
                        answers = {}
                    await worker_registry.send_to(
                        w_id,
                        QuestionResponseMsg(
                            request_id=worker_request_id,
                            answers=answers,
                        ),
                    )

                asyncio.create_task(
                    _await_questions(
                        local_request_id,
                        future,
                        worker_id,
                        msg.request_id,
                        msg.topic_id,
                    )
                )

            elif isinstance(msg, StatusUpdateMsg):
                status = await _get_or_create_status(msg.topic_id)
                status.track_tool(msg.tool_name, msg.input_data)

            elif isinstance(msg, UsageUpdateMsg):
                status = await _get_or_create_status(msg.topic_id)
                status.track_usage(
                    usage={
                        "input_tokens": msg.input_tokens,
                        "output_tokens": msg.output_tokens,
                        "cache_read_input_tokens": msg.cache_read_tokens,
                        "cache_creation_input_tokens": msg.cache_creation_tokens,
                    },
                    model=msg.model,
                )

            elif isinstance(msg, TurnCompletedMsg):
                # Finalize status with full data
                status = _status_updaters.get(msg.topic_id)
                if status:
                    # Feed final usage
                    status.track_usage(
                        usage={
                            "input_tokens": msg.input_tokens,
                            "output_tokens": msg.output_tokens,
                            "cache_read_input_tokens": msg.cache_read_tokens,
                        },
                        model=msg.model,
                    )
                await _finalize_status(
                    msg.topic_id,
                    cost_usd=msg.cost_usd,
                    duration_ms=msg.duration_ms,
                    tool_count=msg.tool_count,
                )
                # Persist session_id
                if msg.session_id:
                    try:
                        await update_session_id(msg.topic_id, msg.session_id)
                    except Exception:
                        pass
                # Send error if turn failed
                if msg.is_error:
                    try:
                        await bot.send_message(
                            chat_id=settings.chat_id,
                            message_thread_id=msg.topic_id,
                            text=f"❌ Error in remote session",
                        )
                    except Exception:
                        pass

            elif isinstance(msg, RateLimitMsg):
                text = None
                if msg.status == "rejected":
                    resets = ""
                    if msg.resets_at:
                        import datetime
                        dt = datetime.datetime.fromtimestamp(msg.resets_at / 1000)
                        resets = f" Resets at {dt.strftime('%H:%M')}"
                    text = f"🚫 Rate limited!{resets}"
                elif msg.status == "allowed_warning" and msg.utilization:
                    text = f"⚠️ Rate limit: {msg.utilization * 100:.0f}% used"
                if text:
                    try:
                        await bot.send_message(
                            chat_id=settings.chat_id,
                            message_thread_id=msg.topic_id,
                            text=text,
                        )
                    except Exception:
                        pass

            elif isinstance(msg, SystemNotificationMsg):
                try:
                    await send_html_message(
                        bot,
                        chat_id=settings.chat_id,
                        message_thread_id=msg.topic_id,
                        text=msg.text,
                    )
                except Exception:
                    try:
                        await bot.send_message(
                            chat_id=settings.chat_id,
                            message_thread_id=msg.topic_id,
                            text=msg.text,
                        )
                    except Exception:
                        pass

            elif isinstance(msg, SessionStartedMsg):
                if msg.session_id:
                    try:
                        await update_session_id(msg.topic_id, msg.session_id)
                    except Exception:
                        pass
                    try:
                        await update_backend_session_id(msg.topic_id, msg.session_id)
                    except Exception:
                        pass
                logger.info(
                    "Worker %s started session %s on topic %d",
                    worker_id, msg.session_id, msg.topic_id,
                )

            elif isinstance(msg, SessionEndedMsg):
                await _finalize_status(msg.topic_id)
                if msg.error:
                    try:
                        await bot.send_message(
                            chat_id=settings.chat_id,
                            message_thread_id=msg.topic_id,
                            text=f"Session ended with error: {msg.error}",
                        )
                    except Exception as e:
                        logger.error(
                            "Failed to send session error to topic %d: %s",
                            msg.topic_id,
                            e,
                        )
                else:
                    logger.info(
                        "Worker %s ended session on topic %d", worker_id, msg.topic_id
                    )

            elif isinstance(msg, McpSendMessageMsg):
                try:
                    await bot.send_message(
                        chat_id=settings.chat_id,
                        message_thread_id=msg.topic_id,
                        text=msg.text,
                    )
                except Exception as e:
                    logger.error(
                        "McpSendMessage failed for topic %d: %s", msg.topic_id, e
                    )

            elif isinstance(msg, McpReactMsg):
                try:
                    await bot.set_message_reaction(
                        chat_id=settings.chat_id,
                        message_id=msg.message_id,
                        reaction=[ReactionTypeEmoji(emoji=msg.emoji)],
                    )
                except Exception as e:
                    logger.error(
                        "McpReact failed for message %d: %s", msg.message_id, e
                    )

            elif isinstance(msg, McpEditMessageMsg):
                try:
                    await bot.edit_message_text(
                        chat_id=settings.chat_id,
                        message_id=msg.message_id,
                        text=msg.text,
                    )
                except Exception as e:
                    logger.error(
                        "McpEditMessage failed for message %d: %s", msg.message_id, e
                    )

            elif isinstance(msg, McpSendFileMsg):
                try:
                    logger.info(
                        "McpSendFile for topic %d: %s (%s bytes)",
                        msg.topic_id,
                        msg.file_name or Path(msg.file_path).name,
                        len(msg.file_bytes) if msg.file_bytes is not None else "stream",
                    )
                    if msg.file_bytes is not None:
                        document = BufferedInputFile(
                            msg.file_bytes,
                            filename=msg.file_name or Path(msg.file_path).name or "file",
                        )
                    else:
                        document = FSInputFile(msg.file_path)
                    await bot.send_document(
                        chat_id=settings.chat_id,
                        message_thread_id=msg.topic_id,
                        document=document,
                        caption=msg.caption,
                    )
                except Exception as e:
                    logger.error(
                        "McpSendFile failed for topic %d: %s", msg.topic_id, e
                    )

            elif isinstance(msg, PingMsg):
                await send_msg(writer, PongMsg())

            else:
                logger.warning("Unknown message type from worker %s: %r", worker_id, msg)

    except Exception as e:
        logger.error("Error in worker handler for %s: %s", worker_id or "unknown", e)
    finally:
        if worker_id is not None:
            worker_registry.unregister(worker_id)
            logger.info("Worker %s disconnected and unregistered", worker_id)
        if not writer.is_closing():
            writer.close()
