"""Session topic router — messages forwarded to Claude sessions."""

import logging
import os
import tempfile
from pathlib import Path

from aiogram import Bot, F, Router
from aiogram.enums import ContentType
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message, ReactionTypeEmoji

from src.sessions.backend import (
    SUPPORTED_SESSION_PROVIDERS,
    get_default_session_provider,
    is_supported_provider,
    normalize_provider,
    normalize_server_name,
    resolve_workdir_for_server,
    validate_workdir_for_server,
)
from src.sessions.manager import SessionManager
from src.sessions.permissions import PermissionCallback, PermissionManager
from src.sessions.questions import QuestionCallback, QuestionManager, build_question_keyboard
from src.sessions.remote import RemoteSession
from src.sessions.state import SessionState
from src.sessions.voice import transcribe_voice
from src.db.queries import clear_session_id, delete_session_and_topic, get_session_by_thread, insert_session, insert_topic
from src.ipc.server import WorkerRegistry
from src.config import settings

logger = logging.getLogger(__name__)

session_router = Router(name="sessions")


async def _react(message: Message, emoji: str) -> None:
    """Add an emoji reaction to a message, suppressing errors."""
    try:
        await message.react(reaction=[ReactionTypeEmoji(emoji=emoji)])
    except Exception as e:
        logger.warning("Failed to add reaction: %s", e)


def _get_runner_or_none(session_manager: SessionManager, thread_id: int):
    """Get runner, return None if missing or stopped."""
    return session_manager.get(thread_id)


def _parse_new_command_args(text: str) -> tuple[str, str, str, str] | None:
    """Parse `/new` arguments preserving legacy positional behavior.

    Supported forms:
    - /new <name> <workdir>
    - /new <name> <workdir> <server>
    - /new <name> <workdir> <server> <provider>
    - /new <name> <workdir> provider=codex
    - /new <name> <workdir> server=myhost provider=codex
    """
    args = text.split()
    if len(args) < 3:
        return None

    name = args[1]
    workdir = args[2]
    server_name = "local"
    provider = get_default_session_provider()

    for extra in args[3:]:
        if extra.startswith("server="):
            server_name = extra.split("=", 1)[1] or "local"
            continue
        if extra.startswith("provider="):
            provider = extra.split("=", 1)[1] or get_default_session_provider()
            continue
        if extra in SUPPORTED_SESSION_PROVIDERS:
            provider = extra
            continue
        server_name = extra

    return name, workdir, normalize_server_name(server_name), provider


def _default_model_for_provider(provider: str) -> str | None:
    """Return the default model to persist/use for a provider."""
    if provider == "codex":
        return None
    return "opus"
@session_router.callback_query(PermissionCallback.filter())
async def handle_permission_callback(
    query: CallbackQuery,
    callback_data: PermissionCallback,
    permission_manager: PermissionManager,
) -> None:
    """Handle inline button taps for tool permission requests.

    Resolves the pending future in PermissionManager so can_use_tool() can return.
    """
    # Defensive owner check — OwnerAuthMiddleware covers Message events only
    if query.from_user and query.from_user.id != settings.owner_user_id:
        await query.answer()
        return

    resolved = permission_manager.resolve(callback_data.request_id, callback_data.action)

    if not resolved:
        # Stale button tap — future already resolved or expired
        await query.answer(text="This permission has expired", show_alert=True)
        return

    # CRITICAL: call answer() before any other awaits to dismiss spinner (Pitfall 1)
    await query.answer()

    # Delete the permission message after resolving
    if query.message:
        try:
            await query.message.delete()
        except Exception as e:
            logger.warning("Failed to delete permission message: %s", e)


@session_router.callback_query(QuestionCallback.filter())
async def handle_question_callback(
    query: CallbackQuery,
    callback_data: QuestionCallback,
    question_manager: QuestionManager,
) -> None:
    """Handle inline button taps for AskUserQuestion.

    Single-select: one tap answers the question.
    Multi-select: taps toggle, "Done" confirms.
    """
    if query.from_user and query.from_user.id != settings.owner_user_id:
        await query.answer()
        return

    result = question_manager.handle_selection(
        callback_data.request_id,
        callback_data.q_idx,
        callback_data.opt_idx,
    )

    if result is None:
        await query.answer(text="This question has expired", show_alert=True)
        return

    action = result["action"]

    if action == "update_keyboard":
        # Multi-select toggle — rebuild keyboard with checkmarks
        await query.answer()
        keyboard = build_question_keyboard(
            result["request_id"],
            result["q_idx"],
            result["question"],
            result["selected"],
        )
        if query.message:
            try:
                await query.message.edit_reply_markup(reply_markup=keyboard)
            except Exception as e:
                logger.warning("Failed to update question keyboard: %s", e)

    elif action == "question_answered":
        # Single question answered — delete its message
        await query.answer()
        if query.message:
            try:
                await query.message.delete()
            except Exception as e:
                logger.warning("Failed to delete question message: %s", e)

    elif action == "all_done":
        # All questions answered — delete all remaining question messages
        await query.answer()
        pqs = question_manager.get_pending(callback_data.request_id)
        # Delete the current message
        if query.message:
            try:
                await query.message.delete()
            except Exception:
                pass


@session_router.message(
    F.message_thread_id.is_not(None),
    F.message_thread_id != 1,
    Command("new"),
)
async def handle_new(
    message: Message,
    bot: Bot,
    session_manager: SessionManager,
    permission_manager: PermissionManager,
    worker_registry: WorkerRegistry,
) -> None:
    """Create a new Claude session with a dedicated forum thread.

    Usage: /new <name> <workdir> [server-name]
    Works from any thread (including Orchestrator).
    """
    from aiogram.methods import CreateForumTopic

    parsed = _parse_new_command_args(message.text or "")
    if parsed is None:
        await message.reply("Usage: /new <name> <workdir> [server-name] [provider]")
        return

    name, workdir, server_name, provider = parsed
    workdir = resolve_workdir_for_server(server_name, workdir)
    validation_error = validate_workdir_for_server(server_name, workdir)
    if validation_error:
        await message.reply(validation_error)
        return

    if not is_supported_provider(provider):
        await message.reply(
            f"Unknown provider '{provider}'. Supported: {', '.join(SUPPORTED_SESSION_PROVIDERS)}."
        )
        return
    provider = normalize_provider(provider)

    if provider == "codex" and not settings.enable_codex:
        await message.reply("Codex sessions are disabled. Set ENABLE_CODEX=true to enable them.")
        return

    # Validate server connection for remote sessions
    if server_name != "local":
        if not worker_registry.is_connected(server_name):
            await message.reply(f"Server '{server_name}' is not connected.")
            return

    # Create forum topic
    topic = await bot(CreateForumTopic(
        chat_id=settings.chat_id,
        name=name,
    ))
    thread_id = topic.message_thread_id

    # Persist to DB
    model = _default_model_for_provider(provider)
    await insert_topic(thread_id, name)
    await insert_session(
        thread_id,
        workdir,
        model=model,
        server=server_name,
        provider=provider,
    )

    # Start session (local or remote)
    if server_name != "local":
        await session_manager.create_remote(
            thread_id=thread_id,
            workdir=workdir,
            worker_id=server_name,
            worker_registry=worker_registry,
            model=model,
            provider=provider,
        )
    else:
        await session_manager.create(
            thread_id=thread_id,
            workdir=workdir,
            bot=bot,
            chat_id=settings.chat_id,
            permission_manager=permission_manager,
            model=model,
            provider=provider,
        )

    await bot.send_message(
        chat_id=settings.chat_id,
        message_thread_id=thread_id,
        text=(
            f"Session <b>{name}</b> started\n"
            f"Provider: <code>{provider}</code>\n"
            f"Model: <code>{model or 'default'}</code>\n"
            f"Thread: <code>{thread_id}</code>\n"
            f"Server: {server_name}\n"
            f"Workdir: <code>{workdir}</code>"
        ),
        parse_mode="HTML",
    )
    await message.reply(f"Session '{name}' created. Thread: <code>{thread_id}</code>", parse_mode="HTML")


@session_router.message(
    F.message_thread_id.is_not(None),
    F.message_thread_id != 1,
    Command("restart"),
)
async def handle_restart(message: Message, bot: Bot, session_manager: SessionManager) -> None:
    """Restart the bot process to pick up new code. Sessions resume automatically."""
    import os
    import sys

    await message.reply("Restarting bot... Sessions will resume automatically.")
    logger.info("Restart requested by owner — stopping sessions before execv")

    # Stop all sessions BEFORE execv to avoid orphaned Claude subprocesses
    from src.db.queries import update_session_state

    for thread_id, runner in session_manager.list_all():
        try:
            await runner.stop()
            await update_session_state(thread_id, "idle")
        except Exception as e:
            logger.error("Error stopping session %d before restart: %s", thread_id, e)

    import asyncio
    await asyncio.sleep(1)

    # Replace current process with a fresh one (same args)
    os.execv(sys.executable, [sys.executable, "-m", "src"])


@session_router.message(
    F.message_thread_id.is_not(None),
    F.message_thread_id != 1,
    Command("clear"),
)
async def handle_clear(
    message: Message, bot: Bot, session_manager: SessionManager,
    permission_manager: PermissionManager,
) -> None:
    """Clear conversation — stop current session and start a fresh one in the same thread."""
    thread_id = message.message_thread_id
    row = await get_session_by_thread(thread_id)
    if row is None:
        await message.reply("No session in this topic.")
        return

    await session_manager.stop(thread_id)
    await clear_session_id(thread_id)

    await session_manager.create(
        thread_id=thread_id,
        workdir=row["workdir"],
        bot=bot,
        chat_id=settings.chat_id,
        permission_manager=permission_manager,
        session_id=None,
        model=row.get("model"),
        provider=row.get("provider"),
    )
    await message.reply("🧹 Conversation cleared. Fresh session started.")


@session_router.message(
    F.message_thread_id.is_not(None),
    F.message_thread_id != 1,
    Command("stop"),
)
async def handle_stop(message: Message, session_manager: SessionManager) -> None:
    """Interrupt the current turn (like Escape in CLI). Session stays alive."""
    thread_id = message.message_thread_id
    runner = session_manager.get(thread_id)
    if runner is None:
        await message.reply("No active session in this topic.")
        return

    interrupted = await runner.interrupt()
    if interrupted:
        await message.reply("Interrupted.")
    else:
        await message.reply("Nothing running to interrupt.")


@session_router.message(
    F.message_thread_id.is_not(None),
    F.message_thread_id != 1,
    Command("close"),
)
async def handle_close(message: Message, bot: Bot, session_manager: SessionManager) -> None:
    """Close session and delete the forum topic.

    Stops the Claude session, cleans up DB records, and removes the Telegram topic.
    """
    thread_id = message.message_thread_id
    runner = session_manager.get(thread_id)

    # Stop session if active
    if runner is not None:
        await session_manager.stop(thread_id)

    # Clean up DB
    await delete_session_and_topic(thread_id)

    # Delete forum topic (fall back to closing if deletion fails)
    try:
        await bot.delete_forum_topic(
            chat_id=settings.chat_id,
            message_thread_id=thread_id,
        )
    except Exception:
        logger.warning("Could not delete topic %d, trying to close instead", thread_id)
        try:
            await bot.close_forum_topic(
                chat_id=settings.chat_id,
                message_thread_id=thread_id,
            )
        except Exception as e:
            logger.error("Could not close topic %d: %s", thread_id, e)


@session_router.message(
    F.message_thread_id.is_not(None),
    F.message_thread_id != 1,
    F.content_type == ContentType.VOICE,
)
async def handle_voice(message: Message, session_manager: SessionManager) -> None:
    """Transcribe voice message and enqueue text to Claude session (INPT-02).

    Reaction flow: 🎤 on receipt (transcribing) → 👀 when enqueued to session.
    """
    thread_id = message.message_thread_id
    runner = session_manager.get(thread_id)

    if runner is None:
        return

    if runner.state == SessionState.STOPPED:
        await message.reply("Session is stopped. Use /new to create a new one.")
        return

    # 🎤 = transcribing (not yet in session)
    await _react(message, "🎤")

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".ogg", delete=False) as tmp:
            tmp_path = tmp.name
        await message.bot.download(file=message.voice.file_id, destination=tmp_path)
        text = await transcribe_voice(tmp_path)
        if not text.strip():
            await message.reply("Could not transcribe voice message.")
            return
        # 👀 = enqueued to Claude session
        await _react(message, "👀")
        await runner.enqueue(text, reply_to_message_id=message.message_id)
    except ConnectionError as e:
        await message.reply(str(e))
    except Exception as e:
        logger.error("Voice transcription error in thread %d: %s", thread_id, e)
        await message.reply(f"Voice transcription failed: {e}")
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except Exception:
                pass


@session_router.message(
    F.message_thread_id.is_not(None),
    F.message_thread_id != 1,
    F.content_type == ContentType.PHOTO,
)
async def handle_photo(message: Message, session_manager: SessionManager) -> None:
    """Download photo to workdir and enqueue path description to Claude session (INPT-03)."""
    thread_id = message.message_thread_id
    runner = session_manager.get(thread_id)

    if runner is None:
        return

    if runner.state == SessionState.STOPPED:
        await message.reply("Session is stopped. Use /new to create a new one.")
        return

    try:
        photo = message.photo[-1]
        # Download photo to memory
        from io import BytesIO
        buf = BytesIO()
        await message.bot.download(file=photo.file_id, destination=buf)
        image_data = buf.getvalue()
        caption = message.caption or ""

        logger.info(
            "Photo received in topic %d (%d bytes, remote=%s)",
            thread_id,
            len(image_data),
            isinstance(runner, RemoteSession),
        )

        await _react(message, "👀")

        if isinstance(runner, RemoteSession):
            logger.info("Forwarding photo from topic %d to worker %s", thread_id, runner.worker_id)
            await runner.enqueue_file(
                file_name=f"photo_{photo.file_unique_id}.jpg",
                file_bytes=image_data,
                caption=caption,
                media_type="image/jpeg",
                reply_to_message_id=message.message_id,
                is_image=True,
            )
        elif hasattr(runner, "enqueue_image"):
            await runner.enqueue_image(
                image_data=image_data,
                media_type="image/jpeg",
                caption=caption,
                reply_to_message_id=message.message_id,
            )
        else:
            dest = Path(runner.workdir) / f"photo_{photo.file_unique_id}.jpg"
            dest.write_bytes(image_data)
            enqueue_text = f"User sent a photo: {dest}\n{caption}".strip()
            await runner.enqueue(enqueue_text, reply_to_message_id=message.message_id)
    except ConnectionError as e:
        await message.reply(str(e))
    except Exception as e:
        logger.error("Photo download error in thread %d: %s", thread_id, e)
        await message.reply(f"Failed to download photo: {e}")


@session_router.message(
    F.message_thread_id.is_not(None),
    F.message_thread_id != 1,
    F.content_type == ContentType.DOCUMENT,
)
async def handle_document(message: Message, session_manager: SessionManager) -> None:
    """Download document to workdir and enqueue path description to Claude session (INPT-04)."""
    thread_id = message.message_thread_id
    runner = session_manager.get(thread_id)

    if runner is None:
        return

    if runner.state == SessionState.STOPPED:
        await message.reply("Session is stopped. Use /new to create a new one.")
        return

    try:
        filename = message.document.file_name or f"file_{message.document.file_unique_id}"
        caption = message.caption or ""
        from io import BytesIO

        buf = BytesIO()
        await message.bot.download(file=message.document.file_id, destination=buf)
        file_bytes = buf.getvalue()
        caption = message.caption or ""
        # 👀 = enqueued to Claude session
        await _react(message, "👀")
        if isinstance(runner, RemoteSession):
            await runner.enqueue_file(
                file_name=filename,
                file_bytes=file_bytes,
                caption=caption,
                media_type=message.document.mime_type,
                reply_to_message_id=message.message_id,
                is_image=False,
            )
        else:
            dest = Path(runner.workdir) / filename
            dest.write_bytes(file_bytes)
            enqueue_text = f"User sent file: {filename} at {dest}\n{caption}".strip()
            await runner.enqueue(enqueue_text, reply_to_message_id=message.message_id)
    except ConnectionError as e:
        await message.reply(str(e))
    except Exception as e:
        logger.error("Document download error in thread %d: %s", thread_id, e)
        await message.reply(f"Failed to download file: {e}")


@session_router.message(
    F.message_thread_id.is_not(None),
    F.message_thread_id != 1,
    Command("list"),
)
async def handle_list_in_session(
    message: Message,
    session_manager: SessionManager,
    worker_registry: "WorkerRegistry",
) -> None:
    """List all active sessions — works from any thread."""
    from src.sessions.remote import RemoteSession
    from src.ipc.server import WorkerRegistry

    sessions = session_manager.list_all()
    if not sessions:
        await message.reply("No active sessions.")
        return

    lines = []
    for thread_id, runner in sessions:
        if isinstance(runner, RemoteSession):
            server = runner.worker_id
            status = "(connected)" if runner.is_alive else "(disconnected)"
        else:
            server = "local"
            status = ""
        provider = getattr(runner, "provider", get_default_session_provider())
        server_info = f"on <i>{server}</i> {status}".strip()
        auto = " 🤖auto" if getattr(runner, "auto_mode", False) else ""
        lines.append(
            f"- <b>{thread_id}</b>: {runner.workdir} [{runner.state.name}] "
            f"provider=<code>{provider}</code> {server_info}{auto}"
        )

    # Also show connected workers
    workers = worker_registry.list_workers()
    if workers:
        lines.append(f"\nWorkers: {', '.join(workers)}")

    await message.reply("\n".join(lines), parse_mode="HTML")


@session_router.message(
    F.message_thread_id.is_not(None),
    F.message_thread_id != 1,
)
async def handle_session_message(message: Message, session_manager: SessionManager) -> None:
    """Forward all text messages to the Claude session.

    All slash commands except reserved ones (/stop, /close, /list) are forwarded as raw text
    via runner.enqueue(text). This lets Claude handle /model, /clear, /compact, /reset,
    /help, /config etc. natively.
    """
    thread_id = message.message_thread_id
    runner = session_manager.get(thread_id)

    if runner is None:
        return  # No active session — silently ignore

    if runner.state == SessionState.STOPPED:
        await message.reply("Session is stopped. Use /new to create a new one.")
        return

    text = message.text or ""
    if not text:
        return

    # 👀 = enqueued to Claude session
    await _react(message, "👀")

    # Enqueue to session with reply tracking
    try:
        await runner.enqueue(text, reply_to_message_id=message.message_id)
    except ConnectionError as e:
        await message.reply(str(e))
