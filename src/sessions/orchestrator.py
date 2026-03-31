"""Orchestrator session that manages other Telegram provider sessions."""

import asyncio
import contextlib
import html
import logging
from pathlib import Path

from aiogram import Bot
from aiogram.methods import CreateForumTopic
from claude_agent_sdk import create_sdk_mcp_server, tool

from src.config import settings
from src.bot.output import html_code, html_bold, send_html_message
from src.sessions.backend import (
    SUPPORTED_SESSION_PROVIDERS,
    get_default_session_provider,
    get_orchestrator_server_guidance,
    is_supported_provider,
    load_private_infra_context,
    normalize_provider,
    normalize_server_name,
    resolve_workdir_for_server,
    validate_workdir_for_server,
)
from src.db.queries import insert_session, insert_topic, get_all_active_sessions
from src.sessions.manager import SessionManager
from src.sessions.permissions import PermissionManager

logger = logging.getLogger(__name__)

ORCHESTRATOR_TOPIC_NAME = "🎯 Orchestrator"
_ORCHESTRATOR_FALLBACK_LOCKS: dict[int, asyncio.Lock] = {}

WELCOME_MESSAGE = """\
<b>🎯 Telegram Multi-Thread Router</b>

Provider sessions in Telegram — each thread is an isolated workspace.

<b>Commands (work from any thread):</b>
<code>/new &lt;name&gt; &lt;workdir&gt; [server] [provider]</code> — create a new session
<code>/list</code> — list active sessions
<code>/restart</code> — restart bot (sessions resume)
<code>/stop</code> — interrupt current turn
<code>/close</code> — stop session + delete thread

<b>Input types:</b>
💬 Text — forwarded to the active provider
🎤 Voice — transcribed via Whisper, then forwarded
📷 Photo / 📎 Files — downloaded to workdir, path sent to the provider session

<b>Permissions:</b>
Read-only tools are auto-approved.
Write / exec / side-effecting tools ask for confirmation by default.

<b>Auto-mode:</b>
Enable it per session if you want that session to stop asking for permissions.
Examples:
- <code>Enable auto-mode for session 141680</code>
- <code>Disable auto-mode for session 141680</code>

<b>Orchestrator (this thread):</b>
I can create, manage, stop sessions, and toggle auto-mode for them.
Just ask in natural language.
"""


def _orchestrator_session_created_text(
    *,
    name: str,
    thread_id: int,
    provider: str,
    model: str | None,
    server: str,
    workdir: str,
) -> str:
    """Return a stable acknowledgment for a newly created session."""
    return (
        "✅ Session created\n"
        f"Name: {html_bold(name)}\n"
        f"Thread: {html_code(thread_id)}\n"
        f"Provider: {html_code(provider)}\n"
        f"Model: {html_code(model or 'default')}\n"
        f"Server: {html.escape(server)}\n"
        f"Workdir: {html_code(workdir)}"
    )


def _orchestrator_auto_mode_text(thread_id: int, enabled: bool) -> str:
    """Return a stable acknowledgment for auto-mode changes."""
    status = "enabled" if enabled else "disabled"
    return (
        f"🤖 Auto-mode {html_bold(status)}\n"
        f"Thread: {html_code(thread_id)}"
    )


def _orchestrator_session_stopped_text(thread_id: int) -> str:
    """Return a stable acknowledgment for a stopped session."""
    return f"🛑 Session stopped\nThread: {html_code(thread_id)}"


async def _notify_orchestrator(
    bot: Bot,
    *,
    chat_id: int,
    orchestrator_thread_id: int | None,
    text: str,
) -> None:
    """Best-effort send an acknowledgment to the orchestrator thread."""
    if orchestrator_thread_id is None:
        return
    with contextlib.suppress(Exception):
        await send_html_message(
            bot,
            chat_id=chat_id,
            message_thread_id=orchestrator_thread_id,
            text=text,
        )


def _build_orchestrator_system_prompt(provider: str) -> str:
    """Return the provider-specific orchestrator prompt."""
    default_provider = get_default_session_provider()
    provider_label = "Codex" if provider == "codex" else "Claude Code"
    private_context = load_private_infra_context()
    extra_context = f"\n\nPrivate infrastructure context:\n{private_context}" if private_context else ""
    return (
        f"You are a full {provider_label} session with additional session management capabilities.\n\n"
        "You have all standard coding/session tools plus orchestrator MCP tools:\n"
        f"- create_session(name, workdir, server, provider): Create a new session in a new Telegram thread. "
        f"Provider defaults to \"{default_provider}\" and can also be one of: "
        f"{', '.join(SUPPORTED_SESSION_PROVIDERS)}.\n"
        "- list_sessions(): List all active sessions with status and goals\n"
        "- stop_session(thread_id): Stop a session\n"
        "- auto_mode(thread_id, enable): Toggle auto-approvals for a session\n"
        "- goal_mode(thread_id, goal_text, enable): Set a goal for a session. "
        "When enabled, you will be notified after each turn and on idle (10min). "
        "Review progress and push the session forward via send_to_session, or disable goal_mode when done.\n"
        "- send_to_session(thread_id, message): Send a message to a session (enqueue a user prompt)\n\n"
        "You can browse filesystems, run commands, inspect projects, and manage sessions. "
        "When the user asks to work on a project on a specific server, use create_session to spawn "
        "a dedicated session for it.\n"
        "Critical path rules:\n"
        "- Never pass a macOS /Users/... path to a remote server session.\n"
        "- If the target server is remote and the user names a repo rather than an absolute server path, "
        "resolve the server path first.\n"
        "- If you know both local and server paths for a repo, use the server path on remote workers.\n\n"
        f"{get_orchestrator_server_guidance()}"
        f"{extra_context}"
    )


def _orchestrator_model_for_provider(provider: str) -> str | None:
    """Return the default orchestrator model for the provider."""
    if provider == "codex":
        return None
    return "sonnet"


def _orchestrator_provider_candidates(preferred: str | None) -> list[str]:
    """Return startup/fallback candidates in priority order."""
    primary = normalize_provider(preferred or get_default_session_provider())
    ordered = [primary]
    if primary != "claude":
        ordered.append("claude")
    if primary != "codex" and settings.enable_codex:
        ordered.append("codex")
    elif primary == "codex":
        ordered.append("claude")

    candidates: list[str] = []
    for provider in ordered:
        if provider == "codex" and not settings.enable_codex:
            continue
        if provider not in candidates:
            candidates.append(provider)
    return candidates


# --- Goal mode state ---
_goal_idle_watchdogs: dict[int, asyncio.Task] = {}
_goal_last_notify: dict[int, float] = {}  # thread_id -> monotonic timestamp
_GOAL_NOTIFY_DEBOUNCE = 30.0  # seconds
_GOAL_IDLE_TIMEOUT = 600.0  # 10 minutes


def _orchestrator_fallback_lock(thread_id: int) -> asyncio.Lock:
    """Return a stable per-thread fallback lock."""
    lock = _ORCHESTRATOR_FALLBACK_LOCKS.get(thread_id)
    if lock is None:
        lock = asyncio.Lock()
        _ORCHESTRATOR_FALLBACK_LOCKS[thread_id] = lock
    return lock


def create_orchestrator_mcp_server(
    bot: Bot,
    chat_id: int,
    orchestrator_thread_id: int,
    session_manager: SessionManager,
    permission_manager: PermissionManager,
    worker_registry,
):
    """Create MCP server with session management tools for the orchestrator."""

    @tool(
        "create_session",
        "Create a new session in a new Telegram thread. Returns the thread ID. "
        f"Provider defaults to '{get_default_session_provider()}' and may be 'codex' when enabled. "
        "Model: 'opus' (default), 'sonnet', 'haiku', or full name like 'claude-sonnet-4-6'.",
        {"name": str, "workdir": str, "server": str, "provider": str, "model": str},
    )
    async def create_session(args: dict) -> dict:
        name = args["name"]
        server_name = normalize_server_name(args.get("server", "local"))
        workdir = resolve_workdir_for_server(server_name, args["workdir"])
        raw_provider = args.get("provider") or get_default_session_provider()
        model = args.get("model")

        if not is_supported_provider(raw_provider):
            return {"content": [{"type": "text", "text": f"Error: Unsupported provider '{raw_provider}'"}]}
        provider = normalize_provider(raw_provider)
        validation_error = validate_workdir_for_server(server_name, workdir)
        if validation_error:
            return {"content": [{"type": "text", "text": f"Error: {validation_error}"}]}
        if model in (None, "", "default"):
            model = _orchestrator_model_for_provider(provider)
        elif provider == "codex" and model == "opus":
            model = None

        if provider == "codex" and not settings.enable_codex:
            return {"content": [{"type": "text", "text": "Error: Codex sessions are disabled by config"}]}

        try:
            # Validate remote server
            if server_name != "local" and not worker_registry.is_connected(server_name):
                return {"content": [{"type": "text", "text": f"Error: Server '{server_name}' not connected"}]}

            # Create forum topic
            topic = await bot(CreateForumTopic(chat_id=chat_id, name=name))
            thread_id = topic.message_thread_id

            # Persist
            await insert_topic(thread_id, name)
            await insert_session(
                thread_id,
                workdir,
                model=model,
                server=server_name,
                provider=provider,
            )

            # Start session
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
                    chat_id=chat_id,
                    permission_manager=permission_manager,
                    model=model,
                    provider=provider,
                )

            await send_html_message(
                bot,
                chat_id=chat_id,
                message_thread_id=thread_id,
                text=(
                    f"Session {html_bold(name)} started\n"
                    f"Provider: {html_code(provider)}\n"
                    f"Model: {html_code(model or 'default')}\n"
                    f"Thread: {html_code(thread_id)}\n"
                    f"Server: {html.escape(server_name)}\n"
                    f"Workdir: {html_code(workdir)}"
                ),
            )
            await _notify_orchestrator(
                bot,
                chat_id=chat_id,
                orchestrator_thread_id=orchestrator_thread_id,
                text=_orchestrator_session_created_text(
                    name=name,
                    thread_id=thread_id,
                    provider=provider,
                    model=model,
                    server=server_name,
                    workdir=workdir,
                ),
            )

            return {
                "content": [{
                    "type": "text",
                    "text": (
                        f"Session '{name}' created. Thread ID: {thread_id}, "
                        f"provider: {provider}, model: {model or 'default'}, server: {server_name}"
                    ),
                }]
            }
        except Exception as e:
            logger.error("create_session error: %s", e)
            return {"content": [{"type": "text", "text": f"Error creating session: {e}"}]}

    @tool(
        "list_sessions",
        "List all active sessions with their status, server, provider, and working directory",
        {},
    )
    async def list_sessions(args: dict) -> dict:
        from src.sessions.remote import RemoteSession

        sessions = session_manager.list_all()
        if not sessions:
            return {"content": [{"type": "text", "text": "No active sessions."}]}

        lines = []
        for thread_id, runner in sessions:
            server = runner.worker_id if isinstance(runner, RemoteSession) else "local"
            provider = getattr(runner, "provider", get_default_session_provider())
            line = (
                f"- Thread {thread_id}: {runner.workdir} [{runner.state.name}] "
                f"provider={provider} on {server}"
            )
            goal = getattr(runner, "goal_text", None)
            if goal:
                line += f" 🎯 goal: {goal}"
            lines.append(line)

        return {"content": [{"type": "text", "text": "\n".join(lines)}]}

    @tool(
        "stop_session",
        "Stop an active session by its thread ID",
        {"thread_id": int},
    )
    async def stop_session(args: dict) -> dict:
        thread_id = args["thread_id"]
        try:
            runner = session_manager.get(thread_id)
            if not runner:
                return {"content": [{"type": "text", "text": f"No session found for thread {thread_id}"}]}

            await session_manager.stop(thread_id)
            from src.db.queries import update_session_state
            await update_session_state(thread_id, "stopped")
            await _notify_orchestrator(
                bot,
                chat_id=chat_id,
                orchestrator_thread_id=orchestrator_thread_id,
                text=_orchestrator_session_stopped_text(thread_id),
            )

            return {"content": [{"type": "text", "text": f"Session {thread_id} stopped."}]}
        except Exception as e:
            logger.error("stop_session error: %s", e)
            return {"content": [{"type": "text", "text": f"Error: {e}"}]}

    @tool(
        "auto_mode",
        "Toggle auto-mode for a session — auto-approves all permissions without prompting the user. "
        "Pass enable=true to enable, enable=false to disable.",
        {"thread_id": int, "enable": bool},
    )
    async def auto_mode(args: dict) -> dict:
        thread_id = args["thread_id"]
        enable = args["enable"]
        runner = session_manager.get(thread_id)
        if not runner:
            return {"content": [{"type": "text", "text": f"No session found for thread {thread_id}"}]}

        runner.auto_mode = enable
        from src.db.queries import update_auto_mode
        await update_auto_mode(thread_id, enable)
        status = "enabled" if enable else "disabled"

        try:
            await bot.send_message(
                chat_id=chat_id,
                message_thread_id=thread_id,
                text=f"🤖 Auto-mode {status}",
            )
        except Exception:
            pass
        await _notify_orchestrator(
            bot,
            chat_id=chat_id,
            orchestrator_thread_id=orchestrator_thread_id,
            text=_orchestrator_auto_mode_text(thread_id, enable),
        )

        return {"content": [{"type": "text", "text": f"Auto-mode {status} for thread {thread_id}"}]}

    # --- Goal mode helpers (closures over bot/chat_id/orchestrator_thread_id) ---

    import time

    async def _goal_notify(thread_id: int, reason: str) -> None:
        """Inject a goal-mode notification into the orchestrator session so it acts on it."""
        now = time.monotonic()
        last = _goal_last_notify.get(thread_id, 0)
        if now - last < _GOAL_NOTIFY_DEBOUNCE:
            return
        _goal_last_notify[thread_id] = now

        runner = session_manager.get(thread_id)
        goal = getattr(runner, "goal_text", None) or "unknown"
        state = runner.state.name if runner else "UNKNOWN"
        prompt = (
            f"[GOAL CHECK] Session thread {thread_id} — {reason}.\n"
            f"Goal: {goal}\n"
            f"Current state: {state}\n\n"
            "Check on this session's progress. If the goal is not yet achieved, "
            "use send_to_session to push it forward with a specific instruction. "
            "If the goal IS achieved, call goal_mode(thread_id={thread_id}, goal_text='', enable=false)."
        )
        # Inject into the orchestrator's own session queue so Claude processes it
        orch_runner = session_manager.get(orchestrator_thread_id)
        if orch_runner is not None:
            try:
                await orch_runner.enqueue(prompt)
            except Exception as e:
                logger.warning("Failed to enqueue goal notification for thread %d: %s", thread_id, e)
        else:
            logger.warning("Goal notify: orchestrator runner not found for thread %d", orchestrator_thread_id)

    async def _goal_turn_complete_callback(thread_id: int) -> None:
        await _goal_notify(thread_id, "turn completed")

    def _make_turn_callback(tid: int):
        async def _cb():
            await _goal_turn_complete_callback(tid)
        return _cb

    async def _goal_idle_watchdog(thread_id: int) -> None:
        """Wait GOAL_IDLE_TIMEOUT then notify orchestrator if session is still idle."""
        try:
            while True:
                await asyncio.sleep(_GOAL_IDLE_TIMEOUT)
                runner = session_manager.get(thread_id)
                if runner is None or not getattr(runner, "goal_text", None):
                    return
                if runner.state.name == "IDLE":
                    await _goal_notify(thread_id, f"idle for {int(_GOAL_IDLE_TIMEOUT / 60)}min")
        except asyncio.CancelledError:
            pass

    def _start_goal_watchdog(thread_id: int) -> None:
        old = _goal_idle_watchdogs.pop(thread_id, None)
        if old is not None:
            old.cancel()
        _goal_idle_watchdogs[thread_id] = asyncio.create_task(_goal_idle_watchdog(thread_id))

    def _stop_goal_watchdog(thread_id: int) -> None:
        old = _goal_idle_watchdogs.pop(thread_id, None)
        if old is not None:
            old.cancel()
        _goal_last_notify.pop(thread_id, None)

    # --- New MCP tools ---

    @tool(
        "goal_mode",
        "Set a goal for a session and monitor its progress. "
        "The orchestrator will be notified after each turn and on idle (10min). "
        "Also enables auto_mode. Pass enable=false to disable and clear the goal.",
        {"thread_id": int, "goal_text": str, "enable": bool},
    )
    async def goal_mode(args: dict) -> dict:
        thread_id = args["thread_id"]
        goal_text = args.get("goal_text", "")
        enable = args.get("enable", True)
        runner = session_manager.get(thread_id)
        if not runner:
            return {"content": [{"type": "text", "text": f"No session found for thread {thread_id}"}]}

        from src.db.queries import update_goal_text, update_auto_mode

        if enable:
            if not goal_text:
                return {"content": [{"type": "text", "text": "Error: goal_text is required when enabling goal mode"}]}
            runner.goal_text = goal_text
            runner._on_turn_complete = _make_turn_callback(thread_id)
            runner.auto_mode = True
            await update_goal_text(thread_id, goal_text)
            await update_auto_mode(thread_id, True)
            _start_goal_watchdog(thread_id)

            try:
                await bot.send_message(
                    chat_id=chat_id, message_thread_id=thread_id,
                    text=f"🎯 Goal mode enabled: {goal_text}\n🤖 Auto-mode enabled",
                )
            except Exception:
                pass
            await _notify_orchestrator(
                bot, chat_id=chat_id, orchestrator_thread_id=orchestrator_thread_id,
                text=f"🎯 Goal mode enabled for thread {thread_id}\nGoal: {html.escape(goal_text)}",
            )
            return {"content": [{"type": "text", "text": f"Goal mode enabled for thread {thread_id}: {goal_text}"}]}
        else:
            runner.goal_text = None
            runner._on_turn_complete = None
            runner.auto_mode = False
            await update_goal_text(thread_id, None)
            await update_auto_mode(thread_id, False)
            _stop_goal_watchdog(thread_id)

            try:
                await bot.send_message(
                    chat_id=chat_id, message_thread_id=thread_id,
                    text="🎯 Goal mode disabled\n🤖 Auto-mode disabled",
                )
            except Exception:
                pass
            await _notify_orchestrator(
                bot, chat_id=chat_id, orchestrator_thread_id=orchestrator_thread_id,
                text=f"🎯 Goal mode disabled for thread {thread_id}",
            )
            return {"content": [{"type": "text", "text": f"Goal mode disabled for thread {thread_id}"}]}

    @tool(
        "send_to_session",
        "Send a message to a session — injects a user prompt into the session's queue.",
        {"thread_id": int, "message": str},
    )
    async def send_to_session(args: dict) -> dict:
        thread_id = args["thread_id"]
        message = args["message"]
        runner = session_manager.get(thread_id)
        if not runner:
            return {"content": [{"type": "text", "text": f"No session found for thread {thread_id}"}]}

        await runner.enqueue(message)
        return {"content": [{"type": "text", "text": f"Message sent to thread {thread_id}"}]}

    return create_sdk_mcp_server(
        "orchestrator",
        tools=[create_session, list_sessions, stop_session, auto_mode, goal_mode, send_to_session],
    )


async def _wait_for_orchestrator_startup(runner, provider: str) -> None:
    """Wait briefly to catch immediate startup failure before declaring success."""
    for _ in range(30):
        if not runner.is_alive or runner.state.name == "STOPPED":
            raise RuntimeError(f"{provider} orchestrator stopped during startup")
        if provider != "codex" or getattr(runner, "backend_session_id", None):
            return
        await asyncio.sleep(0.1)
    if provider == "codex":
        raise RuntimeError("codex orchestrator did not obtain a backend thread id during startup")


def _build_orchestrator_runner(
    *,
    provider: str,
    thread_id: int,
    chat_id: int,
    bot: Bot,
    session_manager: SessionManager,
    permission_manager: PermissionManager,
    question_manager,
    worker_registry,
    model: str | None,
    session_id: str | None,
    backend_session_id: str | None,
    orchestrator_mcp_url: str | None,
):
    """Build a provider-specific orchestrator runner without starting it."""
    from src.sessions.codex_runner import CodexRunner
    from src.sessions.runner import SessionRunner

    orch_mcp = create_orchestrator_mcp_server(
        bot,
        chat_id,
        thread_id,
        session_manager,
        permission_manager,
        worker_registry,
    )

    if provider == "codex":
        if not settings.enable_codex:
            raise RuntimeError("Codex is disabled but configured as the orchestrator provider")
        if not orchestrator_mcp_url:
            raise RuntimeError("Codex orchestrator MCP server URL is missing")
        return CodexRunner(
            thread_id=thread_id,
            workdir=str(Path.home()),
            bot=bot,
            chat_id=chat_id,
            permission_manager=permission_manager,
            question_manager=question_manager,
            backend_session_id=backend_session_id or session_id,
            model=model,
            developer_instructions=_build_orchestrator_system_prompt(provider),
            mcp_server_urls={"orchestrator": orchestrator_mcp_url},
        )

    runner = SessionRunner(
        thread_id=thread_id,
        workdir=str(Path.home()),
        bot=bot,
        chat_id=chat_id,
        permission_manager=permission_manager,
        question_manager=question_manager,
        model=model,
        session_id=session_id,
    )
    runner._extra_mcp = {"orchestrator": orch_mcp}
    runner._system_prompt_override = _build_orchestrator_system_prompt(provider)
    return runner


async def _start_orchestrator_runner(
    *,
    provider: str,
    thread_id: int,
    chat_id: int,
    bot: Bot,
    session_manager: SessionManager,
    permission_manager: PermissionManager,
    question_manager,
    worker_registry,
    model: str | None,
    session_id: str | None,
    backend_session_id: str | None,
    orchestrator_mcp_url: str | None,
):
    """Create, register, start, and validate an orchestrator runner."""
    runner = _build_orchestrator_runner(
        provider=provider,
        thread_id=thread_id,
        chat_id=chat_id,
        bot=bot,
        session_manager=session_manager,
        permission_manager=permission_manager,
        question_manager=question_manager,
        worker_registry=worker_registry,
        model=model,
        session_id=session_id,
        backend_session_id=backend_session_id,
        orchestrator_mcp_url=orchestrator_mcp_url,
    )

    async with session_manager._lock:
        session_manager._sessions[thread_id] = runner
    try:
        await runner.start()
        await _wait_for_orchestrator_startup(runner, provider)
        return runner
    except Exception:
        async with session_manager._lock:
            if session_manager._sessions.get(thread_id) is runner:
                session_manager._sessions.pop(thread_id, None)
        with contextlib.suppress(Exception):
            await runner.stop()
        raise


def _attach_orchestrator_fallback(
    *,
    runner,
    thread_id: int,
    current_provider: str,
    bot: Bot,
    chat_id: int,
    session_manager: SessionManager,
    permission_manager: PermissionManager,
    question_manager,
    worker_registry,
    orchestrator_mcp_url: str | None,
) -> None:
    """Attach a runtime fallback callback to the active orchestrator runner."""

    async def _on_provider_exhausted(reason: str) -> None:
        await _fallback_orchestrator_provider(
            thread_id=thread_id,
            current_provider=current_provider,
            reason=reason,
            bot=bot,
            chat_id=chat_id,
            session_manager=session_manager,
            permission_manager=permission_manager,
            question_manager=question_manager,
            worker_registry=worker_registry,
            orchestrator_mcp_url=orchestrator_mcp_url,
        )

    runner._provider_exhausted_callback = _on_provider_exhausted


async def _fallback_orchestrator_provider(
    *,
    thread_id: int,
    current_provider: str,
    reason: str,
    bot: Bot,
    chat_id: int,
    session_manager: SessionManager,
    permission_manager: PermissionManager,
    question_manager,
    worker_registry,
    orchestrator_mcp_url: str | None,
) -> bool:
    """Switch the orchestrator thread to another enabled provider."""
    from src.db.queries import update_session_provider

    lock = _orchestrator_fallback_lock(thread_id)
    async with lock:
        active = session_manager.get(thread_id)
        if active is None or getattr(active, "provider", None) != current_provider:
            return False

        fallback_candidates = [
            provider
            for provider in _orchestrator_provider_candidates(current_provider)
            if provider != current_provider
        ]
        if not fallback_candidates:
            await send_html_message(
                bot,
                chat_id=chat_id,
                message_thread_id=thread_id,
                text=(
                    f"⚠️ Orchestrator provider {html_code(current_provider)} became unavailable.\n"
                    "No fallback provider is available."
                ),
            )
            return False

        async with session_manager._lock:
            old_runner = session_manager._sessions.pop(thread_id, None)
        if old_runner is not None:
            with contextlib.suppress(Exception):
                await old_runner.stop()

        for fallback_provider in fallback_candidates:
            fallback_model = _orchestrator_model_for_provider(fallback_provider)
            await update_session_provider(thread_id, fallback_provider, fallback_model)
            try:
                new_runner = await _start_orchestrator_runner(
                    provider=fallback_provider,
                    thread_id=thread_id,
                    chat_id=chat_id,
                    bot=bot,
                    session_manager=session_manager,
                    permission_manager=permission_manager,
                    question_manager=question_manager,
                    worker_registry=worker_registry,
                    model=fallback_model,
                    session_id=None,
                    backend_session_id=None,
                    orchestrator_mcp_url=orchestrator_mcp_url,
                )
                _attach_orchestrator_fallback(
                    runner=new_runner,
                    thread_id=thread_id,
                    current_provider=fallback_provider,
                    bot=bot,
                    chat_id=chat_id,
                    session_manager=session_manager,
                    permission_manager=permission_manager,
                    question_manager=question_manager,
                    worker_registry=worker_registry,
                    orchestrator_mcp_url=orchestrator_mcp_url,
                )
                await send_html_message(
                    bot,
                    chat_id=chat_id,
                    message_thread_id=thread_id,
                    text=(
                        "⚠️ Orchestrator provider fallback activated.\n"
                        f"From: {html_code(current_provider)}\n"
                        f"To: {html_code(fallback_provider)}\n"
                        f"Reason: {html.escape(reason)}"
                    ),
                )
                logger.warning(
                    "Orchestrator provider fallback: %s -> %s (%s)",
                    current_provider,
                    fallback_provider,
                    reason,
                )
                return True
            except Exception as e:
                logger.error(
                    "Failed to switch orchestrator from %s to %s: %s",
                    current_provider,
                    fallback_provider,
                    e,
                )

        await send_html_message(
            bot,
            chat_id=chat_id,
            message_thread_id=thread_id,
            text=(
                f"❌ Orchestrator provider {html_code(current_provider)} failed.\n"
                f"Reason: {html.escape(reason)}\n"
                "Fallback providers could not be started."
            ),
        )
        return False


async def ensure_orchestrator(
    bot: Bot,
    chat_id: int,
    session_manager: SessionManager,
    permission_manager: PermissionManager,
    question_manager,
    worker_registry,
    orchestrator_mcp_url: str | None = None,
) -> int | None:
    """Ensure the orchestrator thread and session exist. Returns thread_id or None on error."""
    from src.db.queries import (
        get_orchestrator_topic,
        get_session_by_thread,
        insert_topic,
        insert_session,
        update_session_provider,
    )

    # Check if orchestrator topic already exists in DB
    orch = await get_orchestrator_topic()
    preferred_provider = get_default_session_provider()
    if orch:
        thread_id = orch["thread_id"]
        logger.info("Orchestrator topic exists: thread=%d", thread_id)
    else:
        # Create new orchestrator topic
        try:
            topic = await bot(CreateForumTopic(chat_id=chat_id, name=ORCHESTRATOR_TOPIC_NAME))
            thread_id = topic.message_thread_id
            await insert_topic(thread_id, ORCHESTRATOR_TOPIC_NAME, is_orchestrator=True)
            await insert_session(
                thread_id,
                str(Path.home()),
                model=_orchestrator_model_for_provider(preferred_provider),
                server="local",
                provider=preferred_provider,
            )
            await bot.send_message(
                chat_id=chat_id,
                message_thread_id=thread_id,
                text=WELCOME_MESSAGE,
                parse_mode="HTML",
            )
            logger.info("Created orchestrator topic: thread=%d", thread_id)
        except Exception as e:
            logger.error("Failed to create orchestrator topic: %s", e)
            return None

    # Start orchestrator session if not already running
    if session_manager.get(thread_id):
        logger.info("Orchestrator session already running")
        return thread_id

    existing = await get_session_by_thread(thread_id)
    persisted_provider = normalize_provider(existing.get("provider") if existing else preferred_provider)
    preferred_provider = persisted_provider
    session_id = existing["session_id"] if existing else None
    backend_session_id = existing.get("backend_session_id") if existing else None

    try:
        startup_errors: list[str] = []
        for provider in _orchestrator_provider_candidates(preferred_provider):
            model = (
                existing.get("model")
                if existing and provider == persisted_provider
                else _orchestrator_model_for_provider(provider)
            )
            try:
                if provider != persisted_provider:
                    await update_session_provider(thread_id, provider, model)
                runner = await _start_orchestrator_runner(
                    provider=provider,
                    thread_id=thread_id,
                    chat_id=chat_id,
                    bot=bot,
                    session_manager=session_manager,
                    permission_manager=permission_manager,
                    question_manager=question_manager,
                    worker_registry=worker_registry,
                    model=model,
                    session_id=session_id if provider == persisted_provider else None,
                    backend_session_id=backend_session_id if provider == persisted_provider else None,
                    orchestrator_mcp_url=orchestrator_mcp_url,
                )
                _attach_orchestrator_fallback(
                    runner=runner,
                    thread_id=thread_id,
                    current_provider=provider,
                    bot=bot,
                    chat_id=chat_id,
                    session_manager=session_manager,
                    permission_manager=permission_manager,
                    question_manager=question_manager,
                    worker_registry=worker_registry,
                    orchestrator_mcp_url=orchestrator_mcp_url,
                )
                if orch:
                    await send_html_message(
                        bot,
                        chat_id=chat_id,
                        message_thread_id=thread_id,
                        text=(
                            "🎯 Orchestrator restarted and ready.\n"
                            f"Provider: {html_code(provider)}"
                        ),
                    )
                logger.info(
                    "Orchestrator session started in thread %d with provider %s",
                    thread_id,
                    provider,
                )
                return thread_id
            except Exception as e:
                logger.error("Failed to start orchestrator on %s: %s", provider, e)
                startup_errors.append(f"{provider}: {e}")

        await bot.send_message(
            chat_id=chat_id,
            message_thread_id=thread_id,
            text=(
                "❌ Failed to start orchestrator on all available providers.\n"
                + "\n".join(startup_errors[-2:])
            ),
        )
        return None
    except Exception as e:
        logger.error("Failed to start orchestrator session: %s", e)
        return None
