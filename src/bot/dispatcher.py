"""Dispatcher factory — assembles middleware, routers, and lifecycle hooks."""

import asyncio
import logging

from aiogram import Bot, Dispatcher

from src.bot.middlewares import OwnerAuthMiddleware
from src.bot.routers.general import general_router
from src.bot.routers.session import session_router
from src.config import settings
from src.db.schema import init_db
from src.ipc.server import WorkerRegistry, start_ipc_server
from src.sessions.manager import SessionManager
from src.sessions.permissions import PermissionManager
from src.sessions.questions import QuestionManager

logger = logging.getLogger(__name__)


def build_dispatcher() -> Dispatcher:
    """Create and configure the Dispatcher with all routers and middleware."""
    dp = Dispatcher()

    # Owner auth — outer middleware fires before any router filters
    dp.message.outer_middleware(
        OwnerAuthMiddleware(owner_id=settings.owner_user_id)
    )

    # General topic handles management commands (thread_id=1 or None) — fallback
    dp.include_router(general_router)

    # Session topics handle Claude session messages (all other thread_ids)
    dp.include_router(session_router)

    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    return dp


async def on_startup(bot: Bot, dispatcher: Dispatcher) -> None:
    """Called when polling starts. Initialize database and SessionManager."""
    await init_db()

    # Auto-detect chat_id: load from DB if not in env
    if settings.chat_id is None:
        from src.db.queries import get_bot_setting
        saved_chat_id = await get_bot_setting("chat_id")
        if saved_chat_id:
            settings.chat_id = int(saved_chat_id)
            logger.info("Loaded chat_id from DB: %d", settings.chat_id)

    permission_manager = PermissionManager()
    await permission_manager.load_from_db()
    dispatcher["permission_manager"] = permission_manager
    question_manager = QuestionManager()
    dispatcher["question_manager"] = question_manager
    manager = SessionManager(question_manager=question_manager)
    dispatcher["session_manager"] = manager

    # Start TCP IPC server for remote worker connections (non-fatal if port busy)
    worker_registry = WorkerRegistry()
    dispatcher["worker_registry"] = worker_registry
    try:
        ipc_server = await start_ipc_server(
            settings.ipc_host,
            settings.ipc_port,
            settings.auth_token,
            bot,
            manager,
            permission_manager,
            worker_registry,
            question_manager=question_manager,
        )
        dispatcher["ipc_server"] = ipc_server
    except OSError as e:
        logger.warning("IPC server failed to start (port %d busy): %s", settings.ipc_port, e)
        dispatcher["ipc_server"] = None

    # Only proceed with session resume and orchestrator if chat_id is known
    if settings.chat_id:
        # Resume sessions that were active before bot stopped
        resumed = await manager.resume_all(bot, settings.chat_id, permission_manager)
        if resumed:
            logger.info("Resumed %d session(s) from database", resumed)

        # Start health monitoring background task
        from src.sessions.health import health_check_loop
        health_task = asyncio.create_task(
            health_check_loop(manager, bot, settings.chat_id)
        )
        dispatcher["health_task"] = health_task

        orchestrator_mcp_server = None
        orchestrator_mcp_url = None
        if settings.enable_codex:
            from src.sessions.orchestrator_mcp import LocalOrchestratorMcpServer

            orchestrator_mcp_server = LocalOrchestratorMcpServer(
                bot,
                settings.chat_id,
                None,
                manager,
                permission_manager,
                worker_registry,
            )
            orchestrator_mcp_url = await orchestrator_mcp_server.start()
            dispatcher["orchestrator_mcp_server"] = orchestrator_mcp_server

        # Start orchestrator session
        from src.sessions.orchestrator import ensure_orchestrator
        orch_thread = await ensure_orchestrator(
            bot,
            settings.chat_id,
            manager,
            permission_manager,
            question_manager,
            worker_registry,
            orchestrator_mcp_url=orchestrator_mcp_url,
        )
        if orch_thread:
            if orchestrator_mcp_server is not None:
                orchestrator_mcp_server.set_orchestrator_thread_id(orch_thread)
            dispatcher["orchestrator_thread_id"] = orch_thread
            logger.info("Orchestrator running in thread %d", orch_thread)
    else:
        logger.info("No chat_id configured — waiting for first message from owner")

    logger.info("Bot startup complete — SessionManager initialized")


async def on_shutdown(dispatcher: Dispatcher) -> None:
    """Called when polling stops. Graceful shutdown preserves session state for resume."""
    # Cancel health check
    health_task: asyncio.Task | None = dispatcher.get("health_task")
    if health_task:
        health_task.cancel()
        try:
            await health_task
        except asyncio.CancelledError:
            pass

    orchestrator_mcp_server = dispatcher.get("orchestrator_mcp_server")
    if orchestrator_mcp_server:
        await orchestrator_mcp_server.stop()

    # Close IPC server
    ipc_server: asyncio.Server | None = dispatcher.get("ipc_server")
    if ipc_server:
        ipc_server.close()
        await ipc_server.wait_closed()

    # Disconnect Claude SDK clients but keep DB state as idle (NOT stopped)
    # so resume_all picks them up on next startup
    manager: SessionManager | None = dispatcher.get("session_manager")
    if manager:
        from src.db.queries import update_session_state
        for thread_id, runner in manager.list_all():
            try:
                # Ensure session_id is saved, then disconnect SDK without marking stopped
                await runner.stop()
                # Override: mark as idle so resume_all picks it up
                await update_session_state(thread_id, "idle")
            except Exception as e:
                logger.error("Error stopping session %d: %s", thread_id, e)

    logger.info("Bot shutting down — sessions preserved for resume")
