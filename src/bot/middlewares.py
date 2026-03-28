"""Middleware for owner-only message filtering with auto chat_id detection."""

import asyncio
import logging
from typing import Any, Awaitable, Callable

from aiogram import BaseMiddleware
from aiogram.types import Message

logger = logging.getLogger(__name__)


class OwnerAuthMiddleware(BaseMiddleware):
    """Drop all messages not from the owner. Auto-detect chat_id on first message.

    Registered as outer middleware on dp.message so it fires before
    any router filters. Silent drop — no reply to non-owner users.

    If chat_id is not set (None), the first message from the owner
    will set it and persist to DB.
    """

    def __init__(self, owner_id: int) -> None:
        self.owner_id = owner_id
        self._setup_done = asyncio.Event()
        self._setup_lock = asyncio.Lock()

    async def __call__(
        self,
        handler: Callable[[Message, dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: dict[str, Any],
    ) -> Any:
        # Guard: from_user can be None for channel posts
        if not event.from_user:
            return None

        # Guard: only process messages from the owner
        if event.from_user.id != self.owner_id:
            return None

        from src.config import settings

        # Auto-detect chat_id from first owner message
        if settings.chat_id is None:
            async with self._setup_lock:
                if settings.chat_id is None:  # double-check
                    settings.chat_id = event.chat.id
                    logger.info("Auto-detected chat_id: %d", event.chat.id)
                    # Persist to DB
                    try:
                        from src.db.queries import set_bot_setting
                        await set_bot_setting("chat_id", str(event.chat.id))
                    except Exception as e:
                        logger.error("Failed to persist chat_id: %s", e)
                    # Signal setup complete
                    self._setup_done.set()
                    # Trigger deferred orchestrator setup
                    await self._deferred_setup(event, data)

        # Guard: only process messages from the detected chat
        if event.chat.id != settings.chat_id:
            return None

        return await handler(event, data)

    async def _deferred_setup(self, event: Message, data: dict[str, Any]) -> None:
        """Create orchestrator after chat_id is detected for the first time."""
        from src.config import settings
        from src.sessions.orchestrator import ensure_orchestrator

        bot = event.bot
        dispatcher = data.get("dispatcher")
        if not dispatcher:
            return

        session_manager = dispatcher.get("session_manager")
        permission_manager = dispatcher.get("permission_manager")
        question_manager = dispatcher.get("question_manager")
        worker_registry = dispatcher.get("worker_registry")

        if not all([session_manager, permission_manager, question_manager, worker_registry]):
            logger.warning("Cannot create orchestrator — dispatcher not fully initialized")
            return

        orchestrator_mcp_url = None
        if settings.enable_codex:
            orchestrator_mcp_server = dispatcher.get("orchestrator_mcp_server")
            if orchestrator_mcp_server is None:
                from src.sessions.orchestrator_mcp import LocalOrchestratorMcpServer

                orchestrator_mcp_server = LocalOrchestratorMcpServer(
                    bot,
                    settings.chat_id,
                    None,
                    session_manager,
                    permission_manager,
                    worker_registry,
                )
                dispatcher["orchestrator_mcp_server"] = orchestrator_mcp_server
            orchestrator_mcp_url = await orchestrator_mcp_server.start()

        orch_thread = await ensure_orchestrator(
            bot,
            settings.chat_id,
            session_manager,
            permission_manager,
            question_manager,
            worker_registry,
            orchestrator_mcp_url=orchestrator_mcp_url,
        )
        if orch_thread:
            if orchestrator_mcp_server is not None:
                orchestrator_mcp_server.set_orchestrator_thread_id(orch_thread)
            dispatcher["orchestrator_thread_id"] = orch_thread
            logger.info("Orchestrator created after chat_id auto-detection: thread %d", orch_thread)
