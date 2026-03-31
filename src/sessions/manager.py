"""SessionManager — maps thread_id to SessionRunner or RemoteSession instances."""

import asyncio
import logging
from typing import Optional

from aiogram import Bot

from src.sessions.backend import (
    SessionBackend,
    SessionProvider,
    load_repo_local_instructions,
    normalize_provider,
)
from src.sessions.codex_runner import CodexRunner
from src.sessions.permissions import PermissionManager
from src.sessions.questions import QuestionManager
from src.sessions.remote import RemoteSession
from src.sessions.runner import SessionRunner

logger = logging.getLogger(__name__)


class SessionManager:
    """Manages the lifecycle of all active SessionRunner and RemoteSession instances."""

    def __init__(self, question_manager: QuestionManager | None = None) -> None:
        self._sessions: dict[int, SessionBackend] = {}
        self._lock = asyncio.Lock()
        self._question_manager = question_manager

    async def create(
        self,
        thread_id: int,
        workdir: str,
        bot: Bot,
        chat_id: int,
        permission_manager: PermissionManager,
        session_id: str | None = None,
        backend_session_id: str | None = None,
        model: str | None = None,
        provider: str | None = None,
    ) -> SessionBackend:
        """Create and start a new SessionRunner for the given thread_id.

        Raises ValueError if a session for that thread already exists.
        """
        normalized_provider = normalize_provider(provider)
        async with self._lock:
            if thread_id in self._sessions:
                raise ValueError(f"Session for topic {thread_id} already exists")
            runner: SessionBackend
            if normalized_provider == "codex":
                runner = CodexRunner(
                    thread_id=thread_id,
                    workdir=workdir,
                    bot=bot,
                    chat_id=chat_id,
                    permission_manager=permission_manager,
                    question_manager=self._question_manager,
                    backend_session_id=backend_session_id,
                    model=model,
                    base_instructions=load_repo_local_instructions(workdir),
                )
            else:
                runner = SessionRunner(
                    thread_id=thread_id,
                    workdir=workdir,
                    bot=bot,
                    chat_id=chat_id,
                    permission_manager=permission_manager,
                    question_manager=self._question_manager,
                    session_id=session_id or backend_session_id,
                    model=model,
                )
            self._sessions[thread_id] = runner
            await runner.start()
            return runner

    async def create_remote(
        self,
        thread_id: int,
        workdir: str,
        worker_id: str,
        worker_registry,
        session_id: str | None = None,
        backend_session_id: str | None = None,
        model: str | None = None,
        provider: str | None = None,
        provider_options: dict | None = None,
    ) -> RemoteSession:
        """Create a RemoteSession proxy and send StartSessionMsg to the worker.

        Raises ValueError if a session for that thread already exists.
        """
        async with self._lock:
            if thread_id in self._sessions:
                raise ValueError(f"Session for topic {thread_id} already exists")
            session = RemoteSession(
                thread_id=thread_id,
                workdir=workdir,
                worker_id=worker_id,
                worker_registry=worker_registry,
                session_id=session_id,
                backend_session_id=backend_session_id,
                provider=provider,
                model=model,
                provider_options=provider_options,
            )
            self._sessions[thread_id] = session
            await session.start()
            return session

    def get(self, thread_id: int) -> Optional[SessionBackend]:
        """Return the runner/session for thread_id, or None if not found."""
        return self._sessions.get(thread_id)

    async def stop(self, thread_id: int) -> None:
        """Stop and remove the runner for thread_id. No-op if not found."""
        async with self._lock:
            runner = self._sessions.pop(thread_id, None)
            if runner:
                await runner.stop()

    def list_all(self) -> list[tuple[int, SessionBackend]]:
        """Return all (thread_id, runner) pairs."""
        return list(self._sessions.items())

    def get_server(self, thread_id: int) -> str:
        """Return the server name for a session: worker_id for remote, 'local' for local."""
        runner = self._sessions.get(thread_id)
        if runner is None:
            return "local"
        return runner.worker_id if isinstance(runner, RemoteSession) else "local"

    async def resume_all(self, bot: Bot, chat_id: int, permission_manager: PermissionManager) -> int:
        """Resume all local sessions that were active when bot last stopped.

        Remote sessions are skipped — they re-register when the worker reconnects.
        Orchestrator is skipped — ensure_orchestrator() handles it with proper MCP tools.
        Returns number of successfully resumed sessions.
        """
        from src.db.queries import get_resumable_sessions, get_orchestrator_topic, update_session_state

        rows = await get_resumable_sessions()
        resumed = 0

        # Get orchestrator thread_id to skip it (ensure_orchestrator handles it)
        orch = await get_orchestrator_topic()
        orch_thread_id = orch["thread_id"] if orch else None

        for row in rows:
            thread_id = row["thread_id"]
            session_id = row["session_id"]
            backend_session_id = row.get("backend_session_id")
            workdir = row["workdir"]
            model = row.get("model")
            server = row.get("server", "local")
            provider = normalize_provider(row.get("provider"))

            # Skip remote sessions — worker reconnect handles re-registration
            if server != "local":
                logger.info(
                    "Skipping remote session topic %d (server=%s provider=%s) on resume",
                    thread_id,
                    server,
                    provider,
                )
                continue

            # Skip orchestrator — ensure_orchestrator() handles it with proper MCP tools
            if thread_id == orch_thread_id:
                logger.info("Skipping orchestrator topic %d on resume (handled separately)", thread_id)
                continue

            try:
                runner = await self.create(
                    thread_id=thread_id,
                    workdir=workdir,
                    bot=bot,
                    chat_id=chat_id,
                    permission_manager=permission_manager,
                    session_id=session_id,
                    backend_session_id=backend_session_id,
                    model=model,
                    provider=provider,
                )
                # Restore auto_mode and goal_text from DB
                if row.get("auto_mode"):
                    runner.auto_mode = True
                if row.get("goal_text"):
                    runner.goal_text = row["goal_text"]
                await bot.send_message(
                    chat_id=chat_id,
                    message_thread_id=thread_id,
                    text="Session resumed after bot restart.",
                )
                resumed += 1
                logger.info("Resumed session %s in topic %d", session_id, thread_id)
            except Exception as e:
                logger.error(
                    "Failed to resume session %s in topic %d: %s", session_id, thread_id, e
                )
                # Mark as stopped in DB so it doesn't retry on next restart
                try:
                    await update_session_state(thread_id, "stopped")
                except Exception:
                    pass

        return resumed
