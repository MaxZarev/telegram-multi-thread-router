"""Embedded MCP server used by the Codex orchestrator session."""

from __future__ import annotations

import asyncio
import contextlib
import html
import logging
import socket

import uvicorn
from aiogram import Bot
from aiogram.methods import CreateForumTopic
from mcp.server.fastmcp import FastMCP

from src.config import settings
from src.bot.output import html_bold, html_code, send_html_message
from src.db.queries import insert_session, insert_topic, update_auto_mode, update_session_state
from src.sessions.orchestrator import (
    _notify_orchestrator,
    _orchestrator_auto_mode_text,
    _orchestrator_session_created_text,
    _orchestrator_session_stopped_text,
)
from src.sessions.backend import (
    get_default_session_provider,
    get_orchestrator_server_guidance,
    is_supported_provider,
    load_private_infra_context,
    normalize_provider,
    normalize_server_name,
    resolve_workdir_for_server,
    validate_workdir_for_server,
)
from src.sessions.manager import SessionManager
from src.sessions.permissions import PermissionManager
from src.sessions.remote import RemoteSession

logger = logging.getLogger(__name__)


def _default_model_for_provider(provider: str) -> str | None:
    """Return the persisted default model for a provider."""
    if provider == "codex":
        return None
    return "opus"


class LocalOrchestratorMcpServer:
    """Lifecycle wrapper for a local FastMCP HTTP/SSE server."""

    def __init__(
        self,
        bot: Bot,
        chat_id: int,
        orchestrator_thread_id: int | None,
        session_manager: SessionManager,
        permission_manager: PermissionManager,
        worker_registry,
    ) -> None:
        self._bot = bot
        self._chat_id = chat_id
        self._orchestrator_thread_id = orchestrator_thread_id
        self._session_manager = session_manager
        self._permission_manager = permission_manager
        self._worker_registry = worker_registry
        private_context = load_private_infra_context()
        self._fastmcp = FastMCP(
            name="orchestrator",
            instructions=(
                "Create and manage Telegram provider sessions.\n\n"
                "Critical path rules:\n"
                "- Never pass a macOS /Users/... path to a remote server session.\n"
                "- If the user names a repo for a remote server, resolve the remote path first.\n\n"
                f"{get_orchestrator_server_guidance()}"
                f"{f'{chr(10)}{chr(10)}Private infrastructure context:{chr(10)}{private_context}' if private_context else ''}"
            ),
            host="127.0.0.1",
            port=0,
            log_level="WARNING",
            streamable_http_path="/mcp",
        )
        self._server: uvicorn.Server | None = None
        self._task: asyncio.Task | None = None
        self.url: str | None = None
        self._register_tools()

    def _register_tools(self) -> None:
        @self._fastmcp.tool(
            name="create_session",
            description=(
                "Create a new session in a new Telegram thread. "
                f"Provider defaults to '{get_default_session_provider()}' and may be 'codex' when enabled."
            ),
        )
        async def create_session(
            name: str,
            workdir: str,
            server: str = "local",
            provider: str | None = None,
            model: str | None = None,
        ) -> str:
            raw_provider = provider or get_default_session_provider()
            if not is_supported_provider(raw_provider):
                return f"Error: Unsupported provider '{raw_provider}'"

            normalized_provider = normalize_provider(raw_provider)
            if normalized_provider == "codex" and not settings.enable_codex:
                return "Error: Codex sessions are disabled by config"

            server = normalize_server_name(server)
            workdir = resolve_workdir_for_server(server, workdir)
            validation_error = validate_workdir_for_server(server, workdir)
            if validation_error:
                return f"Error: {validation_error}"

            if server != "local" and not self._worker_registry.is_connected(server):
                return f"Error: Server '{server}' not connected"

            if model in (None, "", "default"):
                model = _default_model_for_provider(normalized_provider)
            elif normalized_provider == "codex" and model == "opus":
                model = None

            topic = await self._bot(CreateForumTopic(chat_id=self._chat_id, name=name))
            thread_id = topic.message_thread_id

            await insert_topic(thread_id, name)
            await insert_session(
                thread_id,
                workdir,
                model=model,
                server=server,
                provider=normalized_provider,
            )

            if server != "local":
                await self._session_manager.create_remote(
                    thread_id=thread_id,
                    workdir=workdir,
                    worker_id=server,
                    worker_registry=self._worker_registry,
                    model=model,
                    provider=normalized_provider,
                )
            else:
                await self._session_manager.create(
                    thread_id=thread_id,
                    workdir=workdir,
                    bot=self._bot,
                    chat_id=self._chat_id,
                    permission_manager=self._permission_manager,
                    model=model,
                    provider=normalized_provider,
                )

            await send_html_message(
                self._bot,
                chat_id=self._chat_id,
                message_thread_id=thread_id,
                text=(
                    f"Session {html_bold(name)} started\n"
                    f"Provider: {html_code(normalized_provider)}\n"
                    f"Model: {html_code(model or 'default')}\n"
                    f"Thread: {html_code(thread_id)}\n"
                    f"Server: {html.escape(server)}\n"
                    f"Workdir: {html_code(workdir)}"
                ),
            )
            await _notify_orchestrator(
                self._bot,
                chat_id=self._chat_id,
                orchestrator_thread_id=self._orchestrator_thread_id,
                text=_orchestrator_session_created_text(
                    name=name,
                    thread_id=thread_id,
                    provider=normalized_provider,
                    model=model,
                    server=server,
                    workdir=workdir,
                ),
            )
            return (
                f"Session '{name}' created. Thread ID: {thread_id}, "
                f"provider: {normalized_provider}, model: {model or 'default'}, server: {server}"
            )

        @self._fastmcp.tool(name="list_sessions", description="List all active sessions.")
        async def list_sessions() -> str:
            sessions = self._session_manager.list_all()
            if not sessions:
                return "No active sessions."

            lines = []
            for thread_id, runner in sessions:
                server = runner.worker_id if isinstance(runner, RemoteSession) else "local"
                provider = getattr(runner, "provider", get_default_session_provider())
                lines.append(
                    f"- Thread {thread_id}: {runner.workdir} [{runner.state.name}] "
                    f"provider={provider} on {server}"
                )
            return "\n".join(lines)

        @self._fastmcp.tool(name="stop_session", description="Stop a session by Telegram thread ID.")
        async def stop_session(thread_id: int) -> str:
            runner = self._session_manager.get(thread_id)
            if not runner:
                return f"No session found for thread {thread_id}"

            await self._session_manager.stop(thread_id)
            await update_session_state(thread_id, "stopped")
            await _notify_orchestrator(
                self._bot,
                chat_id=self._chat_id,
                orchestrator_thread_id=self._orchestrator_thread_id,
                text=_orchestrator_session_stopped_text(thread_id),
            )
            return f"Session {thread_id} stopped."

        @self._fastmcp.tool(
            name="auto_mode",
            description="Toggle auto-mode for a session by thread ID.",
        )
        async def auto_mode(thread_id: int, enable: bool) -> str:
            runner = self._session_manager.get(thread_id)
            if not runner:
                return f"No session found for thread {thread_id}"

            runner.auto_mode = enable
            await update_auto_mode(thread_id, enable)
            status = "enabled" if enable else "disabled"
            with contextlib.suppress(Exception):
                await self._bot.send_message(
                    chat_id=self._chat_id,
                    message_thread_id=thread_id,
                    text=f"🤖 Auto-mode {status}",
                )
            await _notify_orchestrator(
                self._bot,
                chat_id=self._chat_id,
                orchestrator_thread_id=self._orchestrator_thread_id,
                text=_orchestrator_auto_mode_text(thread_id, enable),
            )
            return f"Auto-mode {status} for thread {thread_id}"

    async def start(self) -> str:
        """Start the embedded SSE server and return its URL."""
        if self.url is not None:
            return self.url

        port = self._reserve_port()
        app = self._fastmcp.streamable_http_app()
        config = uvicorn.Config(
            app,
            host="127.0.0.1",
            port=port,
            log_level="warning",
            access_log=False,
        )
        self._server = uvicorn.Server(config)
        self._task = asyncio.create_task(self._server.serve())
        await self._wait_until_ready(port)
        self.url = f"http://127.0.0.1:{port}/mcp"
        return self.url

    async def stop(self) -> None:
        """Stop the embedded SSE server."""
        if self._server is not None:
            self._server.should_exit = True
        if self._task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._server = None
        self._task = None
        self.url = None

    def set_orchestrator_thread_id(self, thread_id: int | None) -> None:
        """Update the thread that should receive explicit orchestration acknowledgments."""
        self._orchestrator_thread_id = thread_id

    @staticmethod
    def _reserve_port() -> int:
        """Pick a loopback TCP port for the embedded server."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return sock.getsockname()[1]

    async def _wait_until_ready(self, port: int) -> None:
        """Wait until the HTTP port starts accepting connections."""
        for _ in range(100):
            if self._task and self._task.done():
                exc = self._task.exception()
                if exc is not None:
                    raise exc
                break
            try:
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
            except OSError:
                await asyncio.sleep(0.05)
                continue
            writer.close()
            await writer.wait_closed()
            return
        raise RuntimeError("Timed out starting orchestrator MCP server")
