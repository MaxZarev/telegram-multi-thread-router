"""Embedded MCP server exposing Telegram output tools to a single session."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
from pathlib import Path

import uvicorn
from aiogram.types import FSInputFile, ReactionTypeEmoji
from mcp.server.fastmcp import FastMCP

logger = logging.getLogger(__name__)


class LocalTelegramOutputMcpServer:
    """Lifecycle wrapper for per-session Telegram output tools over streamable HTTP."""

    def __init__(self, bot, chat_id: int, thread_id: int) -> None:
        self._bot = bot
        self._chat_id = chat_id
        self._thread_id = thread_id
        self._fastmcp = FastMCP(
            name=f"telegram-{thread_id}",
            instructions="Telegram output tools scoped to the current thread.",
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
            name="reply",
            description="Send a text reply to the current Telegram thread.",
        )
        async def reply(text: str) -> str:
            await self._bot.send_message(
                chat_id=self._chat_id,
                message_thread_id=self._thread_id,
                text=text,
            )
            return "Message sent"

        @self._fastmcp.tool(
            name="send_file",
            description=(
                "Send a file to the current Telegram thread. "
                "Use an absolute path or a path relative to the current working directory."
            ),
        )
        async def send_file(path: str, caption: str | None = None) -> str:
            file_path = Path(path)
            if not file_path.exists():
                return f"Error: File not found: {path}"
            if file_path.stat().st_size > 50 * 1024 * 1024:
                return "Error: File exceeds Telegram 50MB limit"

            await self._bot.send_document(
                chat_id=self._chat_id,
                message_thread_id=self._thread_id,
                document=FSInputFile(str(file_path)),
                caption=caption,
            )
            return f"File sent: {file_path.name}"

        @self._fastmcp.tool(
            name="react",
            description="React to a Telegram message in the current chat.",
        )
        async def react(message_id: int, emoji: str) -> str:
            await self._bot.set_message_reaction(
                chat_id=self._chat_id,
                message_id=message_id,
                reaction=[ReactionTypeEmoji(emoji=emoji)],
            )
            return "Reaction added"

        @self._fastmcp.tool(
            name="edit_message",
            description="Edit a Telegram message previously sent in the current chat.",
        )
        async def edit_message(message_id: int, text: str) -> str:
            await self._bot.edit_message_text(
                chat_id=self._chat_id,
                message_id=message_id,
                text=text,
            )
            return "Message edited"

    async def start(self) -> str:
        """Start the embedded server and return its local URL."""
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
        logger.info(
            "Started telegram MCP server for thread %d on %s",
            self._thread_id,
            self.url,
        )
        return self.url

    async def stop(self) -> None:
        """Stop the embedded server."""
        if self._server is not None:
            self._server.should_exit = True
        if self._task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._server = None
        self._task = None
        self.url = None

    @staticmethod
    def _reserve_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return sock.getsockname()[1]

    async def _wait_until_ready(self, port: int) -> None:
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
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            return
        raise RuntimeError("Telegram MCP server did not start in time")
