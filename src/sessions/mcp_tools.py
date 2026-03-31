"""MCP tools factory: creates an in-process MCP server with 4 Telegram output tools."""

import logging
from pathlib import Path

from aiogram import Bot
from aiogram.types import FSInputFile, ReactionTypeEmoji
from claude_agent_sdk import create_sdk_mcp_server, tool

logger = logging.getLogger(__name__)


def create_telegram_mcp_server(bot: Bot, chat_id: int, thread_id: int):
    """Create an in-process MCP server with 4 Telegram output tools.

    All tools are closures bound to the provided bot, chat_id, and thread_id.
    The returned McpSdkServerConfig can be passed to ClaudeAgentOptions.mcp_servers.

    Args:
        bot: The aiogram Bot instance to use for sending messages.
        chat_id: The Telegram chat ID.
        thread_id: The Telegram thread (message_thread_id) to send to.

    Returns:
        McpSdkServerConfig: An in-process MCP server with reply, send_file,
        react, and edit_message tools.
    """

    @tool("reply", "Send a text message to the user in the current Telegram thread", {"text": str})
    async def reply(args: dict) -> dict:
        text = args["text"]
        try:
            await bot.send_message(
                chat_id=chat_id,
                message_thread_id=thread_id,
                text=text,
            )
            return {"content": [{"type": "text", "text": "Message sent"}]}
        except Exception as e:
            logger.error("reply tool error: %s", e)
            return {"content": [{"type": "text", "text": f"Error: {e}"}]}

    @tool(
        "send_file",
        "Send a file to the user in the current Telegram thread. Path must be absolute or relative to working directory.",
        {"path": str},
    )
    async def send_file(args: dict) -> dict:
        path = args["path"]
        try:
            file_path = Path(path)
            if not file_path.exists():
                return {"content": [{"type": "text", "text": f"Error: File not found: {path}"}]}
            if file_path.stat().st_size > 50 * 1024 * 1024:
                return {"content": [{"type": "text", "text": "File exceeds 50MB Telegram limit"}]}
            await bot.send_document(
                chat_id=chat_id,
                message_thread_id=thread_id,
                document=FSInputFile(path),
            )
            return {"content": [{"type": "text", "text": f"File sent: {file_path.name}"}]}
        except Exception as e:
            logger.error("send_file tool error: %s", e)
            return {"content": [{"type": "text", "text": f"Error: {e}"}]}

    @tool(
        "react",
        "Add an emoji reaction to a specific message in the thread",
        {"emoji": str, "message_id": int},
    )
    async def react(args: dict) -> dict:
        emoji = args["emoji"]
        message_id = args["message_id"]
        try:
            await bot.set_message_reaction(
                chat_id=chat_id,
                message_id=message_id,
                reaction=[ReactionTypeEmoji(emoji=emoji)],
            )
            return {"content": [{"type": "text", "text": "Reaction added"}]}
        except Exception as e:
            logger.error("react tool error: %s", e)
            return {"content": [{"type": "text", "text": f"Error: {e}"}]}

    @tool(
        "edit_message",
        "Edit a previously sent message in the thread",
        {"text": str, "message_id": int},
    )
    async def edit_message(args: dict) -> dict:
        text = args["text"]
        message_id = args["message_id"]
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
            )
            return {"content": [{"type": "text", "text": "Message edited"}]}
        except Exception as e:
            logger.error("edit_message tool error: %s", e)
            return {"content": [{"type": "text", "text": f"Error: {e}"}]}

    return create_sdk_mcp_server(
        "telegram",
        tools=[reply, send_file, react, edit_message],
    )
