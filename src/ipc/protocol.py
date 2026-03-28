"""IPC protocol — msgspec Struct message types and framing helpers for TCP communication."""

from __future__ import annotations

import asyncio
import logging
from typing import Union

import msgspec

logger = logging.getLogger(__name__)


# ---- Worker → Bot messages ----

class AuthMsg(msgspec.Struct, tag="auth"):
    """Authentication handshake sent by worker upon connection."""
    token: str
    worker_id: str


class SessionStartedMsg(msgspec.Struct, tag="session_started"):
    """Sent by worker when a Claude session has started."""
    topic_id: int
    session_id: str


class AssistantTextMsg(msgspec.Struct, tag="assistant_text"):
    """Chunk of assistant output text to be sent to the Telegram topic."""
    topic_id: int
    text: str
    is_first: bool = False  # True for first text block in turn (for reply-to)


class PermissionRequestMsg(msgspec.Struct, tag="permission_request"):
    """Worker asking the bot to prompt the owner for tool permission."""
    topic_id: int
    request_id: str
    tool_name: str
    input_data: dict


class QuestionRequestMsg(msgspec.Struct, tag="question_request"):
    """Worker asking the bot to show interactive questions to the owner."""
    topic_id: int
    request_id: str
    questions: list[dict]


class StatusUpdateMsg(msgspec.Struct, tag="status_update"):
    """Tool execution status update with rich data."""
    topic_id: int
    tool_name: str
    tool_calls: int
    input_data: dict | None = None
    elapsed_ms: int = 0


class TurnCompletedMsg(msgspec.Struct, tag="turn_completed"):
    """Worker reports that a turn has finished (ResultMessage received)."""
    topic_id: int
    cost_usd: float | None = None
    duration_ms: int = 0
    tool_count: int = 0
    is_error: bool = False
    session_id: str | None = None
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    model: str | None = None


class UsageUpdateMsg(msgspec.Struct, tag="usage_update"):
    """Token/model data from AssistantMessage during a turn."""
    topic_id: int
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_creation_tokens: int = 0
    model: str | None = None


class RateLimitMsg(msgspec.Struct, tag="rate_limit"):
    """Forward rate limit events from worker."""
    topic_id: int
    status: str  # "rejected" | "allowed_warning"
    resets_at: int | None = None
    utilization: float | None = None


class SystemNotificationMsg(msgspec.Struct, tag="system_notification"):
    """Forward SystemMessage from worker (compact, model_change, etc.)."""
    topic_id: int
    subtype: str
    text: str  # Pre-formatted notification text


class SessionEndedMsg(msgspec.Struct, tag="session_ended"):
    """Worker reports that a session has ended, optionally with an error."""
    topic_id: int
    error: str | None = None


class McpSendMessageMsg(msgspec.Struct, tag="mcp_send_message"):
    """MCP tool: send a plain text message to the topic."""
    topic_id: int
    text: str


class McpReactMsg(msgspec.Struct, tag="mcp_react"):
    """MCP tool: add an emoji reaction to a message."""
    topic_id: int
    message_id: int
    emoji: str


class McpEditMessageMsg(msgspec.Struct, tag="mcp_edit_message"):
    """MCP tool: edit the text of an existing message."""
    topic_id: int
    message_id: int
    text: str


class McpSendFileMsg(msgspec.Struct, tag="mcp_send_file"):
    """MCP tool: send a file from the worker's filesystem to the topic."""
    topic_id: int
    file_path: str
    file_name: str | None = None
    file_bytes: bytes | None = None
    caption: str | None = None


# ---- Bot → Worker messages ----

class AuthOkMsg(msgspec.Struct, tag="auth_ok"):
    """Authentication accepted."""
    worker_id: str


class AuthFailMsg(msgspec.Struct, tag="auth_fail"):
    """Authentication rejected."""
    reason: str


class StartSessionMsg(msgspec.Struct, tag="start_session"):
    """Bot instructs worker to start a new Claude session."""
    topic_id: int
    cwd: str
    session_id: str | None = None
    backend_session_id: str | None = None
    model: str | None = None
    provider: str = "claude"
    provider_options: dict | None = None


class StopSessionMsg(msgspec.Struct, tag="stop_session"):
    """Bot instructs worker to stop a running session."""
    topic_id: int


class InterruptMsg(msgspec.Struct, tag="interrupt"):
    """Bot instructs worker to interrupt current turn (like Escape)."""
    topic_id: int


class UserMessageMsg(msgspec.Struct, tag="user_message"):
    """User text message forwarded from bot to worker."""
    topic_id: int
    text: str
    reply_to_message_id: int | None = None


class UserFileMsg(msgspec.Struct, tag="user_file"):
    """User file/photo payload forwarded from bot to worker."""
    topic_id: int
    file_name: str
    file_bytes: bytes
    caption: str | None = None
    media_type: str | None = None
    reply_to_message_id: int | None = None
    is_image: bool = False


class PermissionResponseMsg(msgspec.Struct, tag="permission_response"):
    """Bot forwards the owner's permission decision back to the worker."""
    request_id: str
    action: str  # "allow" | "always" | "deny"


class QuestionResponseMsg(msgspec.Struct, tag="question_response"):
    """Bot forwards question answers back to the worker."""
    request_id: str
    answers: dict[str, str]


class SlashCommandMsg(msgspec.Struct, tag="slash_command"):
    """Bot forwards a slash command to the worker."""
    topic_id: int
    command: str


# ---- Bidirectional messages ----

class PingMsg(msgspec.Struct, tag="ping"):
    """Heartbeat ping — sent by worker, answered by bot with PongMsg."""
    pass


class PongMsg(msgspec.Struct, tag="pong"):
    """Heartbeat pong — bot responds to worker's PingMsg."""
    pass


# ---- Union types for discriminated decoding ----

WorkerToBot = Union[
    AuthMsg,
    SessionStartedMsg,
    AssistantTextMsg,
    PermissionRequestMsg,
    QuestionRequestMsg,
    StatusUpdateMsg,
    TurnCompletedMsg,
    UsageUpdateMsg,
    RateLimitMsg,
    SystemNotificationMsg,
    SessionEndedMsg,
    McpSendMessageMsg,
    McpReactMsg,
    McpEditMessageMsg,
    McpSendFileMsg,
    PingMsg,
]

BotToWorker = Union[
    AuthOkMsg,
    AuthFailMsg,
    StartSessionMsg,
    StopSessionMsg,
    InterruptMsg,
    UserMessageMsg,
    UserFileMsg,
    PermissionResponseMsg,
    QuestionResponseMsg,
    SlashCommandMsg,
    PongMsg,
]

# Module-level encoder/decoders — reuse for performance
_enc = msgspec.msgpack.Encoder()
_w2b_dec = msgspec.msgpack.Decoder(WorkerToBot)
_b2w_dec = msgspec.msgpack.Decoder(BotToWorker)


# ---- Framing helpers ----

async def send_msg(writer: asyncio.StreamWriter, msg) -> None:
    """Encode msg and write it with a 4-byte big-endian length prefix."""
    payload = _enc.encode(msg)
    prefix = len(payload).to_bytes(4, "big")
    writer.write(prefix + payload)
    await writer.drain()


async def recv_w2b(reader: asyncio.StreamReader) -> WorkerToBot | None:
    """Read one WorkerToBot message. Returns None on EOF/disconnect.

    Decode errors from newer workers are logged and skipped.
    """
    while True:
        try:
            prefix = await reader.readexactly(4)
            n = int.from_bytes(prefix, "big")
            payload = await reader.readexactly(n)
            return _w2b_dec.decode(payload)
        except asyncio.IncompleteReadError:
            return None
        except (msgspec.DecodeError, msgspec.ValidationError) as e:
            logger.warning("recv_w2b: skipping undecodable message (%s)", e)
            continue


async def recv_b2w(reader: asyncio.StreamReader) -> BotToWorker | None:
    """Read one BotToWorker message. Returns None on EOF/disconnect.

    Decode errors (e.g. unknown message tags from a newer bot) are logged
    and skipped — the caller sees the next valid message instead of crashing.
    """
    while True:
        try:
            prefix = await reader.readexactly(4)
            n = int.from_bytes(prefix, "big")
            payload = await reader.readexactly(n)
            return _b2w_dec.decode(payload)
        except asyncio.IncompleteReadError:
            return None
        except (msgspec.DecodeError, msgspec.ValidationError) as e:
            logger.warning("recv_b2w: skipping undecodable message (%s) — worker may need updating", e)
            continue
