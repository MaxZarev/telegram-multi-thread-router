"""IPC Worker Client — connects to bot server and runs Claude sessions on remote machines.

Usage:
    python -m src.ipc.client --host <bot-server-ip> --port 9600 --token <auth-token> --worker-id <name>
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import signal
import sys
from dataclasses import dataclass, field
from pathlib import Path

from src.ipc.protocol import (
    AuthMsg,
    AuthOkMsg,
    AuthFailMsg,
    AssistantTextMsg,
    InterruptMsg,
    PermissionRequestMsg,
    PermissionResponseMsg,
    PingMsg,
    PongMsg,
    RateLimitMsg,
    SessionEndedMsg,
    SessionStartedMsg,
    StartSessionMsg,
    StatusUpdateMsg,
    StopSessionMsg,
    SystemNotificationMsg,
    TurnCompletedMsg,
    UsageUpdateMsg,
    UserFileMsg,
    UserMessageMsg,
    SlashCommandMsg,
    QuestionResponseMsg,
    send_msg,
    recv_b2w,
)

logger = logging.getLogger(__name__)

try:
    from claude_agent_sdk import (
        ClaudeSDKClient,
        ClaudeAgentOptions,
        AssistantMessage,
        ResultMessage,
        SystemMessage,
        TextBlock,
        ToolUseBlock,
        PermissionResultAllow,
        PermissionResultDeny,
        PermissionUpdate,
        HookMatcher,
    )
    from claude_agent_sdk.types import (
        PermissionRuleValue,
        RateLimitEvent,
        TaskStartedMessage,
        TaskProgressMessage,
        TaskNotificationMessage,
    )
    HAS_SDK = True
except ImportError:
    HAS_SDK = False


async def _dummy_pretool_hook(input_data, tool_use_id, context):
    return {"continue_": True}


def _build_system_prompt(workdir: str) -> str:
    claude_md = Path(workdir) / "CLAUDE.md"
    base = ""
    try:
        base = claude_md.read_text()
    except (FileNotFoundError, PermissionError):
        pass
    return f"{base}\n\nYou are helping in directory {workdir}".strip()


@dataclass
class _QueueItem:
    """Queue item for WorkerSession — text or multimodal content."""
    text: str | None = None
    content_blocks: list | None = None


class WorkerSession:
    """Manages one Claude session on the worker side, proxying all messages to bot."""

    def __init__(
        self,
        topic_id: int,
        cwd: str,
        writer: asyncio.StreamWriter,
        session_id: str | None = None,
        model: str | None = None,
    ) -> None:
        self.topic_id = topic_id
        self.cwd = cwd
        self.session_id = session_id
        self.model = model
        self._writer = writer
        self._message_queue: asyncio.Queue[_QueueItem | None] = asyncio.Queue()
        self._task: asyncio.Task | None = None
        self._client: ClaudeSDKClient | None = None
        self._pending_permissions: dict[str, asyncio.Future] = {}
        self._allowed_tools: set[str] = set()

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def _send(self, msg) -> None:
        """Send msg to bot, swallowing broken pipe errors."""
        try:
            await send_msg(self._writer, msg)
        except Exception as e:
            logger.warning("Failed to send to bot: %s", e)

    async def _run(self) -> None:
        system_prompt = _build_system_prompt(self.cwd)
        options = ClaudeAgentOptions(
            cwd=self.cwd,
            model=self.model,
            system_prompt=system_prompt,
            can_use_tool=self._can_use_tool,
            hooks={"PreToolUse": [HookMatcher(matcher=None, hooks=[_dummy_pretool_hook])]},
            resume=self.session_id,
            include_partial_messages=True,
        )
        try:
            async with ClaudeSDKClient(options=options) as client:
                self._client = client
                await self._send(SessionStartedMsg(
                    topic_id=self.topic_id,
                    session_id=self.session_id or "",
                ))
                logger.info("Session started for topic %d in %s", self.topic_id, self.cwd)

                # Mid-turn injection task
                inject_task = asyncio.create_task(self._inject_loop(client))

                while True:
                    item = await self._message_queue.get()
                    if item is None:
                        break

                    self._is_running = True
                    await self._send_query(client, item)
                    await self._drain_response(client)
                    self._is_running = False

                inject_task.cancel()
                try:
                    await inject_task
                except asyncio.CancelledError:
                    pass

        except Exception as e:
            logger.error("Session error for topic %d: %s", self.topic_id, e)
            await self._send(SessionEndedMsg(topic_id=self.topic_id, error=str(e)))
        else:
            await self._send(SessionEndedMsg(topic_id=self.topic_id))
        finally:
            self._client = None

    async def _send_query(self, client: ClaudeSDKClient, item: _QueueItem) -> None:
        """Send a query to the SDK — text or multimodal (image + text)."""
        if item.content_blocks:
            message = {
                "type": "user",
                "message": {"role": "user", "content": item.content_blocks},
                "parent_tool_use_id": None,
                "session_id": "default",
            }
            await client._transport.write(json.dumps(message) + "\n")
        else:
            await client.query(item.text)

    async def _inject_loop(self, client: ClaudeSDKClient) -> None:
        """Inject queued messages mid-turn (like local runner)."""
        try:
            while True:
                if not getattr(self, '_is_running', False):
                    await asyncio.sleep(0.1)
                    continue
                try:
                    item = self._message_queue.get_nowait()
                except asyncio.QueueEmpty:
                    await asyncio.sleep(0.1)
                    continue
                if item is None:
                    await self._message_queue.put(None)
                    break
                logger.info("Injecting mid-turn message in topic %d", self.topic_id)
                await self._send_query(client, item)
        except asyncio.CancelledError:
            pass

    async def _can_use_tool(self, tool_name, input_data, context):
        """Permission callback — checks local cache, then asks bot."""
        if tool_name in self._allowed_tools:
            return PermissionResultAllow(updated_input=input_data)

        import uuid
        request_id = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._pending_permissions[request_id] = future

        await self._send(PermissionRequestMsg(
            topic_id=self.topic_id,
            request_id=request_id,
            tool_name=tool_name,
            input_data=input_data,
        ))

        try:
            action = await asyncio.wait_for(future, timeout=300.0)
        except asyncio.TimeoutError:
            self._pending_permissions.pop(request_id, None)
            return PermissionResultDeny(message="Timed out")

        if action == "always":
            self._allowed_tools.add(tool_name)
            return PermissionResultAllow(
                updated_input=input_data,
                updated_permissions=[
                    PermissionUpdate(
                        type="addRules",
                        rules=[PermissionRuleValue(tool_name=tool_name)],
                        behavior="allow",
                    )
                ],
            )
        elif action == "allow":
            return PermissionResultAllow(updated_input=input_data)
        return PermissionResultDeny(message="Denied by user")

    def resolve_permission(self, request_id: str, action: str) -> None:
        future = self._pending_permissions.pop(request_id, None)
        if future and not future.done():
            future.set_result(action)

    async def _drain_response(self, client: ClaudeSDKClient) -> None:
        """Receive all messages from current turn and forward everything to bot."""
        tool_count = 0
        first_text_sent = False
        last_model = None
        last_usage = {}

        async for msg in client.receive_response():
            if isinstance(msg, AssistantMessage):
                # Track model
                if hasattr(msg, "model") and msg.model:
                    last_model = msg.model

                # Send usage update
                usage = getattr(msg, "usage", None)
                if usage:
                    last_usage = usage
                    await self._send(UsageUpdateMsg(
                        topic_id=self.topic_id,
                        input_tokens=usage.get("input_tokens", 0),
                        output_tokens=usage.get("output_tokens", 0),
                        cache_read_tokens=usage.get("cache_read_input_tokens", 0),
                        cache_creation_tokens=usage.get("cache_creation_input_tokens", 0),
                        model=last_model,
                    ))

                for block in msg.content:
                    if isinstance(block, TextBlock) and block.text:
                        is_first = not first_text_sent
                        first_text_sent = True
                        await self._send(AssistantTextMsg(
                            topic_id=self.topic_id,
                            text=block.text,
                            is_first=is_first,
                        ))
                    elif isinstance(block, ToolUseBlock):
                        tool_count += 1
                        await self._send(StatusUpdateMsg(
                            topic_id=self.topic_id,
                            tool_name=block.name,
                            tool_calls=tool_count,
                            input_data=block.input if hasattr(block, "input") else None,
                        ))

            elif isinstance(msg, TaskProgressMessage):
                last_tool = getattr(msg, "last_tool_name", None)
                if last_tool:
                    tool_count += 1
                    await self._send(StatusUpdateMsg(
                        topic_id=self.topic_id,
                        tool_name=f"Agent/{last_tool}",
                        tool_calls=tool_count,
                    ))

            elif isinstance(msg, TaskStartedMessage):
                logger.info("Sub-agent started in topic %d: %s", self.topic_id, getattr(msg, "description", ""))

            elif isinstance(msg, TaskNotificationMessage):
                status = getattr(msg, "status", "")
                summary = getattr(msg, "summary", "")
                if status == "failed":
                    await self._send(SystemNotificationMsg(
                        topic_id=self.topic_id,
                        subtype="sub_agent_failed",
                        text=f"⚠️ Sub-agent failed: {summary[:200]}",
                    ))
                logger.info("Sub-agent %s in topic %d: %s", status, self.topic_id, summary[:100])

            elif isinstance(msg, RateLimitEvent):
                info = msg.rate_limit_info
                if info.status in ("rejected", "allowed_warning"):
                    await self._send(RateLimitMsg(
                        topic_id=self.topic_id,
                        status=info.status,
                        resets_at=info.resets_at,
                        utilization=info.utilization,
                    ))

            elif isinstance(msg, SystemMessage):
                subtype = msg.subtype
                data = msg.data
                notification = None
                if "compact" in subtype:
                    summary = data.get("summary", data.get("message", ""))
                    notification = f"📦 Compact: {summary}" if summary else "📦 Conversation compacted"
                elif subtype == "model_change":
                    model = data.get("model", "unknown")
                    effort = data.get("effort", "")
                    notification = f"🔄 Model: {model}"
                    if effort:
                        notification += f" · {effort}"
                if notification:
                    await self._send(SystemNotificationMsg(
                        topic_id=self.topic_id, subtype=subtype, text=notification,
                    ))

            elif isinstance(msg, ResultMessage):
                if self.session_id is None and msg.session_id:
                    self.session_id = msg.session_id

                # Send turn completion with all data
                result_usage = getattr(msg, "usage", None) or last_usage
                await self._send(TurnCompletedMsg(
                    topic_id=self.topic_id,
                    cost_usd=msg.total_cost_usd,
                    duration_ms=msg.duration_ms,
                    tool_count=tool_count,
                    is_error=msg.is_error,
                    session_id=msg.session_id,
                    input_tokens=result_usage.get("input_tokens", 0),
                    output_tokens=result_usage.get("output_tokens", 0),
                    cache_read_tokens=result_usage.get("cache_read_input_tokens", 0),
                    model=last_model,
                ))

                logger.info(
                    "Turn complete for topic %d: cost=$%s, duration=%dms, tools=%d",
                    self.topic_id, msg.total_cost_usd, msg.duration_ms, tool_count,
                )

    async def enqueue(self, text: str) -> None:
        await self._message_queue.put(_QueueItem(text=text))

    async def enqueue_image(
        self,
        image_data: bytes,
        media_type: str = "image/jpeg",
        caption: str = "",
    ) -> None:
        """Queue an image message. Claude sees the image directly."""
        b64 = base64.b64encode(image_data).decode()
        blocks = [
            {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": b64}},
        ]
        if caption:
            blocks.append({"type": "text", "text": caption})
        else:
            blocks.append({"type": "text", "text": "User sent a photo. Describe what you see."})
        await self._message_queue.put(_QueueItem(content_blocks=blocks))

    async def interrupt(self) -> None:
        """Interrupt current turn without stopping."""
        if self._client:
            try:
                await self._client.interrupt()
            except Exception as e:
                logger.warning("Failed to interrupt topic %d: %s", self.topic_id, e)

    async def stop(self) -> None:
        if self._client:
            try:
                await self._client.interrupt()
            except Exception:
                pass
        await self._message_queue.put(None)
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=10.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()


class WorkerClient:
    """Connects to the bot IPC server and manages local Claude sessions."""

    def __init__(self, host: str, port: int, token: str, worker_id: str) -> None:
        self.host = host
        self.port = port
        self.token = token
        self.worker_id = worker_id
        self._sessions: dict[int, WorkerSession] = {}
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._running = True

    async def connect(self) -> bool:
        try:
            self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
        except (OSError, ConnectionRefusedError) as e:
            logger.error("Cannot connect to %s:%d: %s", self.host, self.port, e)
            return False

        await send_msg(self._writer, AuthMsg(token=self.token, worker_id=self.worker_id))
        msg = await recv_b2w(self._reader)
        if isinstance(msg, AuthOkMsg):
            logger.info("Authenticated as worker '%s'", self.worker_id)
            return True
        elif isinstance(msg, AuthFailMsg):
            logger.error("Authentication failed: %s", msg.reason)
            return False
        else:
            logger.error("Unexpected auth response: %r", msg)
            return False

    async def _heartbeat_loop(self) -> None:
        """Send ping every 30s to keep TCP alive and detect dead connections."""
        try:
            while self._running and self._writer and not self._writer.is_closing():
                await asyncio.sleep(30)
                try:
                    await send_msg(self._writer, PingMsg())
                except Exception:
                    logger.warning("Heartbeat send failed — connection likely dead")
                    break
        except asyncio.CancelledError:
            pass

    async def run(self) -> None:
        assert self._reader is not None and self._writer is not None

        heartbeat = asyncio.create_task(self._heartbeat_loop())

        try:
          while self._running:
            msg = await recv_b2w(self._reader)
            if msg is None:
                logger.info("Bot disconnected")
                break

            if isinstance(msg, PongMsg):
                continue  # Heartbeat response — ignore

            if isinstance(msg, StartSessionMsg):
                logger.info("Starting session: topic=%d cwd=%s", msg.topic_id, msg.cwd)
                session = WorkerSession(
                    topic_id=msg.topic_id,
                    cwd=msg.cwd,
                    writer=self._writer,
                    session_id=msg.session_id,
                    model=msg.model,
                )
                self._sessions[msg.topic_id] = session
                await session.start()

            elif isinstance(msg, StopSessionMsg):
                session = self._sessions.pop(msg.topic_id, None)
                if session:
                    logger.info("Stopping session topic=%d", msg.topic_id)
                    await session.stop()

            elif isinstance(msg, InterruptMsg):
                session = self._sessions.get(msg.topic_id)
                if session:
                    logger.info("Interrupting session topic=%d", msg.topic_id)
                    await session.interrupt()

            elif isinstance(msg, UserMessageMsg):
                session = self._sessions.get(msg.topic_id)
                if session:
                    await session.enqueue(msg.text)
                else:
                    logger.warning("UserMessage for unknown topic %d", msg.topic_id)

            elif isinstance(msg, UserFileMsg):
                session = self._sessions.get(msg.topic_id)
                if session:
                    # Save file to workdir
                    dest = Path(session.cwd) / Path(msg.file_name).name
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    dest.write_bytes(msg.file_bytes)
                    logger.info("Saved file for topic %d: %s (%d bytes)", msg.topic_id, dest, len(msg.file_bytes))

                    if msg.is_image and hasattr(session, "enqueue_image"):
                        await session.enqueue_image(
                            image_data=msg.file_bytes,
                            media_type=msg.media_type or "image/jpeg",
                            caption=msg.caption or "",
                        )
                    else:
                        prefix = "User sent a photo" if msg.is_image else "User sent file"
                        enqueue_text = f"{prefix}: {dest}\n{msg.caption or ''}".strip()
                        await session.enqueue(enqueue_text)
                else:
                    logger.warning("UserFile for unknown topic %d", msg.topic_id)

            elif isinstance(msg, PermissionResponseMsg):
                for session in self._sessions.values():
                    if msg.request_id in session._pending_permissions:
                        session.resolve_permission(msg.request_id, msg.action)
                        break

            elif isinstance(msg, SlashCommandMsg):
                session = self._sessions.get(msg.topic_id)
                if session:
                    await session.enqueue(msg.command)
                else:
                    logger.warning("SlashCommand for unknown topic %d", msg.topic_id)

            else:
                logger.warning("Unknown message from bot: %r", msg)
        finally:
            heartbeat.cancel()
            try:
                await heartbeat
            except asyncio.CancelledError:
                pass

    async def shutdown(self) -> None:
        self._running = False
        for topic_id, session in list(self._sessions.items()):
            try:
                await session.stop()
            except Exception as e:
                logger.error("Error stopping session %d: %s", topic_id, e)
        self._sessions.clear()
        if self._writer and not self._writer.is_closing():
            self._writer.close()


async def main(host: str, port: int, token: str, worker_id: str) -> None:
    if not HAS_SDK:
        logger.error("claude_agent_sdk not installed")
        sys.exit(1)

    client = WorkerClient(host, port, token, worker_id)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(client.shutdown()))

    retry_delay = 1
    max_delay = 60

    while True:
        if await client.connect():
            retry_delay = 1
            try:
                await client.run()
            except Exception as e:
                logger.error("Worker loop error: %s", e)
            finally:
                await client.shutdown()

        if not client._running:
            break

        logger.info("Reconnecting in %ds...", retry_delay)
        await asyncio.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, max_delay)
        client = WorkerClient(host, port, token, worker_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IPC Worker Client")
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=9600)
    parser.add_argument("--token", required=True)
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    try:
        import uvloop
        uvloop.install()
    except ImportError:
        pass

    asyncio.run(main(args.host, args.port, args.token, args.worker_id))
