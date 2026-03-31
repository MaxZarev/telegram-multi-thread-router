"""WorkerClient — TCP connection loop, session management, and IPC bridges."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict, is_dataclass
from pathlib import Path
from types import MethodType
from typing import TYPE_CHECKING
from uuid import uuid4

from claude_agent_sdk import PermissionResultAllow, PermissionResultDeny

from src.ipc.protocol import (
    AuthMsg,
    AuthOkMsg,
    InterruptMsg,
    PermissionRequestMsg,
    QuestionRequestMsg,
    QuestionResponseMsg,
    SessionStartedMsg,
    SessionEndedMsg,
    StartSessionMsg,
    StopSessionMsg,
    UserFileMsg,
    UserMessageMsg,
    PermissionResponseMsg,
    SlashCommandMsg,
    send_msg,
    recv_b2w,
)
from src.sessions.codex_runner import CodexRunner
from src.sessions.backend import normalize_provider, load_repo_local_instructions
from src.sessions.runner import SessionRunner
from src.sessions.permissions import PermissionManager
from src.sessions.state import SessionState
from src.worker.output_channel import WorkerOutputChannel

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

def _coerce_user_file_msg(msg: object) -> UserFileMsg | None:
    """Normalize file payloads even if runtime class identity differs.

    In multi-checkout worker deployments we have seen msg objects whose repr is
    `src.ipc.protocol.UserFileMsg` but whose class identity doesn't match the
    one imported in this module. Matching by shape keeps file/photo delivery
    working across those boundaries.
    """
    if isinstance(msg, UserFileMsg):
        return msg
    if type(msg).__name__ != "UserFileMsg":
        return None

    if is_dataclass(msg):
        data = asdict(msg)
    else:
        required = ("topic_id", "file_name", "file_bytes")
        optional = (
            "caption",
            "media_type",
            "reply_to_message_id",
            "is_image",
        )
        if not all(hasattr(msg, attr) for attr in required):
            return None
        data = {attr: getattr(msg, attr) for attr in (*required, *optional) if hasattr(msg, attr)}

    try:
        return UserFileMsg(**data)
    except Exception:
        return None


class WorkerClient:
    """Manages the TCP connection to the bot and local Claude sessions.

    Lifecycle:
      run()  — main reconnect loop with exponential backoff; call with asyncio.run()
    """

    def __init__(
        self,
        host: str,
        port: int,
        auth_token: str,
        worker_id: str,
    ) -> None:
        self._host = host
        self._port = port
        self._auth_token = auth_token
        self._worker_id = worker_id

        self._sessions: dict[int, SessionRunner | CodexRunner] = {}
        self._output_channel: WorkerOutputChannel | None = None
        self._writer: asyncio.StreamWriter | None = None
        # Pending permission request futures: request_id -> Future[str]
        self._permission_futures: dict[str, asyncio.Future] = {}
        self._question_futures: dict[str, asyncio.Future] = {}

    async def run(self) -> None:
        """Main reconnect loop with exponential backoff (1s → 2s → 4s → … → 60s max)."""
        delay = 1.0
        while True:
            try:
                reader, writer = await asyncio.open_connection(self._host, self._port)
                self._writer = writer

                # Authentication handshake
                await send_msg(writer, AuthMsg(token=self._auth_token, worker_id=self._worker_id))
                response = await recv_b2w(reader)
                if not isinstance(response, AuthOkMsg):
                    logger.error(
                        "Worker %s: auth rejected by bot: %s", self._worker_id, response
                    )
                    writer.close()
                    await writer.wait_closed()
                    break  # permanent auth failure — do not retry

                delay = 1.0  # reset backoff on successful connection
                logger.info(
                    "Worker %s: connected to bot IPC at %s:%d",
                    self._worker_id,
                    self._host,
                    self._port,
                )

                # Create or update the shared output channel
                if self._output_channel is None:
                    self._output_channel = WorkerOutputChannel(writer, chat_id=0)
                else:
                    self._output_channel.set_writer(writer)

                # Re-register existing sessions so the bot knows we still have them
                for topic_id, runner in list(self._sessions.items()):
                    session_id = getattr(runner, "backend_session_id", None) or runner.session_id
                    if session_id:
                        try:
                            await send_msg(
                                writer,
                                SessionStartedMsg(
                                    topic_id=topic_id,
                                    session_id=session_id,
                                ),
                            )
                        except OSError as e:
                            logger.warning(
                                "Worker %s: failed to re-register topic %d: %s",
                                self._worker_id,
                                topic_id,
                                e,
                            )

                # Run the receive loop until EOF or error
                await self._receive_loop(reader)

            except (ConnectionRefusedError, OSError, asyncio.TimeoutError) as e:
                logger.warning(
                    "Worker %s: IPC connect failed: %s — retrying in %.0fs",
                    self._worker_id,
                    e,
                    delay,
                )
            finally:
                self._on_disconnected()

            await asyncio.sleep(delay)
            delay = min(delay * 2, 60.0)

    async def _receive_loop(self, reader: asyncio.StreamReader) -> None:
        """Process messages from the bot until EOF."""
        while True:
            msg = await recv_b2w(reader)
            if msg is None:
                logger.info("Worker %s: IPC connection closed by bot (EOF)", self._worker_id)
                break

            if isinstance(msg, StartSessionMsg):
                await self._start_session(msg)
            elif isinstance(msg, StopSessionMsg):
                await self._stop_session(msg.topic_id)
            elif isinstance(msg, UserMessageMsg):
                runner = self._sessions.get(msg.topic_id)
                if runner:
                    await runner.enqueue(msg.text)
                else:
                    logger.warning(
                        "Worker %s: UserMessage for unknown topic %d",
                        self._worker_id,
                        msg.topic_id,
                    )
            elif (user_file_msg := _coerce_user_file_msg(msg)) is not None:
                await self._handle_file_input(user_file_msg)
            elif isinstance(msg, PermissionResponseMsg):
                self._resolve_permission(msg.request_id, msg.action)
            elif isinstance(msg, QuestionResponseMsg):
                self._resolve_question(msg.request_id, msg.answers)
            elif isinstance(msg, InterruptMsg):
                runner = self._sessions.get(msg.topic_id)
                if runner:
                    await runner.interrupt()
                else:
                    logger.warning(
                        "Worker %s: Interrupt for unknown topic %d",
                        self._worker_id,
                        msg.topic_id,
                    )
            elif isinstance(msg, SlashCommandMsg):
                runner = self._sessions.get(msg.topic_id)
                if runner:
                    await runner.enqueue(msg.command)
                else:
                    logger.warning(
                        "Worker %s: SlashCommand for unknown topic %d",
                        self._worker_id,
                        msg.topic_id,
                    )
            else:
                logger.warning("Worker %s: unknown message type: %s", self._worker_id, type(msg))

    async def _start_session(self, msg: StartSessionMsg) -> None:
        """Create and start a new local provider session for the given topic."""
        topic_id = msg.topic_id
        provider = normalize_provider(getattr(msg, "provider", "claude"))

        if topic_id in self._sessions:
            runner = self._sessions[topic_id]
            if runner.is_alive:
                logger.warning(
                    "Worker %s: StartSession for topic %d but session already running",
                    self._worker_id,
                    topic_id,
                )
                return
            # Dead session — replace it
            del self._sessions[topic_id]

        if self._output_channel is None:
            logger.error(
                "Worker %s: cannot start session — no output channel yet", self._worker_id
            )
            return

        if provider == "codex":
            runner = self._build_codex_runner(topic_id, msg)
        elif provider == "claude":
            runner = self._build_claude_runner(topic_id, msg)
        else:
            logger.error(
                "Worker %s: provider %s is not supported by the Python worker",
                self._worker_id,
                provider,
            )
            await self._output_channel._send(
                SessionEndedMsg(
                    topic_id=topic_id,
                    error=f"Provider '{provider}' is not supported by worker '{self._worker_id}'",
                )
            )
            return

        self._sessions[topic_id] = runner
        await runner.start()
        asyncio.create_task(self._announce_session_started(topic_id, runner))
        logger.info(
            "Worker %s: started %s session for topic %d (cwd=%s)",
            self._worker_id,
            provider,
            topic_id,
            msg.cwd,
        )

    def _build_claude_runner(self, topic_id: int, msg: StartSessionMsg) -> SessionRunner:
        """Create a worker-side Claude runner with the TCP permission bridge."""
        if self._output_channel is None:
            raise RuntimeError("worker output channel is not initialized")

        perm_mgr = PermissionManager()
        runner = SessionRunner(
            thread_id=topic_id,
            workdir=msg.cwd,
            bot=self._output_channel,
            chat_id=0,
            permission_manager=perm_mgr,
            session_id=msg.session_id or msg.backend_session_id,
            model=msg.model,
        )

        original_allowed_tools = runner._allowed_tools

        async def _worker_can_use_tool(
            _runner: SessionRunner,
            tool_name: str,
            input_data: dict,
            context,
        ) -> PermissionResultAllow | PermissionResultDeny:
            if tool_name in original_allowed_tools:
                return PermissionResultAllow(updated_input=input_data)

            prev_state = _runner.state
            _runner.state = SessionState.WAITING_PERMISSION
            request_id, future = perm_mgr.create_request()
            self._permission_futures[request_id] = future

            try:
                await self._output_channel._send(
                    PermissionRequestMsg(
                        topic_id=topic_id,
                        request_id=request_id,
                        tool_name=tool_name,
                        input_data=input_data,
                    )
                )
                action = await asyncio.wait_for(future, timeout=300.0)
            except asyncio.TimeoutError:
                perm_mgr.expire(request_id)
                return PermissionResultDeny(
                    message="Timed out — user did not respond within 5 minutes"
                )
            except asyncio.CancelledError:
                perm_mgr.expire(request_id)
                raise
            finally:
                _runner.state = prev_state
                self._permission_futures.pop(request_id, None)

            if action == "allow":
                return PermissionResultAllow(updated_input=input_data)
            if action == "always":
                original_allowed_tools.add(tool_name)
                return PermissionResultAllow(updated_input=input_data)
            return PermissionResultDeny(message="Denied by user")

        runner._can_use_tool = MethodType(_worker_can_use_tool, runner)  # type: ignore[method-assign]
        return runner

    def _build_codex_runner(self, topic_id: int, msg: StartSessionMsg) -> CodexRunner:
        """Create a worker-side Codex runner with IPC approval/question bridges."""
        if self._output_channel is None:
            raise RuntimeError("worker output channel is not initialized")

        provider_options = msg.provider_options or {}
        base_instructions = provider_options.get("base_instructions") or load_repo_local_instructions(msg.cwd)
        runner = CodexRunner(
            thread_id=topic_id,
            workdir=msg.cwd,
            bot=self._output_channel,
            chat_id=0,
            backend_session_id=msg.backend_session_id or msg.session_id,
            model=msg.model,
            base_instructions=base_instructions,
            developer_instructions=provider_options.get("developer_instructions"),
            mcp_server_urls=provider_options.get("mcp_server_urls"),
        )

        async def _worker_ask_permission(
            _runner: CodexRunner,
            method: str,
            params: dict,
        ) -> dict:
            if _runner.auto_mode:
                return {"decision": "accept"}

            tool_name = "Bash" if "commandExecution" in method else "Edit"
            input_data = {
                "command": params.get("command", ""),
                "file_path": params.get("grantRoot", ""),
                "reason": params.get("reason", ""),
            }
            action = await self._request_permission(topic_id, tool_name, input_data)
            if action == "always":
                return {"decision": "acceptForSession"}
            if action == "allow":
                return {"decision": "accept"}
            return {"decision": "decline"}

        async def _worker_ask_permissions_profile(
            _runner: CodexRunner,
            params: dict,
        ) -> dict:
            permissions = params.get("permissions", {})
            if _runner.auto_mode:
                return {"scope": "session", "permissions": permissions}

            action = await self._request_permission(
                topic_id,
                "request_permissions",
                permissions,
            )
            if action in {"allow", "always"}:
                return {
                    "scope": "session" if action == "always" else "turn",
                    "permissions": permissions,
                }
            return {"scope": "turn", "permissions": {}}

        async def _worker_ask_user_input(
            _runner: CodexRunner,
            params: dict,
        ) -> dict:
            questions = params.get("questions", [])
            if not questions:
                return {"answers": {}}

            answers = await self._request_questions(topic_id, questions)
            payload: dict[str, dict] = {"answers": {}}
            for question in questions:
                qid = question.get("id")
                qtext = question.get("question", "")
                if not qid:
                    continue
                raw_answer = answers.get(qtext)
                if raw_answer in (None, "(no selection)"):
                    selected = []
                else:
                    selected = [
                        part.strip()
                        for part in str(raw_answer).split(",")
                        if part.strip()
                    ]
                payload["answers"][qid] = {"answers": selected}
            return payload

        runner._ask_telegram_permission = MethodType(  # type: ignore[method-assign]
            _worker_ask_permission,
            runner,
        )
        runner._ask_telegram_permissions_profile = MethodType(  # type: ignore[method-assign]
            _worker_ask_permissions_profile,
            runner,
        )
        runner._ask_telegram_user_input = MethodType(  # type: ignore[method-assign]
            _worker_ask_user_input,
            runner,
        )
        return runner

    async def _request_permission(self, topic_id: int, tool_name: str, input_data: dict) -> str:
        """Send a permission request over IPC and await the owner's response."""
        if self._output_channel is None:
            return "deny"

        request_id = str(uuid4())
        future = asyncio.get_running_loop().create_future()
        self._permission_futures[request_id] = future
        try:
            await self._output_channel._send(
                PermissionRequestMsg(
                    topic_id=topic_id,
                    request_id=request_id,
                    tool_name=tool_name,
                    input_data=input_data,
                )
            )
            return await asyncio.wait_for(future, timeout=300.0)
        except asyncio.TimeoutError:
            return "deny"
        finally:
            self._permission_futures.pop(request_id, None)

    async def _request_questions(self, topic_id: int, questions: list[dict]) -> dict[str, str]:
        """Send interactive questions over IPC and await the owner's answers."""
        if self._output_channel is None:
            return {}

        request_id = str(uuid4())
        future = asyncio.get_running_loop().create_future()
        self._question_futures[request_id] = future
        try:
            await self._output_channel._send(
                QuestionRequestMsg(
                    topic_id=topic_id,
                    request_id=request_id,
                    questions=questions,
                )
            )
            return await asyncio.wait_for(future, timeout=300.0)
        except asyncio.TimeoutError:
            return {}
        finally:
            self._question_futures.pop(request_id, None)

    async def _announce_session_started(
        self,
        topic_id: int,
        runner: SessionRunner | CodexRunner,
    ) -> None:
        """Publish the backend session ID once the runner has one."""
        if self._output_channel is None:
            return

        for _ in range(100):
            session_id = getattr(runner, "backend_session_id", None) or runner.session_id
            if session_id:
                await self._output_channel._send(
                    SessionStartedMsg(topic_id=topic_id, session_id=session_id)
                )
                return
            await asyncio.sleep(0.1)

    async def _handle_file_input(self, msg: UserFileMsg) -> None:
        """Persist a remote user attachment and forward it into the target session."""
        runner = self._sessions.get(msg.topic_id)
        if runner is None:
            logger.warning(
                "Worker %s: UserFile for unknown topic %d",
                self._worker_id,
                msg.topic_id,
            )
            return

        dest = Path(runner.workdir) / Path(msg.file_name).name
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(msg.file_bytes)
        logger.info(
            "Worker %s: saved file for topic %d to %s (%d bytes, image=%s)",
            self._worker_id,
            msg.topic_id,
            dest,
            len(msg.file_bytes),
            msg.is_image,
        )

        caption = msg.caption or ""
        if msg.is_image and hasattr(runner, "enqueue_image"):
            logger.info(
                "Worker %s: forwarding image for topic %d into native Codex image input",
                self._worker_id,
                msg.topic_id,
            )
            await runner.enqueue_image(
                image_data=msg.file_bytes,
                media_type=msg.media_type or "image/jpeg",
                caption=caption,
                reply_to_message_id=msg.reply_to_message_id,
            )
            return

        prefix = "User sent a photo" if msg.is_image else "User sent file"
        enqueue_text = f"{prefix}: {dest}\n{caption}".strip()
        logger.info(
            "Worker %s: forwarding file for topic %d as text prompt",
            self._worker_id,
            msg.topic_id,
        )
        await runner.enqueue(enqueue_text, reply_to_message_id=msg.reply_to_message_id)

    def _resolve_permission(self, request_id: str, action: str) -> None:
        """Resolve a pending permission future from a bot response."""
        future = self._permission_futures.pop(request_id, None)
        if future is None:
            logger.warning(
                "Worker %s: PermissionResponse for unknown request_id %s",
                self._worker_id,
                request_id,
            )
            return
        if not future.done():
            future.set_result(action)
        else:
            logger.warning(
                "Worker %s: PermissionResponse for already-resolved request_id %s",
                self._worker_id,
                request_id,
            )

    def _resolve_question(self, request_id: str, answers: dict[str, str]) -> None:
        """Resolve a pending question future from a bot response."""
        future = self._question_futures.pop(request_id, None)
        if future is None:
            logger.warning(
                "Worker %s: QuestionResponse for unknown request_id %s",
                self._worker_id,
                request_id,
            )
            return
        if not future.done():
            future.set_result(answers)
        else:
            logger.warning(
                "Worker %s: QuestionResponse for already-resolved request_id %s",
                self._worker_id,
                request_id,
            )

    def _on_disconnected(self) -> None:
        """Called when the TCP connection drops. Resolve all pending IPC futures."""
        if self._permission_futures:
            logger.info(
                "Worker %s: TCP disconnect — cancelling %d pending permission futures",
                self._worker_id,
                len(self._permission_futures),
            )
            for future in self._permission_futures.values():
                if not future.done():
                    # Resolve with "deny" so _can_use_tool unblocks cleanly
                    future.set_result("deny")
            self._permission_futures.clear()

        if self._question_futures:
            logger.info(
                "Worker %s: TCP disconnect — cancelling %d pending question futures",
                self._worker_id,
                len(self._question_futures),
            )
            for future in self._question_futures.values():
                if not future.done():
                    future.set_result({})
            self._question_futures.clear()

        if self._writer:
            try:
                self._writer.close()
            except Exception:
                pass
            self._writer = None

        logger.info("Worker %s: disconnected from bot IPC", self._worker_id)

    async def _stop_session(self, topic_id: int) -> None:
        """Stop and remove a running session."""
        runner = self._sessions.pop(topic_id, None)
        if runner is None:
            logger.warning(
                "Worker %s: StopSession for unknown topic %d", self._worker_id, topic_id
            )
            return
        await runner.stop()
        logger.info("Worker %s: stopped session for topic %d", self._worker_id, topic_id)
