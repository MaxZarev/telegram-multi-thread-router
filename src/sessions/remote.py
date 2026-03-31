"""RemoteSession — bot-side proxy for a provider session running on a remote worker."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from src.ipc.protocol import (
    InterruptMsg,
    StartSessionMsg,
    StopSessionMsg,
    UserFileMsg,
    UserMessageMsg,
)
from src.sessions.backend import SessionProvider, normalize_provider
from src.sessions.state import SessionState

if TYPE_CHECKING:
    from src.ipc.server import WorkerRegistry


logger = logging.getLogger(__name__)


class RemoteSession:
    """Proxy for a session that runs on a remote worker process.

    Mimics the SessionRunner interface (enqueue, stop, state, is_alive, workdir,
    thread_id, session_id) so SessionManager can store both types transparently.
    """

    def __init__(
        self,
        thread_id: int,
        workdir: str,
        worker_id: str,
        worker_registry: WorkerRegistry,
        session_id: str | None = None,
        backend_session_id: str | None = None,
        provider: str | None = None,
        model: str | None = None,
        provider_options: dict | None = None,
    ) -> None:
        self.thread_id = thread_id
        self.workdir = workdir
        self.worker_id = worker_id
        self.provider: SessionProvider = normalize_provider(provider)
        self.session_id = session_id
        self.backend_session_id = backend_session_id or session_id
        self.model = model
        self.state: SessionState = SessionState.IDLE
        self.auto_mode: bool = False
        self.provider_options = provider_options
        self._registry = worker_registry

    @property
    def is_alive(self) -> bool:
        """True if the remote worker is currently connected."""
        return self._registry.is_connected(self.worker_id)

    async def start(self) -> None:
        """Send StartSessionMsg to the worker to begin the remote provider session."""
        sent = await self._registry.send_to(
            self.worker_id,
            StartSessionMsg(
                topic_id=self.thread_id,
                cwd=self.workdir,
                session_id=self.session_id,
                backend_session_id=self.backend_session_id,
                model=self.model,
                provider=self.provider,
                provider_options=self.provider_options,
            ),
        )
        if not sent:
            raise ConnectionError(
                f"Worker '{self.worker_id}' is not connected. Cannot start session."
            )

    async def enqueue(self, text: str, reply_to_message_id: int | None = None) -> None:
        """Forward a user message to the worker.

        Raises ConnectionError if the worker is not connected, so the caller
        can notify the user instead of silently dropping the message.
        """
        sent = await self._registry.send_to(
            self.worker_id,
            UserMessageMsg(
                topic_id=self.thread_id,
                text=text,
                reply_to_message_id=reply_to_message_id,
            ),
        )
        if not sent:
            raise ConnectionError(
                f"Worker '{self.worker_id}' is not connected. Message not delivered."
            )

    async def interrupt(self) -> bool:
        """Interrupt the active turn on the remote worker."""
        sent = await self._registry.send_to(
            self.worker_id,
            InterruptMsg(topic_id=self.thread_id),
        )
        return sent

    async def enqueue_file(
        self,
        *,
        file_name: str,
        file_bytes: bytes,
        caption: str = "",
        media_type: str | None = None,
        reply_to_message_id: int | None = None,
        is_image: bool = False,
    ) -> None:
        """Forward a file/photo payload to the worker."""
        logger.info(
            "Sending file to worker %s for topic %d: %s (%d bytes, image=%s)",
            self.worker_id,
            self.thread_id,
            file_name,
            len(file_bytes),
            is_image,
        )
        sent = await self._registry.send_to(
            self.worker_id,
            UserFileMsg(
                topic_id=self.thread_id,
                file_name=file_name,
                file_bytes=file_bytes,
                caption=caption or None,
                media_type=media_type,
                reply_to_message_id=reply_to_message_id,
                is_image=is_image,
            ),
        )
        if not sent:
            raise ConnectionError(
                f"Worker '{self.worker_id}' is not connected. File not delivered."
            )

    async def stop(self) -> None:
        """Instruct the worker to stop this session and mark state locally."""
        await self._registry.send_to(
            self.worker_id,
            StopSessionMsg(topic_id=self.thread_id),
        )
        self.state = SessionState.STOPPED
