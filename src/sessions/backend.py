"""Shared session backend contract plus provider/server/workdir helpers."""

from __future__ import annotations

import os
from pathlib import Path
import re
from typing import Literal, Protocol, runtime_checkable

from src.sessions.state import SessionState

SessionProvider = Literal["claude", "codex"]

SUPPORTED_SESSION_PROVIDERS: tuple[SessionProvider, ...] = ("claude", "codex")
SERVER_ALIAS_MAP: dict[str, str] = {
    "": "local",
    "local": "local",
    "mac": "local",
    "macbook": "local",
    "this-mac": "local",
    "personal-mac": "local",
    "personal": "personal",
    "personal-server": "personal",
    "personal_server": "personal",
}

_REPO_LINE_RE = re.compile(
    r"^- \*\*(?P<label>[^*]+)\*\*: (?P<paths>.+)$"
)


def get_default_session_provider() -> SessionProvider:
    """Return the configured default provider, clamped to supported values."""
    provider = os.getenv("DEFAULT_PROVIDER")
    if provider is None:
        env_file = Path(".env")
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if line.startswith("DEFAULT_PROVIDER="):
                    provider = line.split("=", 1)[1]
                    break
    if (provider or "claude").strip().lower() == "codex":
        return "codex"
    return "claude"


DEFAULT_SESSION_PROVIDER: SessionProvider = get_default_session_provider()

_LIMIT_ERROR_PATTERNS: tuple[str, ...] = (
    "rate limit",
    "rate-limited",
    "rate limited",
    "usage limit",
    "quota",
    "insufficient_quota",
    "credit balance",
    "credits exhausted",
    "exhausted",
    "too many requests",
    "429",
)


def is_supported_provider(provider: str | None) -> bool:
    """Return True if the value is one of the explicit supported providers."""
    return provider in SUPPORTED_SESSION_PROVIDERS


def normalize_provider(provider: str | None) -> SessionProvider:
    """Normalize a provider string, defaulting legacy/empty values to the configured default."""
    if provider == "codex":
        return "codex"
    if provider == "claude":
        return "claude"
    return get_default_session_provider()


def looks_like_provider_limit_error(text: str | None) -> bool:
    """Best-effort classifier for provider exhaustion/quota failures."""
    if not text:
        return False
    haystack = text.strip().lower()
    return any(pattern in haystack for pattern in _LIMIT_ERROR_PATTERNS)


def normalize_server_name(server: str | None) -> str:
    """Normalize well-known server aliases while preserving unknown worker IDs."""
    if server is None:
        return "local"
    value = server.strip()
    if not value:
        return "local"
    return SERVER_ALIAS_MAP.get(value.lower(), value)


def get_orchestrator_server_guidance() -> str:
    """Return the environment-specific server routing guidance for orchestrators."""
    return (
        "Execution environment:\n"
        "- Use server='local' for this Mac.\n"
        "- Use server='personal' for the Personal Server "
        "(SSH host 'personal-server', IP 167.235.155.73).\n"
        "- Accepted aliases for the Personal Server are: personal, personal-server, personal_server.\n"
    )


def load_private_infra_context() -> str:
    """Load optional private infra instructions from local docs."""
    parts: list[str] = []
    for name in ("AGENTS.local.md", "CLAUDE.local.md"):
        path = Path(name)
        if not path.exists():
            continue
        try:
            text = path.read_text().strip()
        except OSError:
            continue
        if text and text not in parts:
            parts.append(text)
    return "\n\n".join(parts)


def load_repo_local_instructions(workdir: str) -> str | None:
    """Load repo-local agent instructions when present."""
    for name in ("AGENTS.md", "CLAUDE.md"):
        path = Path(workdir).expanduser() / name
        try:
            text = path.read_text().strip()
        except OSError:
            continue
        if text:
            return text
    return None


def load_repo_path_map() -> dict[str, dict[str, str]]:
    """Parse repo path mappings from the private local infra doc if present."""
    text = load_private_infra_context()
    if not text:
        return {}

    mappings: dict[str, dict[str, str]] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        match = _REPO_LINE_RE.match(line)
        if not match:
            continue

        label = match.group("label").strip().lower()
        paths = re.findall(r"`([^`]+)`", match.group("paths"))
        if not paths:
            continue

        aliases = {
            label,
            label.replace(" repo", "").strip(),
        }
        for path in paths:
            aliases.add(Path(path.rstrip("/")).name.lower())

        entry: dict[str, str] = {}
        if paths:
            first = paths[0].rstrip("/")
            if first.startswith("/Users/"):
                entry["local"] = first
        if len(paths) >= 2:
            second = paths[1].rstrip("/")
            if second.startswith("/"):
                entry["personal"] = second
                entry["server"] = second

        if not entry:
            continue

        for alias in aliases:
            if alias:
                mappings[alias] = dict(entry)

    return mappings


def resolve_workdir_for_server(server: str | None, workdir: str) -> str:
    """Resolve a user-provided workdir into a server-appropriate path when possible."""
    server_name = normalize_server_name(server)
    raw_workdir = workdir.strip()
    if server_name == "local" or not raw_workdir:
        return raw_workdir

    repo_map = load_repo_path_map()
    normalized = raw_workdir.rstrip("/")
    path_name = Path(normalized).name.lower()

    candidates = [normalized.lower(), path_name]
    for candidate in candidates:
        entry = repo_map.get(candidate)
        if not entry:
            continue
        remote_path = entry.get(server_name) or entry.get("server")
        local_path = entry.get("local")
        if remote_path and (
            normalized == local_path
            or normalized.startswith("/Users/")
            or normalized.lower() == candidate
        ):
            return remote_path

    return raw_workdir


def validate_workdir_for_server(server: str | None, workdir: str) -> str | None:
    """Return an error message when a workdir is obviously invalid for the target server."""
    server_name = normalize_server_name(server)
    if server_name == "local":
        return None

    normalized = workdir.strip()
    if normalized.startswith("/Users/"):
        return (
            f"Remote server '{server_name}' cannot use macOS path '{normalized}'. "
            "Resolve the server path first (for example '/root/...')."
        )
    return None


@runtime_checkable
class SessionBackend(Protocol):
    """Common contract implemented by local and remote session backends."""

    thread_id: int
    workdir: str
    provider: SessionProvider
    session_id: str | None
    backend_session_id: str | None
    state: SessionState
    auto_mode: bool

    @property
    def is_alive(self) -> bool: ...

    async def start(self) -> None: ...

    async def enqueue(self, text: str, reply_to_message_id: int | None = None) -> None: ...

    async def interrupt(self) -> bool: ...

    async def stop(self) -> None: ...
