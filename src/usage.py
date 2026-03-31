"""Cached fetcher for Anthropic OAuth usage (rate limits)."""

from __future__ import annotations

import asyncio
import json
import logging
import subprocess
import time
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

_CACHE_TTL = 300  # 5 minutes, matches statusline script
_API_URL = "https://api.anthropic.com/api/oauth/usage"


@dataclass
class WindowUsage:
    utilization: int  # 0-100 percentage
    resets_at: str | None  # ISO timestamp


@dataclass
class UsageData:
    five_hour: WindowUsage | None = None
    seven_day: WindowUsage | None = None
    fetched_at: float = 0.0


_cache: UsageData | None = None
_lock = asyncio.Lock()


def _get_oauth_token() -> str | None:
    """Read OAuth token from macOS Keychain or Linux credentials file."""
    import sys

    if sys.platform == "darwin":
        try:
            creds_raw = subprocess.check_output(
                ["security", "find-generic-password", "-s", "Claude Code-credentials", "-w"],
                stderr=subprocess.DEVNULL,
                timeout=3,
            )
            creds = json.loads(creds_raw)
            return creds.get("claudeAiOauth", {}).get("accessToken")
        except Exception:
            pass

    # Linux / fallback: ~/.claude/credentials.json
    try:
        from pathlib import Path
        creds_path = Path.home() / ".claude" / "credentials.json"
        creds = json.loads(creds_path.read_text())
        return creds.get("claudeAiOauth", {}).get("accessToken")
    except Exception:
        return None


def _parse_window(data: dict[str, Any] | None) -> WindowUsage | None:
    if not data:
        return None
    util = data.get("utilization")
    if util is None:
        return None
    return WindowUsage(
        utilization=int(util),
        resets_at=data.get("resets_at"),
    )


async def fetch_usage() -> UsageData | None:
    """Fetch usage from Anthropic API with caching. Returns None on failure."""
    global _cache

    now = time.monotonic()
    if _cache and (now - _cache.fetched_at) < _CACHE_TTL:
        return _cache

    async with _lock:
        # Double-check after acquiring lock
        if _cache and (now - _cache.fetched_at) < _CACHE_TTL:
            return _cache

        token = await asyncio.get_event_loop().run_in_executor(None, _get_oauth_token)
        if not token:
            return _cache  # return stale cache if available

        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    _API_URL,
                    headers={
                        "Authorization": f"Bearer {token}",
                        "anthropic-beta": "oauth-2025-04-20",
                        "Content-Type": "application/json",
                    },
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status != 200:
                        logger.debug("Usage API returned %d", resp.status)
                        return _cache
                    data = await resp.json()
        except Exception as e:
            logger.debug("Usage API fetch failed: %s", e)
            return _cache

        _cache = UsageData(
            five_hour=_parse_window(data.get("five_hour")),
            seven_day=_parse_window(data.get("seven_day")),
            fetched_at=time.monotonic(),
        )
        return _cache


def format_usage(usage: UsageData) -> str:
    """Format usage for Telegram display: '5h: 23% (2h14m) · 7d: 8% (5d3h)'."""
    parts = []
    if usage.five_hour:
        part = f"5h: {usage.five_hour.utilization}%"
        remaining = _format_remaining(usage.five_hour.resets_at)
        if remaining:
            part += f" ({remaining})"
        parts.append(part)
    if usage.seven_day:
        part = f"7d: {usage.seven_day.utilization}%"
        remaining = _format_remaining(usage.seven_day.resets_at)
        if remaining:
            part += f" ({remaining})"
        parts.append(part)
    return " · ".join(parts)


def _format_remaining(resets_at: str | None) -> str:
    """Format time remaining until reset."""
    if not resets_at:
        return ""
    try:
        from datetime import datetime, timezone
        reset_dt = datetime.fromisoformat(resets_at.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        diff = int((reset_dt - now).total_seconds())
        if diff < 0:
            diff = 0
        days = diff // 86400
        hours = (diff % 86400) // 3600
        minutes = (diff % 3600) // 60
        if days > 0:
            return f"{days}d{hours}h"
        elif hours > 0:
            return f"{hours}h{minutes}m"
        else:
            return f"{minutes}m"
    except Exception:
        return ""
