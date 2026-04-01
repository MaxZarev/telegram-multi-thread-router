"""Typed configuration loaded from .env file."""

from typing import Literal

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Bot configuration. All fields loaded from environment or .env file."""

    bot_token: str
    owner_user_id: int
    chat_id: int | None = Field(
        default=None,
        validation_alias=AliasChoices("CHAT_ID", "GROUP_CHAT_ID"),
    )
    auth_token: str
    ipc_host: str = "0.0.0.0"
    ipc_port: int = 9800
    enable_codex: bool = False
    default_provider: Literal["claude", "codex"] = "claude"
    stream_intermediate_messages: bool = True
    transcriber: Literal["whisper", "deepgram"] = "whisper"
    deepgram_api_key: str = ""

    # Logging
    log_dir: str = "logs"
    log_max_bytes: int = 50_000_000      # 50 MB
    log_backup_count: int = 3
    log_level: str = "INFO"
    turn_warn_seconds: int = 300          # 5 min
    turn_error_seconds: int = 900         # 15 min

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


settings = Settings()
