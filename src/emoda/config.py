from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Annotated, Self

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "postgresql+asyncpg://emoda:emoda@db:5432/emoda"
    telegram_api_id: int | None = None
    telegram_api_hash: SecretStr | None = None
    telegram_phone: str | None = None
    telegram_session_path: Path = Path("/data/session/collector")
    bot_token: SecretStr | None = None
    bootstrap_admin_ids: Annotated[list[int], NoDecode] = Field(default_factory=list)
    gemini_api_key: SecretStr | None = None
    gemini_model: str = "gemini-3.5-flash-lite"
    gemini_ocr_model: str = "gemini-3.5-flash"
    media_root: Path = Path("/data/media")
    worker_poll_seconds: Decimal = Decimal("1")
    catchup_edit_rescan_limit: int = Field(default=500, ge=1, le=10_000)
    catchup_interval_seconds: int = Field(default=30, ge=5, le=3600)
    media_retention_days: int = Field(default=30, ge=1, le=3650)
    media_retention_batch_size: int = Field(default=25, ge=1, le=1000)
    media_retention_interval_seconds: int = Field(default=60, ge=5, le=86_400)
    media_retention_high_watermark_percent: int = Field(default=80, ge=50, le=95)
    media_retention_low_watermark_percent: int = Field(default=75, ge=40, le=94)
    media_retention_pressure_min_age_days: int = Field(default=1, ge=1, le=30)
    log_level: str = "INFO"
    health_file: Path = Path("/tmp/emoda-healthy")

    @field_validator("bootstrap_admin_ids", mode="before")
    @classmethod
    def parse_ids(cls, value: object) -> object:
        if isinstance(value, str):
            return [int(item.strip()) for item in value.split(",") if item.strip()]
        return value

    @field_validator("telegram_api_hash", "bot_token", "gemini_api_key", mode="before")
    @classmethod
    def empty_secret_is_unset(cls, value: object) -> object:
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @field_validator("telegram_api_id", mode="before")
    @classmethod
    def empty_api_id_is_unset(cls, value: object) -> object:
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @model_validator(mode="after")
    def validate_media_retention_watermarks(self) -> Self:
        if (
            self.media_retention_low_watermark_percent
            >= self.media_retention_high_watermark_percent
        ):
            raise ValueError("media retention low watermark must be below high watermark")
        return self


settings = Settings()
