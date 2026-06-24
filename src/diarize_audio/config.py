"""Environment-driven configuration.

Loaded via `Config.from_env()`. `.env` files are loaded on import by
`__main__.py` before constructing the config so the process sees the
values (via python-dotenv); `from_env` itself only reads `os.environ`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


class ConfigError(ValueError):
    """Raised for malformed or missing configuration."""


_ALLOWED_SPEECH_MODELS = {"best", "nano"}
_ALLOWED_LOG_LEVELS = {"debug", "info", "warning", "error"}


def _get_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _get_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc


def _split_csv(raw: str) -> list[str]:
    return [p.strip() for p in raw.split(",") if p.strip()]


@dataclass
class Config:
    inbox_dirs: list[Path]
    poll_interval_seconds: int
    assemblyai_api_key: str
    speech_model: str
    language_code: str | None
    word_boost: list[str]
    max_retries: int
    in_flight_ttl_minutes: int
    aai_timeout_minutes: int
    drive_enabled: bool
    drive_root_folder_name: str
    upload_raw_audio_to_drive: bool
    log_level: str
    log_dir: Path = field(default_factory=lambda: Path("logs"))

    @classmethod
    def from_env(cls) -> "Config":
        api_key = os.environ.get("ASSEMBLYAI_API_KEY", "").strip()
        if not api_key:
            raise ConfigError("ASSEMBLYAI_API_KEY is required")

        raw_inbox = os.environ.get("INBOX_DIRS", "~/HiDock/archive")
        inbox_dirs = [Path(p).expanduser() for p in _split_csv(raw_inbox)]
        if not inbox_dirs:
            raise ConfigError("INBOX_DIRS must name at least one directory")

        speech_model = os.environ.get("SPEECH_MODEL", "best").strip().lower()
        if speech_model not in _ALLOWED_SPEECH_MODELS:
            raise ConfigError(
                f"SPEECH_MODEL must be one of {sorted(_ALLOWED_SPEECH_MODELS)}, got {speech_model!r}"
            )

        language_code = os.environ.get("LANGUAGE_CODE", "").strip() or None

        word_boost = _split_csv(os.environ.get("WORD_BOOST", ""))

        log_level = os.environ.get("LOG_LEVEL", "info").strip().lower()
        if log_level not in _ALLOWED_LOG_LEVELS:
            raise ConfigError(
                f"LOG_LEVEL must be one of {sorted(_ALLOWED_LOG_LEVELS)}, got {log_level!r}"
            )

        return cls(
            inbox_dirs=inbox_dirs,
            poll_interval_seconds=_get_int("POLL_INTERVAL_SECONDS", 30),
            assemblyai_api_key=api_key,
            speech_model=speech_model,
            language_code=language_code,
            word_boost=word_boost,
            max_retries=_get_int("MAX_RETRIES", 3),
            in_flight_ttl_minutes=_get_int("IN_FLIGHT_TTL_MINUTES", 60),
            aai_timeout_minutes=_get_int("AAI_TIMEOUT_MINUTES", 30),
            drive_enabled=_get_bool("DRIVE_ENABLED", False),
            drive_root_folder_name=os.environ.get("DRIVE_ROOT_FOLDER_NAME", "HiDock Archive"),
            upload_raw_audio_to_drive=_get_bool("UPLOAD_RAW_AUDIO_TO_DRIVE", False),
            log_level=log_level,
        )

    @property
    def primary_inbox(self) -> Path:
        return self.inbox_dirs[0]

    @property
    def state_dir(self) -> Path:
        return self.primary_inbox / ".state"

    @property
    def state_path(self) -> Path:
        return self.state_dir / "transcribe_state.json"

    @property
    def lock_path(self) -> Path:
        return self.state_dir / "diarize_audio.lock"
