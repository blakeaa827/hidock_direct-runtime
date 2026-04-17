"""Configuration loading from environment and the forge secrets `.env`.

Resolution order (first hit wins per variable):
  1. Existing process environment.
  2. `$HIDOCK_DIRECT_ENV_FILE` if set.
  3. `forge/projects/hidock_direct/secrets/.env` discovered relative to this
     runtime repo (sibling of `diarize_audio-runtime`).
  4. `./.env` in the runtime root.

Only the four PRD-defined variables are recognized.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import dotenv_values


RUNTIME_ROOT = Path(__file__).resolve().parents[2]
FORGE_SECRETS_CANDIDATES = (
    RUNTIME_ROOT.parent / "forge" / "projects" / "hidock_direct" / "secrets" / ".env",
    Path.home() / "Library" / "Mobile Documents" / "com~apple~CloudDocs" / "Claude" / "forge" / "projects" / "hidock_direct" / "secrets" / ".env",
)

_TRUE_SET = {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Config:
    archive_dir: Path
    poll_interval_seconds: int
    delete_from_device_after_offload: bool
    transcribe_on_offload: bool
    log_level: str
    source: str  # "env" or "<path>" — for diagnostics

    @property
    def state_dir(self) -> Path:
        return self.archive_dir / ".state"

    @property
    def tmp_dir(self) -> Path:
        return self.archive_dir / ".tmp"

    @property
    def state_path(self) -> Path:
        return self.state_dir / "offload_state.json"

    @property
    def state_bak_path(self) -> Path:
        return self.state_dir / "offload_state.json.bak"

    @property
    def lock_path(self) -> Path:
        return self.state_dir / "hidock_direct.lock"


def _discover_env_file() -> Optional[Path]:
    explicit = os.environ.get("HIDOCK_DIRECT_ENV_FILE")
    if explicit:
        p = Path(explicit).expanduser()
        return p if p.is_file() else None
    for candidate in FORGE_SECRETS_CANDIDATES:
        if candidate.is_file():
            return candidate
    local = RUNTIME_ROOT / ".env"
    return local if local.is_file() else None


def _resolve(name: str, default: str, env_values: dict, overlay: Optional[dict]) -> str:
    if overlay is not None and name in overlay:
        return str(overlay[name])
    if name in os.environ:
        return os.environ[name]
    if name in env_values:
        return env_values[name] or default
    return default


def load_config(env_file: Optional[os.PathLike[str] | str] = None, overlay: Optional[dict] = None) -> Config:
    """Load and return a Config. Missing optional values fall back to defaults."""
    if env_file is not None:
        env_path: Optional[Path] = Path(env_file).expanduser()
    else:
        env_path = _discover_env_file()

    env_values: dict = {}
    if env_path and env_path.is_file():
        env_values = {k: v for k, v in dotenv_values(env_path).items() if v is not None}

    archive = _resolve("HIDOCK_ARCHIVE_DIR", "~/HiDock/archive", env_values, overlay)
    poll = _resolve("POLL_INTERVAL_SECONDS", "10", env_values, overlay)
    delete = _resolve("DELETE_FROM_DEVICE_AFTER_OFFLOAD", "false", env_values, overlay)
    transcribe = _resolve("TRANSCRIBE_ON_OFFLOAD", "true", env_values, overlay)
    log = _resolve("LOG_LEVEL", "info", env_values, overlay).lower()

    try:
        poll_int = int(poll)
    except ValueError as exc:
        raise ValueError(f"POLL_INTERVAL_SECONDS must be an integer, got {poll!r}") from exc
    if poll_int <= 0:
        raise ValueError(f"POLL_INTERVAL_SECONDS must be > 0, got {poll_int}")

    delete_bool = str(delete).strip().lower() in _TRUE_SET
    transcribe_bool = str(transcribe).strip().lower() in _TRUE_SET
    if log not in ("debug", "info", "warning", "error"):
        raise ValueError(f"LOG_LEVEL must be one of debug/info/warning/error, got {log!r}")

    return Config(
        archive_dir=Path(archive).expanduser(),
        poll_interval_seconds=poll_int,
        delete_from_device_after_offload=delete_bool,
        transcribe_on_offload=transcribe_bool,
        log_level=log,
        source=str(env_path) if env_path else "env",
    )
