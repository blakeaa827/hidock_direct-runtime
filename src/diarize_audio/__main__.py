"""`python -m diarize_audio` entry point."""

from __future__ import annotations

import logging
import logging.handlers
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from .assemblyai_client import AAIClient
from .config import Config, ConfigError
from .drive_sync import DriveSync
from .locks import AlreadyLocked, FileLock
from .state import State
from .worker import run_loop


def _load_env() -> None:
    """Load `.env` from an explicit override, the forge secrets dir
    (discovered relative to this runtime repo), then the cwd."""
    candidates: list[Path] = []
    explicit = os.environ.get("DIARIZE_AUDIO_ENV_FILE")
    if explicit:
        candidates.append(Path(explicit).expanduser())
    candidates.append(
        Path(__file__).resolve().parents[2].parent
        / "forge/projects/diarize_audio/secrets/.env"
    )
    candidates.append(Path.cwd() / ".env")
    for p in candidates:
        if p.exists():
            load_dotenv(p, override=False)


def _configure_logging(cfg: Config) -> None:
    cfg.log_dir.mkdir(parents=True, exist_ok=True)
    log_path = cfg.log_dir / "diarize_audio.log"
    handler = logging.handlers.TimedRotatingFileHandler(
        log_path, when="midnight", backupCount=7, encoding="utf-8"
    )
    formatter = logging.Formatter(
        fmt='{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers = [handler, logging.StreamHandler(sys.stderr)]
    root.setLevel(cfg.log_level.upper())


def main() -> int:
    _load_env()
    try:
        cfg = Config.from_env()
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2

    _configure_logging(cfg)
    log = logging.getLogger("diarize_audio")

    cfg.state_dir.mkdir(parents=True, exist_ok=True)
    try:
        lock = FileLock(cfg.lock_path)
        lock.__enter__()
    except AlreadyLocked as exc:
        log.error("another diarize_audio instance is running: %s", exc)
        print(f"another instance holds the lock ({cfg.lock_path})", file=sys.stderr)
        return 3

    try:
        state = State.load(cfg.state_path)
        aai_client = AAIClient(
            api_key=cfg.assemblyai_api_key,
            speech_model=cfg.speech_model,
            language_code=cfg.language_code,
            word_boost=cfg.word_boost,
            timeout_minutes=cfg.aai_timeout_minutes,
        )
        if cfg.drive_enabled:
            drive_sync = DriveSync.default(cfg.drive_root_folder_name)
        else:
            drive_sync = None
        run_loop(cfg, state, aai_client, drive_sync)
    finally:
        lock.__exit__(None, None, None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
