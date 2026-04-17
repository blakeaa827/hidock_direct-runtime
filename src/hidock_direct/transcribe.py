"""Optional transcription bridge: calls diarize_audio's pipeline after offload.

Enabled by `TRANSCRIBE_ON_OFFLOAD=true` (default). Degrades silently if
diarize_audio isn't installed in the venv — the offload still succeeds,
you just don't get a transcript.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

_AVAILABLE: Optional[bool] = None


def _check_available() -> bool:
    global _AVAILABLE
    if _AVAILABLE is not None:
        return _AVAILABLE
    try:
        import diarize_audio  # noqa: F401
        _AVAILABLE = True
    except ImportError:
        _AVAILABLE = False
        log.warning("diarize_audio not installed — TRANSCRIBE_ON_OFFLOAD disabled")
    return _AVAILABLE


def transcribe_file(audio_path: Path, archive_dir: Path) -> Optional[str]:
    """Run the diarize_audio pipeline on a single file.

    Returns the Drive file ID on success, None on failure or if unavailable.
    Exceptions are caught and logged — transcription failure must never
    block the offload pipeline.
    """
    if not _check_available():
        return None

    try:
        from dotenv import load_dotenv

        forge_env = Path.home() / (
            "Library/Mobile Documents/com~apple~CloudDocs/Claude/"
            "forge/projects/diarize_audio/secrets/.env"
        )
        if forge_env.exists():
            load_dotenv(forge_env, override=False)

        from diarize_audio.config import Config
        from diarize_audio.state import State
        from diarize_audio.assemblyai_client import AAIClient
        from diarize_audio.drive_sync import DriveSync
        from diarize_audio.pipeline import process_file

        os.environ.setdefault("INBOX_DIRS", str(archive_dir))
        cfg = Config.from_env()
        cfg.state_dir.mkdir(parents=True, exist_ok=True)
        state = State.load(cfg.state_path)

        aai_client = AAIClient(
            api_key=cfg.assemblyai_api_key,
            speech_model=cfg.speech_model,
            language_code=cfg.language_code,
            word_boost=cfg.word_boost,
            timeout_minutes=cfg.aai_timeout_minutes,
        )

        drive_sync = None
        if cfg.drive_enabled:
            drive_sync = DriveSync.default(cfg.drive_root_folder_name)

        state_key = str(audio_path.relative_to(archive_dir))
        result = process_file(
            wav=audio_path,
            state_key=state_key,
            state=state,
            cfg=cfg,
            aai_client=aai_client,
            drive_sync=drive_sync,
        )

        if result.status == "done":
            log.info("transcribe ok: %s → drive=%s", audio_path.name, result.drive_file_id)
            return result.drive_file_id
        else:
            log.warning("transcribe failed: %s → %s", audio_path.name, result.status)
            return None

    except Exception:
        log.exception("transcribe_file crashed for %s", audio_path.name)
        return None
