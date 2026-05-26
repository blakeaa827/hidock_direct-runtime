"""Optional transcription bridge: calls diarize_audio's pipeline after offload.

Enabled by `TRANSCRIBE_ON_OFFLOAD=true` (default). When diarize_audio is
unavailable the offload still succeeds — but the operator MUST be told,
via an event on the bus. Silent skips are the shipping-bug pattern this
module exists to prevent.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Optional

from .events import (
    EventBus,
    TranscribeComplete,
    TranscribeFailed,
    TranscribeSkipped,
    TranscribeStarted,
)

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
    return _AVAILABLE


def _run_pipeline(audio_path: Path, archive_dir: Path) -> Any:
    """Build the diarize_audio session and run `process_file` against one audio.

    Factored out as a standalone function so tests can monkeypatch it
    without exercising the real AssemblyAI + Drive stack. Returns the
    `ProcessResult` from diarize_audio.
    """
    from dotenv import load_dotenv

    forge_env = (
        Path(__file__).resolve().parents[2].parent
        / "forge/projects/diarize_audio/secrets/.env"
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
    return process_file(
        wav=audio_path,
        state_key=state_key,
        state=state,
        cfg=cfg,
        aai_client=aai_client,
        drive_sync=drive_sync,
    )


def transcribe_file(
    audio_path: Path,
    archive_dir: Path,
    *,
    bus: EventBus,
    device_filename: str,
) -> Optional[str]:
    """Run the diarize_audio pipeline on a single file.

    Publishes exactly one terminal event — `TranscribeSkipped`,
    `TranscribeComplete`, or `TranscribeFailed` — plus `TranscribeStarted`
    before any real work. Never raises; transcription failure must not
    kill the offload worker.
    """
    if not _check_available():
        bus.publish(
            TranscribeSkipped(
                device_filename=device_filename,
                reason="diarize_audio not importable (check .pth visibility / install)",
            )
        )
        log.warning("transcribe skipped: diarize_audio unavailable")
        return None

    bus.publish(TranscribeStarted(device_filename=device_filename))

    try:
        result = _run_pipeline(audio_path, archive_dir)
    except Exception as exc:
        reason = str(exc) or exc.__class__.__name__
        log.exception("transcribe_file crashed for %s", audio_path.name)
        bus.publish(TranscribeFailed(device_filename=device_filename, reason=reason))
        return None

    status = getattr(result, "status", None)
    drive_file_id = getattr(result, "drive_file_id", None)
    if status == "done":
        bus.publish(
            TranscribeComplete(device_filename=device_filename, drive_file_id=drive_file_id)
        )
        log.info("transcribe ok: %s → drive=%s", audio_path.name, drive_file_id)
        return drive_file_id

    reason = f"pipeline status={status!r}"
    bus.publish(TranscribeFailed(device_filename=device_filename, reason=reason))
    log.warning("transcribe failed: %s → %s", audio_path.name, status)
    return None
