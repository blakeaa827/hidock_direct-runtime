"""Per-file pipeline: AAI submit → render → Drive sync → state update.

Atomicity invariant: partial `.md` writes are cleaned up on failure.
`.aai.json` is written first (preserves AAI cost spent) and kept on
render/drive failure — a subsequent re-render is cheap; a re-submit is not.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .assemblyai_client import TranscriptionError
from .config import Config
from .render import render_markdown
from .state import State

log = logging.getLogger(__name__)

MAX_UPLOAD_BYTES = 5 * 1024 * 1024 * 1024  # 5 GiB

# Sentinel so `max_retries=None` (meaning "don't escalate") is distinguishable
# from "caller didn't pass it" (meaning "use cfg.max_retries").
_UNSET = object()


@dataclass
class PipelineResult:
    status: str  # "done" | "error"
    error: str | None = None
    drive_file_id: str | None = None


def process_file(
    wav: Path,
    state_key: str,
    state: State,
    cfg: Config,
    aai_client,
    drive_sync,
    *,
    recorded_at: datetime | None = None,
    max_retries: int | None = _UNSET,  # type: ignore[assignment]
    reuse_existing_transcript: bool = False,
) -> PipelineResult:
    """Transcribe one audio file and write its sidecars.

    `recorded_at` is when the RECORDING STARTED. Pass it whenever the caller
    knows it — a device that reports the time, a filename that encodes it —
    because the file's mtime does not: for audio copied off a device, mtime is
    when the copy finished, which is recording-end plus transfer time. Callers
    that genuinely have no better signal (the standalone inbox watcher) may
    omit it and inherit the mtime fallback.

    `max_retries` overrides `cfg.max_retries` for this call's failure bookkeeping.
    Pass `None` for an operator-initiated retry: `mark_error` then increments
    `retry_count` without ever escalating to `error_permanent`, which
    `should_skip` treats as terminal. The budget exists to stop an *unattended*
    sweep from looping forever; a human pressing a key is not that failure mode.

    `reuse_existing_transcript` renders from an already-present `.aai.json`
    instead of submitting again. The AAI response is written before render and
    kept on render/Drive failure precisely so the retry is free (see the module
    docstring); without this flag a post-payment failure costs a second
    transcription of audio already paid for.
    """
    wav = Path(wav)
    fail_max_retries = cfg.max_retries if max_retries is _UNSET else max_retries
    try:
        size = wav.stat().st_size
    except OSError as exc:
        return _fail(state, cfg, state_key, f"failed to stat source: {exc}", fail_max_retries)
    if size > MAX_UPLOAD_BYTES:
        return _fail(
            state, cfg, state_key,
            f"source file is too large ({size} bytes > {MAX_UPLOAD_BYTES})",
            fail_max_retries,
        )

    state.mark_in_flight(state_key)
    state.save()
    log.info("in_flight", extra={"key": state_key})

    aai_json_path = wav.parent / f"{wav.stem}.aai.json"
    md_path = wav.parent / f"{wav.stem}.md"

    # --- AAI ---
    # `billed` is False only on the reuse path, where the transcript came off
    # disk: the AAI job was already paid for and counted when it was written, so
    # the lifetime counters below must not tick again.
    transcript = _load_cached_transcript(aai_json_path) if reuse_existing_transcript else None
    billed = transcript is None
    if transcript is not None:
        log.info("reusing cached transcript", extra={"key": state_key})
    else:
        # Invariant: once mark_in_flight is persisted (above), NO exception path may
        # leave this entry `in_flight`. TranscriptionError/TimeoutError are the
        # expected failures; the catch-all is defense in depth so an unanticipated
        # exception (SDK/transport/anything) still transitions to a terminal `error`
        # state rather than stranding the file. See
        # bug_report_aai_exception_leak_strands_in_flight.md.
        try:
            transcript = aai_client.transcribe_file(wav)
        except TranscriptionError as exc:
            return _fail(state, cfg, state_key, str(exc), fail_max_retries)
        except TimeoutError as exc:
            return _fail(state, cfg, state_key, f"aai timeout: {exc}", fail_max_retries)
        except Exception as exc:
            return _fail(
                state, cfg, state_key,
                f"unexpected error during transcription: {exc}", fail_max_retries,
            )

        # Persist raw response (atomic write, 0600)
        try:
            _atomic_write_bytes(
                aai_json_path,
                (json.dumps(transcript, indent=2, ensure_ascii=False) + "\n").encode("utf-8"),
                mode=0o600,
            )
        except OSError as exc:
            return _fail(
                state, cfg, state_key, f"failed to persist .aai.json: {exc}", fail_max_retries,
            )

    # --- Render ---
    try:
        if recorded_at is None:
            # No caller-supplied recording time — the standalone inbox-watcher
            # case, where mtime is the only signal available.
            recorded_at = datetime.fromtimestamp(wav.stat().st_mtime)
        if recorded_at.tzinfo is None:
            # Devices report local wall-clock with no zone. Normalize here, at
            # the single choke point feeding the renderer, so `isoformat()`
            # cannot silently emit an offset-less frontmatter value.
            recorded_at = recorded_at.astimezone()
        md_text = render_markdown(
            transcript,
            source_filename=wav.name,
            recorded_at=recorded_at,
        )
    except Exception as exc:
        # Clean up half-written .md if any (render itself returns a full string,
        # so nothing to clean — the atomic writer handles it, but we guard just in case).
        if md_path.exists():
            try:
                md_path.unlink()
            except OSError:
                pass
        return _fail(state, cfg, state_key, f"render failed: {exc}", fail_max_retries)

    try:
        _atomic_write_bytes(md_path, md_text.encode("utf-8"), mode=0o600)
    except OSError as exc:
        return _fail(state, cfg, state_key, f"failed to persist .md: {exc}", fail_max_retries)

    # --- Drive ---
    drive_file_id: str | None = None
    if cfg.drive_enabled:
        rel = _relpath_for(wav, cfg.inbox_dirs)
        year, month = _year_month_from(rel, recorded_at)
        try:
            md_up = drive_sync.upload(md_path, year=year, month=month)
            drive_file_id = md_up.get("id")
            if cfg.upload_raw_audio_to_drive:
                drive_sync.upload(wav, year=year, month=month)
        except Exception as exc:
            return _fail(state, cfg, state_key, f"drive upload failed: {exc}", fail_max_retries)

    speaker_count = _count_speakers(transcript)
    audio_seconds = float(transcript.get("audio_duration") or 0.0)

    state.mark_done(
        state_key,
        assemblyai_id=str(transcript.get("id") or ""),
        audio_duration_seconds=audio_seconds,
        speaker_count=speaker_count,
        drive_file_id=drive_file_id,
    )
    if billed:
        state.increment_global(audio_seconds=audio_seconds)
    state.save()
    log.info("complete", extra={"key": state_key, "aai_id": transcript.get("id")})
    return PipelineResult(status="done", drive_file_id=drive_file_id)


# --- helpers ------------------------------------------------------------


def _load_cached_transcript(aai_json_path: Path) -> dict | None:
    """Return the transcript from a previously-written `.aai.json`, or None.

    None means "no usable cache — submit normally". Absent, empty, unreadable and
    malformed all collapse to that, because the fallback is correct in every case
    and silently producing nothing would be worse than paying again.
    """
    try:
        raw = aai_json_path.read_text()
    except OSError:
        return None
    if not raw.strip():
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _fail(
    state: State, cfg: Config, key: str, error: str, max_retries: int | None = _UNSET
) -> PipelineResult:  # type: ignore[assignment]
    resolved = cfg.max_retries if max_retries is _UNSET else max_retries
    state.mark_error(key, error, max_retries=resolved)
    state.save()
    log.warning("error", extra={"key": key, "error": error})
    return PipelineResult(status="error", error=error)


def _atomic_write_bytes(path: Path, data: bytes, *, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    # Create tmp with restrictive mode from the start
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    with os.fdopen(fd, "wb") as f:
        f.write(data)
    os.replace(tmp, path)
    try:
        os.chmod(path, mode)
    except OSError:
        pass


def _count_speakers(transcript: dict[str, Any]) -> int:
    utts = transcript.get("utterances") or []
    return len({u.get("speaker") for u in utts if u.get("speaker")})


def _relpath_for(wav: Path, inbox_dirs) -> Path:
    resolved = wav.resolve()
    for root in inbox_dirs:
        try:
            return resolved.relative_to(Path(root).expanduser().resolve())
        except ValueError:
            continue
    return Path(wav.name)


def _year_month_from(rel: Path, fallback: datetime) -> tuple[str, str]:
    parts = rel.parts
    if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
        return parts[0], parts[1]
    return f"{fallback.year:04d}", f"{fallback.month:02d}"
