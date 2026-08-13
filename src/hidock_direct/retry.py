"""Operator-initiated retry of failed transcriptions.

The offload path transcribes as a side effect of a successful download. When
that fails — most often because the prepaid AssemblyAI balance ran out, which is
normal operation for an account with spend caps — the recording sits in the
archive with a terminal `error` in the transcription ledger and nothing re-runs
it. This module is what the `r` key drives.

Two invariants, both learned the expensive way (see
`transcription_retry_prd.md` v3 §3):

1. **Operator retries never consume the automatic retry budget.** `mark_error`
   escalates to `error_permanent` at `MAX_RETRIES`, and `should_skip` treats
   that as terminal forever. Three presses against a dead balance would bury the
   very files this exists to recover, so every call passes `max_retries=None`.
2. **A transcript already paid for is re-rendered, never re-purchased.**
   `.aai.json` is written before render precisely so a downstream failure is
   cheap to retry; without `reuse_existing_transcript` those retries would bill
   a second time for audio already transcribed.

This module holds no `State` across files. `_run_pipeline` loads its own per
call and `State.save()` is a whole-file overwrite, so a long-lived snapshot
written back would revert everything the batch had already accomplished.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable, Optional

from .offload import _recorded_at_from_basename
from .state import audio_duration_minutes

log = logging.getLogger(__name__)

# Ledger statuses. Mirrors diarize_audio.state; duplicated rather than imported
# so this module does not depend on vendored internals for constants.
_STATUS_ERROR = "error"
_STATUS_ERROR_PERMANENT = "error_permanent"
_STATUS_IN_FLIGHT = "in_flight"

# Substrings that mean "retrying will fail identically until the operator does
# something." Matched against the ledger's `error` text, which is where
# `_fail` persists the real AssemblyAI message — `transcribe_file` discards it
# and publishes only `pipeline status='error'`.
#
# Each entry records the text as observed and when. Verified against the live
# API 2026-07-30 and against production ledger entries 2026-08-12.
_OPERATOR_ACTIONABLE_MARKERS = (
    "balance is negative",           # 2026-08-11/12, 12 production entries
    "Please top up",                 # same message, second clause
    "401",                           # invalid / revoked key
    "Unauthorized",
    "ConfigError",                   # retired model ID, caught at startup
)

# Failures the audio itself causes. Retrying re-uploads and fails identically.
_PERMANENT_DATA_MARKERS = (
    "no spoken audio",               # 2026-04, 6 production entries
    "source file is too large",      # pipeline.py MAX_UPLOAD_BYTES
    "failed to stat source",         # file vanished
)

CLASS_OPERATOR_ACTIONABLE = "operator_actionable"
CLASS_PERMANENT_DATA = "permanent_data"
CLASS_TRANSIENT = "transient"


def classify_failure(error_text: Optional[str]) -> str:
    """Bucket a ledger `error` string.

    Unmatched text is `transient` on purpose: retrying once costs a few seconds,
    while terminalizing a novel failure would silently strand it.
    """
    text = error_text or ""
    for marker in _OPERATOR_ACTIONABLE_MARKERS:
        if marker in text:
            return CLASS_OPERATOR_ACTIONABLE
    for marker in _PERMANENT_DATA_MARKERS:
        if marker in text:
            return CLASS_PERMANENT_DATA
    return CLASS_TRANSIENT


@dataclass(frozen=True)
class RetryCandidate:
    state_key: str
    path: Path
    exists_on_disk: bool
    has_paid_transcript: bool
    status: str
    retry_count: int
    error: str
    error_class: str
    recorded_at: Optional[datetime]
    duration_minutes: float

    @property
    def default_selected(self) -> bool:
        """Rows retrying can actually help, and only those.

        Everything else is listed and counted — an operator can force it — but
        is never swept in by the default confirm.
        """
        if not self.exists_on_disk:
            return False
        if self.status == _STATUS_ERROR_PERMANENT:
            return False
        if self.error_class == CLASS_PERMANENT_DATA:
            return False
        return True

    @property
    def exclusion_reason(self) -> Optional[str]:
        if not self.exists_on_disk:
            return "file missing from archive"
        if self.status == _STATUS_ERROR_PERMANENT:
            return "retry budget exhausted"
        if self.error_class == CLASS_PERMANENT_DATA:
            return "retrying will not help"
        return None


class LedgerUnavailable(RuntimeError):
    """The archive or its ledger could not be read.

    Distinct from "no candidates" on purpose: a Drive mount that has not
    materialized yet otherwise renders as a confident all-clear.
    """


def find_retry_candidates(
    state, archive_dir: Path, *, in_flight_ttl_minutes: int = 60
) -> list[RetryCandidate]:
    """Every ledger entry an operator could reasonably re-run.

    `state` is a `diarize_audio.state.State`. Read-only.
    """
    archive_dir = Path(archive_dir)
    if not archive_dir.is_dir():
        raise LedgerUnavailable(f"archive directory not found: {archive_dir}")

    files = state.data.get("files", {})
    now = datetime.now().astimezone()
    out: list[RetryCandidate] = []

    for key, entry in sorted(files.items()):
        if not isinstance(entry, dict):
            continue
        status = entry.get("status")
        if status in (_STATUS_ERROR, _STATUS_ERROR_PERMANENT):
            pass
        elif status == _STATUS_IN_FLIGHT and _is_stale(entry, now, in_flight_ttl_minutes):
            pass
        else:
            continue

        path = archive_dir / key
        exists = path.is_file()
        error = entry.get("error") or ""
        out.append(
            RetryCandidate(
                state_key=key,
                path=path,
                exists_on_disk=exists,
                has_paid_transcript=_has_paid_transcript(path) if exists else False,
                status=str(status),
                retry_count=int(entry.get("retry_count") or 0),
                error=error,
                error_class=classify_failure(error),
                recorded_at=_recorded_at_from_basename(path),
                duration_minutes=audio_duration_minutes(path) if exists else 0.0,
            )
        )
    return out


def _is_stale(entry: dict, now: datetime, ttl_minutes: int) -> bool:
    marked = entry.get("marked_in_flight_at")
    if not marked:
        return True  # no timestamp to trust — treat as recoverable
    try:
        ts = datetime.fromisoformat(marked)
    except ValueError:
        return True
    if ts.tzinfo is None:
        ts = ts.astimezone()
    return (now - ts).total_seconds() >= ttl_minutes * 60


def _has_paid_transcript(path: Path) -> bool:
    sidecar = path.parent / f"{path.stem}.aai.json"
    try:
        return sidecar.is_file() and sidecar.stat().st_size > 0
    except OSError:
        return False


@dataclass
class RetryOutcome:
    succeeded: int = 0
    re_rendered: int = 0
    failed: int = 0
    not_attempted: int = 0
    aborted_reason: Optional[str] = None
    failures: list[tuple[str, str]] = field(default_factory=list)

    @property
    def attempted(self) -> int:
        return self.succeeded + self.re_rendered + self.failed


def run_retry_batch(
    candidates: Iterable[RetryCandidate],
    archive_dir: Path,
    *,
    bus,
    transcribe: Optional[Callable] = None,
    load_entry: Optional[Callable[[str], dict]] = None,
    consecutive_failure_limit: int = 2,
) -> RetryOutcome:
    """Re-run each candidate, stopping early when continuing is pointless.

    `transcribe` and `load_entry` are injection seams for tests; production uses
    the real bridge and ledger.
    """
    from . import transcribe as transcribe_mod

    transcribe = transcribe or transcribe_mod.transcribe_file
    load_entry = load_entry or _default_load_entry(archive_dir)

    items = list(candidates)
    outcome = RetryOutcome()
    consecutive = 0

    for index, cand in enumerate(items):
        if not cand.exists_on_disk:
            outcome.not_attempted += 1
            continue

        transcribe(
            cand.path,
            Path(archive_dir),
            bus=bus,
            device_filename=cand.path.name,
            recorded_at=None,
            max_retries=None,
            reuse_existing_transcript=cand.has_paid_transcript,
        )

        entry = load_entry(cand.state_key) or {}
        if entry.get("status") == "done":
            consecutive = 0
            if cand.has_paid_transcript:
                outcome.re_rendered += 1
            else:
                outcome.succeeded += 1
            continue

        outcome.failed += 1
        consecutive += 1
        error = entry.get("error") or ""
        outcome.failures.append((cand.state_key, error))
        klass = classify_failure(error)

        if klass == CLASS_OPERATOR_ACTIONABLE:
            outcome.aborted_reason = _remediation(error)
        elif consecutive >= consecutive_failure_limit:
            outcome.aborted_reason = (
                f"{consecutive} failures in a row — stopping rather than working through "
                f"the rest. Last error: {redact(error)}"
            )

        if outcome.aborted_reason:
            outcome.not_attempted += len(items) - index - 1
            log.warning("retry batch aborted", extra={"reason": outcome.aborted_reason})
            break

    return outcome


def _default_load_entry(archive_dir: Path) -> Callable[[str], dict]:
    def _load(state_key: str) -> dict:
        from diarize_audio.config import Config
        from diarize_audio.state import State

        cfg = Config.from_env()
        return State.load(cfg.state_path).data.get("files", {}).get(state_key, {})

    return _load


def _remediation(error: str) -> str:
    if "balance is negative" in error or "Please top up" in error:
        return (
            "AssemblyAI balance is negative — top up at assemblyai.com, then press r again. "
            "Nothing was billed."
        )
    if "401" in error or "Unauthorized" in error:
        return "AssemblyAI rejected the API key — check ASSEMBLYAI_API_KEY in .env."
    if "ConfigError" in error:
        return f"Configuration problem: {redact(error)}"
    return redact(error)


_UPLOAD_URL_PREFIX = "https://cdn.assemblyai.com/upload/"


def redact(text: str) -> str:
    """Strip account-scoped upload URLs from anything shown to the operator.

    The operator screenshots this app when reporting problems and the repo is
    public; the full text stays available in the log at DEBUG.
    """
    out: list[str] = []
    for token in (text or "").split(" "):
        out.append("<upload-url redacted>" if token.startswith(_UPLOAD_URL_PREFIX) else token)
    return " ".join(out)


def summarize(candidates: Iterable[RetryCandidate]) -> dict:
    """Numbers for the confirm prompt."""
    items = list(candidates)
    selected = [c for c in items if c.default_selected]
    to_transcribe = [c for c in selected if not c.has_paid_transcript]
    re_render = [c for c in selected if c.has_paid_transcript]
    excluded: dict[str, int] = {}
    for c in items:
        reason = c.exclusion_reason
        if reason:
            excluded[reason] = excluded.get(reason, 0) + 1
    hours = sum(c.duration_minutes for c in to_transcribe) / 60.0
    known = [c for c in to_transcribe if c.duration_minutes > 0]
    return {
        "total": len(items),
        "selected": len(selected),
        "to_transcribe": len(to_transcribe),
        "re_render": len(re_render),
        "hours": hours,
        "duration_known": len(known) == len(to_transcribe),
        "excluded": excluded,
    }
