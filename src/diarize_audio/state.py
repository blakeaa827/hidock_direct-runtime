"""Per-file transcription state persisted to transcribe_state.json.

Atomic write discipline: stage to `.tmp`, replace, keep `.bak`.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

STATE_SCHEMA_VERSION = 1

STATUS_PENDING = "pending"
STATUS_IN_FLIGHT = "in_flight"
STATUS_DONE = "done"
STATUS_ERROR = "error"
STATUS_ERROR_PERMANENT = "error_permanent"


class StateError(RuntimeError):
    """Raised for corrupt or incompatible state files."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def _empty_state() -> dict[str, Any]:
    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "files": {},
        "global": {
            "last_run": None,
            "cumulative_audio_minutes": 0.0,
            "cumulative_aai_jobs": 0,
            "errors_in_last_24h": 0,
        },
    }


class State:
    def __init__(self, path: Path, data: dict[str, Any]):
        self.path = path
        self.data = data

    @classmethod
    def load(cls, path: Path) -> "State":
        path = Path(path)
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            return cls(path, _empty_state())
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise StateError(f"state file {path.name} is not valid JSON") from exc
        version = raw.get("schema_version")
        if version != STATE_SCHEMA_VERSION:
            raise StateError(
                f"state file {path.name} schema_version={version}, expected {STATE_SCHEMA_VERSION}"
            )
        raw.setdefault("files", {})
        raw.setdefault("global", _empty_state()["global"])
        for key in ("last_run", "cumulative_audio_minutes", "cumulative_aai_jobs", "errors_in_last_24h"):
            raw["global"].setdefault(key, _empty_state()["global"][key])
        return cls(path, raw)

    # --- persistence --------------------------------------------------

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            bak = self.path.with_suffix(self.path.suffix + ".bak")
            bak.write_bytes(self.path.read_bytes())
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(self.data, indent=2, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, self.path)

    # --- queries ------------------------------------------------------

    def get(self, key: str) -> dict[str, Any] | None:
        return self.data["files"].get(key)

    def should_skip(self, key: str, in_flight_ttl_minutes: int) -> bool:
        entry = self.get(key)
        if entry is None:
            return False
        status = entry.get("status")
        if status in (STATUS_DONE, STATUS_ERROR_PERMANENT):
            return True
        if status == STATUS_IN_FLIGHT:
            marked = entry.get("marked_in_flight_at")
            if not marked:
                return False
            try:
                ts = datetime.fromisoformat(marked)
            except ValueError:
                return False
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            age = datetime.now(timezone.utc) - ts
            return age < timedelta(minutes=in_flight_ttl_minutes)
        return False

    # --- transitions --------------------------------------------------

    def _entry(self, key: str) -> dict[str, Any]:
        return self.data["files"].setdefault(key, {"retry_count": 0})

    def mark_in_flight(self, key: str) -> None:
        e = self._entry(key)
        e["status"] = STATUS_IN_FLIGHT
        e["marked_in_flight_at"] = _now_iso()
        e.setdefault("retry_count", 0)
        e.setdefault("error", None)

    def mark_done(
        self,
        key: str,
        *,
        assemblyai_id: str,
        audio_duration_seconds: float,
        speaker_count: int,
        drive_file_id: str | None = None,
    ) -> None:
        e = self._entry(key)
        e["status"] = STATUS_DONE
        e["transcribed_at"] = _now_iso()
        e["assemblyai_id"] = assemblyai_id
        e["audio_duration_seconds"] = float(audio_duration_seconds)
        e["speaker_count"] = int(speaker_count)
        e["drive_file_id"] = drive_file_id
        e["drive_uploaded_at"] = _now_iso() if drive_file_id else None
        e["error"] = None

    def mark_error(self, key: str, error: str, *, max_retries: int | None = None) -> None:
        e = self._entry(key)
        e["retry_count"] = int(e.get("retry_count", 0)) + 1
        e["error"] = error
        e["transcribed_at"] = None
        if max_retries is not None and e["retry_count"] >= max_retries:
            e["status"] = STATUS_ERROR_PERMANENT
        else:
            e["status"] = STATUS_ERROR

    def reset_stale_in_flight(self, ttl_minutes: int) -> list[str]:
        reset: list[str] = []
        now = datetime.now(timezone.utc)
        for key, entry in self.data["files"].items():
            if entry.get("status") != STATUS_IN_FLIGHT:
                continue
            marked = entry.get("marked_in_flight_at")
            if not marked:
                continue
            try:
                ts = datetime.fromisoformat(marked)
            except ValueError:
                continue
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if now - ts >= timedelta(minutes=ttl_minutes):
                entry["status"] = STATUS_PENDING
                entry["marked_in_flight_at"] = None
                reset.append(key)
        return reset

    def increment_global(self, audio_seconds: float) -> None:
        g = self.data["global"]
        g["cumulative_audio_minutes"] = round(
            float(g.get("cumulative_audio_minutes", 0.0)) + float(audio_seconds) / 60.0, 4
        )
        g["cumulative_aai_jobs"] = int(g.get("cumulative_aai_jobs", 0)) + 1

    def update_last_run(self) -> None:
        self.data["global"]["last_run"] = _now_iso()
