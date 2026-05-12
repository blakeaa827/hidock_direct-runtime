"""`offload_state.json` read/write (atomic, `.bak` fallback) and cumulative
audio minute accounting.

Schema is versioned (`schema_version: 2`). All writes go through `_atomic_write`:
  - snapshot the existing state.json to state.json.bak (one revision)
  - write the new payload to state.json.tmp and fsync
  - rename state.json.tmp -> state.json

v1 -> v2 migration: v2 adds a per-entry `kind` field (`meeting`/`whisper`/
`unknown`) so the offload sweep can leave whispers on device. On first load
after upgrade, v1 entries are auto-rewritten as `kind: "meeting"` (every
pre-existing offload predates the whispers feature and landed in the meetings
archive root). The v1 payload is retained as the `.bak` revision.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from contextlib import contextmanager

import mutagen
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Dict, Iterable, Optional


from .classify import RecordingKind


SCHEMA_VERSION = 2


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def default_schema() -> Dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "devices": {},
        "global": {
            "last_run": None,
            "cumulative_audio_minutes": 0.0,
        },
    }


def audio_duration_minutes(path: os.PathLike[str] | str) -> float:
    """Return the wall-clock duration of an audio file in minutes.

    Handles both WAV (HTA-converted from H1 model) and MP3 (P1 model native)
    via mutagen, which reads container headers without loading the audio
    body. Dispatches on file content (mutagen.File factory), not extension,
    so a mislabeled extension still resolves correctly.

    Returns 0.0 if the header is unreadable or the file is missing (callers
    should prefer that over crashing; we still commit the file to the
    archive, we just don't increment the cumulative counter). OSError /
    PermissionError propagate — disk and permission failures surface
    through the offload pipeline's existing error path rather than
    silently becoming a 0.0 metric.
    """
    try:
        audio = mutagen.File(str(path))
    except mutagen.MutagenError as exc:
        # Mutagen wraps everything in MutagenError: missing file =>
        # MutagenError(FileNotFoundError), permission denied =>
        # MutagenError(PermissionError), malformed header =>
        # HeaderNotFoundError(<message>). Unwrap: PermissionError and other
        # disk-IO errors (OSError except FileNotFoundError) must propagate
        # so disk/permission failures surface through the offload pipeline's
        # existing error path rather than silently becoming 0.0.
        inner = exc.args[0] if exc.args else None
        if isinstance(inner, OSError) and not isinstance(inner, FileNotFoundError):
            raise inner
        return 0.0
    if audio is None or audio.info is None:
        return 0.0
    length = getattr(audio.info, "length", 0.0)
    if not length or length <= 0:
        return 0.0
    return float(length) / 60.0


@dataclass
class DeviceKey:
    model: str
    serial: str

    @property
    def namespaced(self) -> str:
        """e.g. `HiDock-H1-SN12345678`. Stable across runs so the file ledger is keyed consistently."""
        clean_model = self.model.replace(" ", "-")
        return f"{clean_model}-{self.serial}"


class StateStore:
    """Persistent ledger of what's been offloaded from which device.

    Thread-safe: every public operation holds a re-entrant lock. On disk, we
    only keep one previous revision (.bak) — enough to recover from a corrupt
    write, but not a full history. Git / session logs are the journal.
    """

    def __init__(self, path: os.PathLike[str] | str):
        self._path = Path(path)
        self._lock = RLock()
        self._data: Optional[Dict[str, Any]] = None

    @property
    def path(self) -> Path:
        return self._path

    @property
    def bak_path(self) -> Path:
        return self._path.with_suffix(self._path.suffix + ".bak")

    def load(self) -> Dict[str, Any]:
        with self._lock:
            raw = self._read_with_fallback()
            migrated, changed = self._migrate_if_needed(raw)
            if changed:
                # `_atomic_write` snapshots the existing on-disk file to .bak
                # before rewriting, so the v1 payload is retained unmodified.
                self._atomic_write(migrated)
            self._data = migrated
            return self._data

    @staticmethod
    def _migrate_if_needed(data: Dict[str, Any]) -> tuple[Dict[str, Any], bool]:
        """Apply forward migrations in place. Return `(data, changed)`."""
        version = int(data.get("schema_version", 1))
        if version >= SCHEMA_VERSION:
            return data, False
        # v1 -> v2: stamp every processed recording with kind="meeting".
        for device in data.get("devices", {}).values():
            for entry in device.get("processed_recordings", {}).values():
                if "kind" not in entry:
                    entry["kind"] = RecordingKind.MEETING.value
        data["schema_version"] = SCHEMA_VERSION
        return data, True

    def _read_with_fallback(self) -> Dict[str, Any]:
        for candidate in (self._path, self.bak_path):
            if not candidate.exists():
                continue
            try:
                with candidate.open("r", encoding="utf-8") as fh:
                    return json.load(fh)
            except (json.JSONDecodeError, OSError):
                continue
        return default_schema()

    def _atomic_write(self, payload: Dict[str, Any]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        if self._path.exists():
            try:
                shutil.copy2(self._path, self.bak_path)
            except OSError:
                pass
        fd, tmp_path = tempfile.mkstemp(
            prefix=self._path.name + ".",
            suffix=".tmp",
            dir=self._path.parent,
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, sort_keys=True)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, self._path)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass
            raise

    @contextmanager
    def _mutate(self):
        with self._lock:
            if self._data is None:
                self._data = self._read_with_fallback()
            yield self._data
            self._atomic_write(self._data)

    def register_device(self, key: DeviceKey, *, first_seen: Optional[str] = None) -> None:
        with self._mutate() as data:
            ns = key.namespaced
            entry = data["devices"].setdefault(
                ns,
                {
                    "device_model": key.model,
                    "serial": key.serial,
                    "first_seen": first_seen or _utcnow_iso(),
                    "processed_recordings": {},
                },
            )
            entry["device_model"] = key.model
            entry["serial"] = key.serial

    def is_processed(self, key: DeviceKey, device_filename: str) -> bool:
        with self._lock:
            if self._data is None:
                self._data = self._read_with_fallback()
            device = self._data["devices"].get(key.namespaced)
            return bool(device and device_filename in device.get("processed_recordings", {}))

    def kind_for(self, key: DeviceKey, device_filename: str) -> Optional[RecordingKind]:
        """Return the recorded `kind` for a processed file, or None if unseen."""
        with self._lock:
            if self._data is None:
                self._data = self._read_with_fallback()
            device = self._data["devices"].get(key.namespaced)
            if not device:
                return None
            entry = device.get("processed_recordings", {}).get(device_filename)
            if entry is None:
                return None
            raw = entry.get("kind")
            if raw is None:
                return None
            try:
                return RecordingKind(raw)
            except ValueError:
                return None

    def processed_names(self, key: DeviceKey) -> Iterable[str]:
        with self._lock:
            if self._data is None:
                self._data = self._read_with_fallback()
            device = self._data["devices"].get(key.namespaced, {})
            return tuple(device.get("processed_recordings", {}).keys())

    def record_processed(
        self,
        key: DeviceKey,
        *,
        device_filename: str,
        size_bytes: int,
        device_mtime: Optional[str],
        sha256: str,
        archive_path: str,
        device_deleted: bool,
        duration_minutes: float,
        kind: RecordingKind = RecordingKind.MEETING,
    ) -> None:
        """Append a successful offload entry and bump cumulative minutes."""
        with self._mutate() as data:
            ns = key.namespaced
            entry = data["devices"].setdefault(
                ns,
                {
                    "device_model": key.model,
                    "serial": key.serial,
                    "first_seen": _utcnow_iso(),
                    "processed_recordings": {},
                },
            )
            entry["device_model"] = key.model
            entry["serial"] = key.serial
            entry["processed_recordings"][device_filename] = {
                "size_bytes": int(size_bytes),
                "device_mtime": device_mtime,
                "sha256": sha256,
                "downloaded_at": _utcnow_iso(),
                "archive_path": archive_path,
                "device_deleted": bool(device_deleted),
                "kind": RecordingKind(kind).value,
            }
            data["global"]["last_run"] = _utcnow_iso()
            current = float(data["global"].get("cumulative_audio_minutes") or 0.0)
            data["global"]["cumulative_audio_minutes"] = round(current + duration_minutes, 3)

    def mark_touched(self) -> None:
        """Update `global.last_run` without recording a file."""
        with self._mutate() as data:
            data["global"]["last_run"] = _utcnow_iso()

    def snapshot(self) -> Dict[str, Any]:
        """Return a deep-enough copy for read-only callers (TUI, tests)."""
        with self._lock:
            if self._data is None:
                self._data = self._read_with_fallback()
            return json.loads(json.dumps(self._data))
