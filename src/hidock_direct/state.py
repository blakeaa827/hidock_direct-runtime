"""`offload_state.json` read/write (atomic, `.bak` fallback) and cumulative
audio minute accounting.

Schema is versioned (`schema_version: 1`). All writes go through `_atomic_write`:
  - snapshot the existing state.json to state.json.bak (one revision)
  - write the new payload to state.json.tmp and fsync
  - rename state.json.tmp -> state.json
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import wave
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Dict, Iterable, Optional


SCHEMA_VERSION = 1


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


def wav_duration_minutes(path: os.PathLike[str] | str) -> float:
    """Return the wall-clock duration of a WAV in minutes.

    Uses the stdlib `wave` module, which reads the RIFF header without
    loading the entire file. Returns 0.0 if the header is unreadable (callers
    should prefer that over crashing; we still commit the file to the
    archive, we just don't increment the cumulative counter).
    """
    try:
        with wave.open(str(path), "rb") as wav:
            frames = wav.getnframes()
            rate = wav.getframerate()
            if rate <= 0:
                return 0.0
            return frames / rate / 60.0
    except (wave.Error, EOFError, FileNotFoundError):
        return 0.0


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
            self._data = self._read_with_fallback()
            return self._data

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
