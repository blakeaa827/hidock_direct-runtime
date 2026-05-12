"""In-memory HiDock simulator.

Conforms to `hidock_direct.device.DeviceAdapter`. Tests construct one with a
scripted set of `DeviceFile` + content bytes, wire it into `Offloader` /
`App`, and drive scenarios (happy path, size-growing file, mid-transfer
cancel, `.hda` source, etc.) without touching USB.
"""

from __future__ import annotations

import threading
import wave
from dataclasses import dataclass, field
from datetime import datetime
from io import BytesIO
from typing import Callable, Dict, List, Optional

from hidock_direct.device import (
    DeviceFile,
    DeviceInfo,
    DeviceNotConnected,
    TransferAborted,
)


def make_wav_bytes(*, duration_seconds: float = 1.0, sample_rate: int = 16000) -> bytes:
    """Produce a valid little-endian PCM WAV of the given duration.

    Uses the stdlib `wave` module so the header is canonical — `state.audio_duration_minutes`
    can read back the exact duration via mutagen.
    """
    buf = BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(sample_rate)
        nframes = max(1, int(sample_rate * duration_seconds))
        w.writeframes(b"\x00\x00" * nframes)
    return buf.getvalue()


@dataclass
class MockFile:
    """A single mock recording on the device.

    Historical note: an earlier `pending_size_sequence` field simulated
    growing-size polls for the size-stability check. Removed on 2026-04-23
    after hardware verification confirmed Jensen `list_files` only exposes
    completed recordings (see `project_hidock_list_files_hides_in_progress`
    memory entry). Size is now always the final content length.
    """

    name: str
    content: bytes
    device_mtime: Optional[datetime] = None
    # Override for tests that need to simulate a pathological size (e.g.,
    # oversized-file rejection) without allocating gigabytes of content.
    size_override: Optional[int] = None

    def report_size(self) -> int:
        if self.size_override is not None:
            return self.size_override
        return len(self.content)


class MockDevice:
    """DeviceAdapter implementation backed by a Python dict of files."""

    def __init__(
        self,
        *,
        model: str = "hidock-h1",
        serial: str = "SN-TEST-1234",
        files: Optional[List[MockFile]] = None,
        chunk_size: int = 65536,
    ):
        self._model = model
        self._serial = serial
        self._files: Dict[str, MockFile] = {f.name: f for f in (files or [])}
        self._chunk_size = max(1, chunk_size)
        self._connected = False
        # Hook the test can set to raise mid-transfer. Called once per chunk
        # before writing; if it raises, the simulated stream aborts with a
        # `TransferAborted`.
        self.raise_mid_transfer_after_bytes: Optional[int] = None

    # -- file management (test helpers, not part of the adapter API) ---

    def add_file(self, file: MockFile) -> None:
        self._files[file.name] = file

    def list_raw(self) -> List[MockFile]:
        return list(self._files.values())

    # -- adapter surface -----------------------------------------------

    def connect(self) -> DeviceInfo:
        self._connected = True
        return DeviceInfo(model=self._model, serial=self._serial)

    def disconnect(self) -> None:
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected

    def list_files(self) -> List[DeviceFile]:
        if not self._connected:
            raise DeviceNotConnected()
        out: List[DeviceFile] = []
        for f in self._files.values():
            out.append(DeviceFile(name=f.name, size=f.report_size(), device_mtime=f.device_mtime))
        return out

    def get_file_count(self) -> int:
        if not self._connected:
            raise DeviceNotConnected()
        return len(self._files)

    def download_file(
        self,
        name: str,
        size: int,
        *,
        on_chunk: Callable[[bytes], None],
        on_progress: Optional[Callable[[int, int], None]] = None,
        cancel_event: Optional[threading.Event] = None,
    ) -> None:
        if not self._connected:
            raise DeviceNotConnected()
        f = self._files.get(name)
        if f is None:
            raise TransferAborted(f"not on device: {name}")
        data = f.content
        total = len(data)
        sent = 0
        abort_threshold = self.raise_mid_transfer_after_bytes
        while sent < total:
            if cancel_event is not None and cancel_event.is_set():
                raise TransferAborted("cancelled")
            if abort_threshold is not None and sent >= abort_threshold:
                raise TransferAborted("simulated mid-transfer abort")
            chunk = data[sent : sent + self._chunk_size]
            on_chunk(chunk)
            sent += len(chunk)
            if on_progress is not None:
                on_progress(sent, total)

    def delete_file(self, name: str) -> None:
        if not self._connected:
            raise DeviceNotConnected()
        self._files.pop(name, None)
