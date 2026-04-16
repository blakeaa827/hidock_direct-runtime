"""PRD §5.1 state-machine tests.

The app is driven via a `FakeWatcher` that fires attach/detach on command,
and the offloader runs against the in-memory `MockDevice`. This gives us a
full state trajectory without a real HiDock.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime
from pathlib import Path
from typing import List

import pytest

from hidock_direct.app import App, AppState
from hidock_direct.events import (
    DeviceAttached,
    DeviceDetached,
    Event,
    EventBus,
    TransferAborted,
)
from hidock_direct.offload import Offloader
from hidock_direct.state import StateStore
from hidock_direct.usb_watcher import AttachCallback, DetachCallback

from tests.fixtures.mock_device import MockDevice, MockFile, make_wav_bytes


class FakeWatcher:
    def __init__(self):
        self._attach: List[AttachCallback] = []
        self._detach: List[DetachCallback] = []
        self.started = False
        self.stopped = False

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.stopped = True

    def on_attach(self, fn: AttachCallback) -> None:
        self._attach.append(fn)

    def on_detach(self, fn: DetachCallback) -> None:
        self._detach.append(fn)

    def fire_attach(self, vid: int = 0x10D6, pid: int = 0xAF0C) -> None:
        for fn in self._attach:
            fn(vid, pid)

    def fire_detach(self, vid: int = 0x10D6, pid: int = 0xAF0C) -> None:
        for fn in self._detach:
            fn(vid, pid)


def _wait_for_state(app: App, state: AppState, timeout: float = 3.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if app.state == state:
            return
        time.sleep(0.02)
    raise AssertionError(f"state never reached {state}; last state={app.state}")


def _wait_until(predicate, timeout: float = 3.0, message: str = "predicate never true") -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(message)


def _build_app(tmp_path: Path, *, files: List[MockFile] | None = None):
    archive = tmp_path / "arch"
    (archive / ".state").mkdir(parents=True, exist_ok=True)
    (archive / ".tmp").mkdir(parents=True, exist_ok=True)
    bus = EventBus()
    events: list[Event] = []
    bus.subscribe(events.append)
    store = StateStore(archive / ".state" / "offload_state.json")
    mock = MockDevice(files=files or [])
    watcher = FakeWatcher()
    offloader = Offloader(
        adapter=mock,
        store=store,
        bus=bus,
        archive_dir=archive,
        tmp_dir=archive / ".tmp",
        delete_after_offload=False,
        sleep=lambda *_a, **_k: None,
    )
    app = App(
        adapter=mock,
        watcher=watcher,
        offloader=offloader,
        store=store,
        bus=bus,
        poll_interval_seconds=1,
        sleep=lambda *_a, **_k: None,
    )
    return app, bus, events, mock, watcher, archive


def test_state_machine_attach_detach(tmp_path: Path):
    files = [MockFile(name="REC_M.wav", content=make_wav_bytes(), device_mtime=datetime(2026, 4, 12, 10, 0, 0))]
    app, bus, events, mock, watcher, archive = _build_app(tmp_path, files=files)
    runner = threading.Thread(target=app.run, daemon=True)
    runner.start()
    try:
        _wait_until(lambda: watcher.started, message="watcher never started")
        watcher.fire_attach()
        # App walks CONNECTED_IDLE -> SCANNING -> DRAINING -> CONNECTED_IDLE.
        _wait_for_state(app, AppState.CONNECTED_IDLE, timeout=5.0)
        assert any(isinstance(e, DeviceAttached) for e in events)
        archive_wav = list((archive).rglob("*.wav"))
        assert archive_wav, "happy-path scan should have produced a .wav"
        watcher.fire_detach()
        _wait_for_state(app, AppState.IDLE_DISCONNECTED, timeout=3.0)
        assert any(isinstance(e, DeviceDetached) for e in events)
    finally:
        app.stop()
        runner.join(timeout=3.0)


def test_state_machine_detach_mid_drain(tmp_path: Path):
    # Large file so that the drain takes long enough for detach to hit mid-stream.
    big_content = make_wav_bytes(duration_seconds=20.0)
    files = [MockFile(name="REC_BIG.wav", content=big_content)]
    app, bus, events, mock, watcher, archive = _build_app(tmp_path, files=files)

    # Slow the mock's chunk loop so detach has time to land mid-stream.
    mock._chunk_size = 1024  # type: ignore[attr-defined]
    original_download = mock.download_file

    def slow_download(*args, **kwargs):
        real_on_chunk = kwargs.get("on_chunk")

        def pacing_on_chunk(chunk):
            if real_on_chunk is not None:
                real_on_chunk(chunk)
            time.sleep(0.005)

        kwargs["on_chunk"] = pacing_on_chunk
        return original_download(*args, **kwargs)

    mock.download_file = slow_download  # type: ignore[method-assign]

    runner = threading.Thread(target=app.run, daemon=True)
    runner.start()
    try:
        _wait_until(lambda: watcher.started, message="watcher never started")
        watcher.fire_attach()
        _wait_for_state(app, AppState.DRAINING, timeout=5.0)
        # Detach mid-drain.
        watcher.fire_detach()
        _wait_for_state(app, AppState.IDLE_DISCONNECTED, timeout=5.0)
        aborted = [e for e in events if isinstance(e, TransferAborted)]
        assert aborted, "TransferAborted should fire on mid-drain detach"
        # No partial + no archive wav (atomic invariant).
        partials = list((archive / ".tmp").glob("*.partial"))
        assert partials == []
        wavs = list(archive.rglob("*.wav"))
        assert wavs == []
    finally:
        app.stop()
        runner.join(timeout=3.0)
