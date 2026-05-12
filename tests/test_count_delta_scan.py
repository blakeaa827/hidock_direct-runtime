"""Verify the App runs `list_files` only on count-delta.

The expensive scan (`list_files`, ~27s on a full P1) must fire exactly:
1. Once on first attach (`_last_known_count` is None),
2. Whenever `get_file_count` returns a value different from the last.

On polls where count is unchanged, the worker must stay in CONNECTED_IDLE
and NOT call `list_files`. This is the core architectural win that keeps
the TUI's `w`/`u` keys responsive on large devices.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime
from pathlib import Path
from typing import List

from hidock_direct.app import App, AppState
from hidock_direct.events import Event, EventBus, ScanStarted
from hidock_direct.offload import Offloader
from hidock_direct.state import StateStore

from tests.fixtures.mock_device import MockDevice, MockFile, make_wav_bytes


class _FakeWatcher:
    def __init__(self):
        self._attach = []
        self.started = False
    def start(self): self.started = True
    def stop(self): pass
    def on_attach(self, fn): self._attach.append(fn)
    def on_detach(self, fn): pass
    def fire_attach(self, vid=0x10D6, pid=0xAF0C):
        for fn in self._attach: fn(vid, pid)


class _CountingDevice(MockDevice):
    """MockDevice that tracks list_files invocations so the test can assert
    how many full scans fired.
    """
    def __init__(self, **kw):
        super().__init__(**kw)
        self.list_files_calls = 0
        self.get_file_count_calls = 0

    def list_files(self):
        self.list_files_calls += 1
        return super().list_files()

    def get_file_count(self):
        self.get_file_count_calls += 1
        return super().get_file_count()


def _wait_until(pred, timeout=3.0, msg="never"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return
        time.sleep(0.02)
    raise AssertionError(msg)


def test_list_files_fires_once_on_attach_then_skips_when_count_unchanged(tmp_path: Path):
    files = [MockFile(name="20260414-100000-Rec1.hda", content=make_wav_bytes(), device_mtime=datetime(2026, 4, 14, 10, 0, 0))]
    mock = _CountingDevice(files=files)
    archive = tmp_path / "arch"
    (archive / ".state").mkdir(parents=True, exist_ok=True)
    (archive / ".tmp").mkdir(parents=True, exist_ok=True)
    bus = EventBus()
    events: List[Event] = []
    bus.subscribe(events.append)
    store = StateStore(archive / ".state" / "offload_state.json")
    watcher = _FakeWatcher()
    offloader = Offloader(
        adapter=mock, store=store, bus=bus,
        archive_dir=archive, tmp_dir=archive / ".tmp",
        delete_after_offload=False,
        transcribe_on_offload=False,
        hta_converter=_FakeHTAConverter(),
        sleep=lambda *_a, **_k: None,
    )
    app = App(
        adapter=mock, watcher=watcher, offloader=offloader, store=store, bus=bus,
        poll_interval_seconds=0, sleep=lambda *_a, **_k: None,  # tight loop
    )
    runner = threading.Thread(target=app.run, daemon=True)
    runner.start()
    try:
        _wait_until(lambda: watcher.started, msg="watcher never started")
        watcher.fire_attach()
        # Wait for first scan to complete (meeting processed).
        _wait_until(
            lambda: store.is_processed(_key(), "20260414-100000-Rec1.hda"),
            timeout=5.0, msg="first scan never completed",
        )
        # First scan should have fired exactly one list_files call.
        first_list_calls = mock.list_files_calls
        assert first_list_calls == 1, f"expected 1 list_files on first scan, got {first_list_calls}"
        first_count_calls = mock.get_file_count_calls

        # Wait for the worker to poll at least 2 more times. Count is unchanged
        # (still 1), so no additional list_files should fire. Active-wait on
        # get_file_count progress instead of a wall-clock sleep so the test
        # tolerates slow CI / iCloud-synced FS. transcribe_on_offload=False
        # above keeps the worker out of the diarize_audio pipeline (slow on
        # iCloud-synced volumes due to .pth re-hiding + import-time I/O).
        _wait_until(
            lambda: mock.get_file_count_calls >= first_count_calls + 2,
            timeout=3.0,
            msg=f"worker never polled past {first_count_calls + 2} "
                f"(count_calls={mock.get_file_count_calls})",
        )
        assert mock.list_files_calls == first_list_calls, (
            f"list_files fired {mock.list_files_calls - first_list_calls} "
            f"extra times while count was unchanged"
        )
    finally:
        app.stop()
        runner.join(timeout=3.0)


def test_count_delta_triggers_a_new_list_files(tmp_path: Path):
    files = [MockFile(name="20260414-100000-Rec1.hda", content=make_wav_bytes(), device_mtime=datetime(2026, 4, 14, 10, 0, 0))]
    mock = _CountingDevice(files=files)
    archive = tmp_path / "arch"
    (archive / ".state").mkdir(parents=True, exist_ok=True)
    (archive / ".tmp").mkdir(parents=True, exist_ok=True)
    bus = EventBus()
    store = StateStore(archive / ".state" / "offload_state.json")
    watcher = _FakeWatcher()
    offloader = Offloader(
        adapter=mock, store=store, bus=bus,
        archive_dir=archive, tmp_dir=archive / ".tmp",
        delete_after_offload=False,
        transcribe_on_offload=False,
        hta_converter=_FakeHTAConverter(),
        sleep=lambda *_a, **_k: None,
    )
    app = App(
        adapter=mock, watcher=watcher, offloader=offloader, store=store, bus=bus,
        poll_interval_seconds=0, sleep=lambda *_a, **_k: None,
    )
    runner = threading.Thread(target=app.run, daemon=True)
    runner.start()
    try:
        _wait_until(lambda: watcher.started)
        watcher.fire_attach()
        _wait_until(
            lambda: store.is_processed(_key(), "20260414-100000-Rec1.hda"),
            timeout=5.0, msg="first scan never completed",
        )
        baseline_list_calls = mock.list_files_calls

        # Simulate a new recording appearing on the device.
        mock.add_file(MockFile(
            name="20260414-110000-Rec2.hda",
            content=make_wav_bytes(),
            device_mtime=datetime(2026, 4, 14, 11, 0, 0),
        ))
        # Worker should detect the count delta and run a full scan.
        _wait_until(
            lambda: store.is_processed(_key(), "20260414-110000-Rec2.hda"),
            timeout=5.0, msg="second file never got scanned + offloaded",
        )
        assert mock.list_files_calls > baseline_list_calls, \
            "count delta should have triggered a new list_files"
    finally:
        app.stop()
        runner.join(timeout=3.0)


class _FakeHTAConverter:
    def convert_hta_to_wav(self, hta_path: str, output_path=None) -> str:
        out = Path(hta_path).with_suffix(".wav")
        out.write_bytes(Path(hta_path).read_bytes())
        return str(out)


def _key():
    from hidock_direct.state import DeviceKey
    return DeviceKey(model="hidock-h1", serial="SN-TEST-1234")
