"""PRD §5.4 end-to-end: mixed device contents through a scan + auto-offload."""

from __future__ import annotations

import threading
import time
from datetime import datetime
from pathlib import Path
from typing import List

from hidock_direct.app import App, AppState
from hidock_direct.classify import RecordingKind
from hidock_direct.events import Event, EventBus, UnknownsDetected, WhispersDetected
from hidock_direct.offload import Offloader
from hidock_direct.state import StateStore

from tests.fixtures.mock_device import MockDevice, MockFile, make_wav_bytes


class _FakeHTAConverter:
    def convert_hta_to_wav(self, hta_path: str, output_path=None) -> str:
        out = Path(hta_path).with_suffix(".wav")
        out.write_bytes(Path(hta_path).read_bytes())
        return str(out)


class _FakeWatcher:
    def __init__(self):
        self._attach = []
        self._detach = []
        self.started = False

    def start(self): self.started = True
    def stop(self): pass
    def on_attach(self, fn): self._attach.append(fn)
    def on_detach(self, fn): self._detach.append(fn)
    def fire_attach(self, vid=0x10D6, pid=0xAF0C):
        for fn in self._attach:
            fn(vid, pid)


def _wait_until(pred, timeout=3.0, msg="never"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return
        time.sleep(0.02)
    raise AssertionError(msg)


def test_mixed_scan_routes_correctly(tmp_path: Path):
    archive = tmp_path / "arch"
    (archive / ".state").mkdir(parents=True, exist_ok=True)
    (archive / ".tmp").mkdir(parents=True, exist_ok=True)
    bus = EventBus()
    events: List[Event] = []
    bus.subscribe(events.append)
    store = StateStore(archive / ".state" / "offload_state.json")
    mock = MockDevice(files=[
        MockFile(name="20260414-100000-Rec1.hda", content=make_wav_bytes(), device_mtime=datetime(2026, 4, 14, 10, 0, 0)),
        MockFile(name="20260414-100100-Rec2.hda", content=make_wav_bytes(), device_mtime=datetime(2026, 4, 14, 10, 1, 0)),
        MockFile(name="20260414-110000-Wip1.hda", content=make_wav_bytes(), device_mtime=datetime(2026, 4, 14, 11, 0, 0)),
        MockFile(name="20260414-120000-FooBar7.hda", content=make_wav_bytes(), device_mtime=datetime(2026, 4, 14, 12, 0, 0)),
    ])
    watcher = _FakeWatcher()
    offloader = Offloader(
        adapter=mock, store=store, bus=bus,
        archive_dir=archive, tmp_dir=archive / ".tmp",
        delete_after_offload=False,
        hta_converter=_FakeHTAConverter(),
        sleep=lambda *_a, **_k: None,
    )
    app = App(
        adapter=mock, watcher=watcher, offloader=offloader, store=store, bus=bus,
        poll_interval_seconds=1, sleep=lambda *_a, **_k: None,
    )
    runner = threading.Thread(target=app.run, daemon=True)
    runner.start()
    try:
        _wait_until(lambda: watcher.started, msg="watcher never started")
        watcher.fire_attach()
        _wait_until(
            lambda: store.is_processed(_key(), "20260414-100100-Rec2.hda"),
            timeout=5.0,
            msg="meetings never fully drained",
        )

        # Meetings landed in the archive root; whispers/unknowns did not.
        assert (archive / "2026" / "04" / "2026-04-14_100000.wav").exists()
        assert (archive / "2026" / "04" / "2026-04-14_100100.wav").exists()
        assert not (archive / "whispers").exists(), "whispers never offloaded, dir must be absent"
        # No unknown-file archive artifact either.
        remaining_archive = list(archive.rglob("*.wav"))
        assert len(remaining_archive) == 2

        # Ledger contains only the 2 meetings.
        assert store.kind_for(_key(), "20260414-100000-Rec1.hda") is RecordingKind.MEETING
        assert not store.is_processed(_key(), "20260414-110000-Wip1.hda")
        assert not store.is_processed(_key(), "20260414-120000-FooBar7.hda")

        # Classification events fired.
        assert any(isinstance(e, WhispersDetected) and e.count == 1 for e in events)
        assert any(isinstance(e, UnknownsDetected) and e.filenames == ["20260414-120000-FooBar7.hda"] for e in events)

        # App exposes pending buckets for the TUI.
        assert [f.name for f in app._pending_whispers] == ["20260414-110000-Wip1.hda"]
        assert [f.name for f in app._pending_unknowns] == ["20260414-120000-FooBar7.hda"]

        # Operator routes the whisper via App.offload_whisper.
        assert app.offload_whisper("20260414-110000-Wip1.hda") is True
        assert (archive / "whispers" / "2026" / "04" / "2026-04-14_110000.wav").exists()
        assert store.kind_for(_key(), "20260414-110000-Wip1.hda") is RecordingKind.WHISPER
        assert app._pending_whispers == []

        # Operator routes the unknown as a meeting.
        assert app.route_unknown("20260414-120000-FooBar7.hda", RecordingKind.MEETING) is True
        assert (archive / "2026" / "04" / "2026-04-14_120000.wav").exists()
        assert store.kind_for(_key(), "20260414-120000-FooBar7.hda") is RecordingKind.MEETING

        # Re-scan: skipped whispers/unknowns re-surface; processed ones don't.
        mock.add_file(MockFile(name="20260415-090000-Wip9.hda", content=make_wav_bytes(), device_mtime=datetime(2026, 4, 15, 9, 0, 0)))
        result = offloader.scan_pending_files(_key())
        # None of the processed files (meetings + routed whisper + routed unknown)
        # appear in any bucket.
        all_returned = {f.name for f in result.meetings + result.whispers + result.unknowns}
        assert "20260414-100000-Rec1.hda" not in all_returned
        assert "20260414-110000-Wip1.hda" not in all_returned
        assert "20260414-120000-FooBar7.hda" not in all_returned
        # The freshly-added whisper does appear.
        assert "20260415-090000-Wip9.hda" in {f.name for f in result.whispers}
    finally:
        app.stop()
        runner.join(timeout=3.0)


def _key():
    from hidock_direct.state import DeviceKey
    return DeviceKey(model="hidock-h1", serial="SN-TEST-1234")
