"""PRD §2.6 proof of life — keystrokes drive the whisper/unknown flow.

Unlike `test_classify_integration.py` (which calls `App.offload_whisper`
directly), these tests simulate the actual operator-facing entry point:
raw characters dispatched through `TUI._on_key`. The /implement-prd Gate 4
step 5 (Operator-Entry-Point Proof) requires this layer of verification for
any UI feature — internal methods sitting behind the binding don't count.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime
from pathlib import Path
from typing import List

from hidock_direct.app import App
from hidock_direct.classify import RecordingKind
from hidock_direct.events import Event, EventBus
from hidock_direct.offload import Offloader
from hidock_direct.state import StateStore
from hidock_direct.tui import TUI, KeyboardReader

from tests.fixtures.mock_device import MockDevice, MockFile, make_wav_bytes


class _FakeHTAConverter:
    def convert_hta_to_wav(self, hta_path: str, output_path=None) -> str:
        out = Path(hta_path).with_suffix(".wav")
        out.write_bytes(Path(hta_path).read_bytes())
        return str(out)


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


class _NoopKeyboard:
    """Replaces the stdin-reading `KeyboardReader` under pytest (stdin is
    not a TTY). Tests call `tui._on_key(ch)` directly to simulate input.
    """
    def start(self): pass
    def stop(self): pass


def _wait_until(pred, timeout=3.0, msg="never"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return
        time.sleep(0.02)
    raise AssertionError(msg)


def _build(tmp_path: Path, files):
    archive = tmp_path / "arch"
    (archive / ".state").mkdir(parents=True, exist_ok=True)
    (archive / ".tmp").mkdir(parents=True, exist_ok=True)
    bus = EventBus()
    events: List[Event] = []
    bus.subscribe(events.append)
    store = StateStore(archive / ".state" / "offload_state.json")
    mock = MockDevice(files=files)
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
    tui = TUI(
        bus=bus, app=app,
        pending_whispers_provider=lambda: app._pending_whispers,
        pending_unknowns_provider=lambda: app._pending_unknowns,
        keyboard=_NoopKeyboard(),
    )
    return app, tui, store, archive, watcher


def test_w_key_then_space_then_enter_offloads_whisper(tmp_path: Path):
    files = [
        MockFile(name="20260414-100000-Rec1.hda", content=make_wav_bytes(), device_mtime=datetime(2026, 4, 14, 10, 0, 0)),
        MockFile(name="20260414-110000-Wip1.hda", content=make_wav_bytes(), device_mtime=datetime(2026, 4, 14, 11, 0, 0)),
    ]
    app, tui, store, archive, watcher = _build(tmp_path, files)
    runner = threading.Thread(target=app.run, daemon=True)
    runner.start()
    try:
        _wait_until(lambda: watcher.started, msg="watcher never started")
        watcher.fire_attach()
        _wait_until(
            lambda: app._pending_whispers and any(f.name == "20260414-110000-Wip1.hda" for f in app._pending_whispers),
            timeout=5.0, msg="whisper never surfaced",
        )
        # Ensure we're in CONNECTED_IDLE so the key-activation gate passes.
        _wait_until(lambda: tui._state == "CONNECTED_IDLE", timeout=5.0, msg="TUI never hit CONNECTED_IDLE")
        # Simulate the actual keystrokes an operator would type.
        tui._on_key("w")      # open whisper selector
        assert tui._whisper_modal is not None, "modal should open on `w`"
        tui._on_key(" ")      # toggle-select the single whisper
        tui._on_key("\r")     # enter → offload
        _wait_until(
            lambda: (archive / "whispers" / "2026" / "04" / "2026-04-14_110000.wav").exists(),
            timeout=5.0, msg="whisper never landed in archive",
        )
        assert tui._whisper_modal is None, "modal should close after enter"
        assert store.kind_for(_key(), "20260414-110000-Wip1.hda") is RecordingKind.WHISPER
    finally:
        app.stop()
        runner.join(timeout=3.0)


def test_u_key_m_routes_unknown_as_meeting(tmp_path: Path):
    files = [
        MockFile(name="20260414-100000-Rec1.hda", content=make_wav_bytes(), device_mtime=datetime(2026, 4, 14, 10, 0, 0)),
        MockFile(name="20260414-120000-FooBar7.hda", content=make_wav_bytes(), device_mtime=datetime(2026, 4, 14, 12, 0, 0)),
    ]
    app, tui, store, archive, watcher = _build(tmp_path, files)
    runner = threading.Thread(target=app.run, daemon=True)
    runner.start()
    try:
        _wait_until(lambda: watcher.started, msg="watcher never started")
        watcher.fire_attach()
        _wait_until(
            lambda: any(f.name == "20260414-120000-FooBar7.hda" for f in app._pending_unknowns),
            timeout=5.0, msg="unknown never surfaced",
        )
        _wait_until(lambda: tui._state == "CONNECTED_IDLE", timeout=5.0, msg="TUI never hit CONNECTED_IDLE")
        tui._on_key("u")      # open unknown prompt
        assert tui._unknown_queue == ["20260414-120000-FooBar7.hda"]
        tui._on_key("m")      # route as meeting
        _wait_until(
            lambda: (archive / "2026" / "04" / "2026-04-14_120000.wav").exists(),
            timeout=5.0, msg="unknown-as-meeting never landed",
        )
        assert tui._unknown_queue == [], "queue should be empty after routing"
        assert store.kind_for(_key(), "20260414-120000-FooBar7.hda") is RecordingKind.MEETING
    finally:
        app.stop()
        runner.join(timeout=3.0)


def test_w_key_ignored_when_no_pending_whispers(tmp_path: Path):
    files = [MockFile(name="20260414-100000-Rec1.hda", content=make_wav_bytes(), device_mtime=datetime(2026, 4, 14, 10, 0, 0))]
    app, tui, store, archive, watcher = _build(tmp_path, files)
    runner = threading.Thread(target=app.run, daemon=True)
    runner.start()
    try:
        _wait_until(lambda: watcher.started, msg="watcher never started")
        watcher.fire_attach()
        _wait_until(lambda: tui._state == "CONNECTED_IDLE", timeout=5.0, msg="never idle")
        tui._on_key("w")      # no whispers pending; should not open modal
        assert tui._whisper_modal is None
    finally:
        app.stop()
        runner.join(timeout=3.0)


def test_q_cancels_whisper_modal_without_offload(tmp_path: Path):
    files = [
        MockFile(name="20260414-100000-Rec1.hda", content=make_wav_bytes(), device_mtime=datetime(2026, 4, 14, 10, 0, 0)),
        MockFile(name="20260414-110000-Wip1.hda", content=make_wav_bytes(), device_mtime=datetime(2026, 4, 14, 11, 0, 0)),
    ]
    app, tui, store, archive, watcher = _build(tmp_path, files)
    runner = threading.Thread(target=app.run, daemon=True)
    runner.start()
    try:
        _wait_until(lambda: watcher.started, msg="watcher never started")
        watcher.fire_attach()
        _wait_until(lambda: app._pending_whispers, timeout=5.0, msg="whisper never surfaced")
        _wait_until(lambda: tui._state == "CONNECTED_IDLE", timeout=5.0, msg="never idle")
        tui._on_key("w")
        tui._on_key(" ")      # toggle-select
        tui._on_key("q")      # cancel
        assert tui._whisper_modal is None
        assert not (archive / "whispers").exists()
        assert not store.is_processed(_key(), "20260414-110000-Wip1.hda")
    finally:
        app.stop()
        runner.join(timeout=3.0)


def test_keys_ignored_outside_connected_idle(tmp_path: Path):
    """PRD §2.6: `w`/`u` only respond in CONNECTED_IDLE."""
    app, tui, store, archive, watcher = _build(tmp_path, files=[])
    # No runner started — app state stays IDLE_DISCONNECTED.
    # Force pending whispers so the provider isn't empty (separately tested).
    from tests.fixtures.mock_device import MockFile
    app._pending_whispers = [MockFile(name="20260414-110000-Wip1.hda", content=b"")]
    assert tui._state == "IDLE_DISCONNECTED"
    tui._on_key("w")
    assert tui._whisper_modal is None, "w must be ignored outside CONNECTED_IDLE"


def test_keyboard_reader_no_ops_when_stdin_not_tty():
    """Reader must not crash when stdin is not a TTY (pytest, piping)."""
    reader = KeyboardReader(lambda _ch: None)
    reader.start()
    reader.stop()  # must be idempotent + safe even after failed start


def _key():
    from hidock_direct.state import DeviceKey
    return DeviceKey(model="hidock-h1", serial="SN-TEST-1234")
