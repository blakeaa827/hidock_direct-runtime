"""Regression tests: vendored Jensen ConnectionError must be caught and translated.

The vendored Jensen protocol raises Python's builtin ``ConnectionError`` on USB
communication failures. ``device.py`` must translate these into
``DeviceNotConnected`` so the app's worker thread can recover gracefully instead
of crashing.

Bug report: planning/bug_report_jensen_connectionerror_uncaught.md
"""

from __future__ import annotations

import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, List, Optional

import pytest

from hidock_direct.app import App, AppState
from hidock_direct.device import (
    DeviceFile,
    DeviceInfo,
    DeviceNotConnected,
    JensenDeviceAdapter,
)
from hidock_direct.events import Event, EventBus
from hidock_direct.offload import Offloader
from hidock_direct.state import StateStore
from hidock_direct.usb_watcher import AttachCallback, DetachCallback

from tests.fixtures.mock_device import MockDevice, MockFile, make_wav_bytes


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeJensen:
    """Minimal Jensen stand-in that raises ConnectionError on command."""

    def __init__(self):
        self._connected = True
        self._fail_on: set[str] = set()

    def is_connected(self) -> bool:
        return self._connected

    def connect(self, **kw):
        self._connected = True
        return True, None

    def disconnect(self):
        self._connected = False

    def get_device_info(self):
        return {"sn": "FAKE-SN"}

    def list_files(self):
        if "list_files" in self._fail_on:
            raise ConnectionError("Device not connected.")
        return {"files": []}

    def stream_file(self, name, size, *, data_callback, progress_callback=None, cancel_event=None):
        if "stream_file" in self._fail_on:
            raise ConnectionError("Device not connected.")
        return "OK"

    def delete_file(self, name):
        if "delete_file" in self._fail_on:
            raise ConnectionError("Device not connected.")
        return {}


class ConnectionErrorDevice(MockDevice):
    """MockDevice that raises builtin ConnectionError instead of DeviceNotConnected."""

    def __init__(self, **kw):
        super().__init__(**kw)
        self._fail_on: set[str] = set()

    def connect(self) -> DeviceInfo:
        if "connect" in self._fail_on:
            raise ConnectionError("Device not connected.")
        return super().connect()

    def list_files(self) -> List[DeviceFile]:
        if "list_files" in self._fail_on:
            raise ConnectionError("Device not connected.")
        return super().list_files()

    def download_file(self, name, size, *, on_chunk, on_progress=None, cancel_event=None):
        if "download_file" in self._fail_on:
            raise ConnectionError("Device not connected.")
        return super().download_file(name, size, on_chunk=on_chunk, on_progress=on_progress, cancel_event=cancel_event)

    def delete_file(self, name):
        if "delete_file" in self._fail_on:
            raise ConnectionError("Device not connected.")
        return super().delete_file(name)


class FakeWatcher:
    def __init__(self):
        self._attach: List[AttachCallback] = []
        self._detach: List[DetachCallback] = []
        self.started = False

    def start(self): self.started = True
    def stop(self): pass
    def on_attach(self, fn): self._attach.append(fn)
    def on_detach(self, fn): self._detach.append(fn)
    def fire_attach(self, vid=0x10D6, pid=0xAF0C):
        for fn in self._attach: fn(vid, pid)
    def fire_detach(self, vid=0x10D6, pid=0xAF0C):
        for fn in self._detach: fn(vid, pid)


def _wait_for_state(app: App, state: AppState, timeout: float = 3.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if app.state == state:
            return
        time.sleep(0.02)
    raise AssertionError(f"state never reached {state}; last={app.state}")


def _wait_until(pred, timeout=3.0, msg="predicate never true"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return
        time.sleep(0.02)
    raise AssertionError(msg)


# ---------------------------------------------------------------------------
# Phase 1 tests: device.py must translate ConnectionError -> DeviceNotConnected
# ---------------------------------------------------------------------------

class TestJensenAdapterTranslatesConnectionError:
    """JensenDeviceAdapter wraps vendored Jensen calls. When Jensen raises
    builtin ConnectionError, the adapter must re-raise as DeviceNotConnected."""

    def test_list_files_connection_error(self):
        adapter = JensenDeviceAdapter()
        fake = FakeJensen()
        fake._fail_on.add("list_files")
        adapter._jensen = fake
        adapter._info = DeviceInfo(model="test", serial="test")

        with pytest.raises(DeviceNotConnected):
            adapter.list_files()

    def test_download_file_connection_error(self):
        adapter = JensenDeviceAdapter()
        fake = FakeJensen()
        fake._fail_on.add("stream_file")
        adapter._jensen = fake
        adapter._info = DeviceInfo(model="test", serial="test")

        with pytest.raises(DeviceNotConnected):
            adapter.download_file("test.wav", 100, on_chunk=lambda c: None)

    def test_delete_file_connection_error(self):
        adapter = JensenDeviceAdapter()
        fake = FakeJensen()
        fake._fail_on.add("delete_file")
        adapter._jensen = fake
        adapter._info = DeviceInfo(model="test", serial="test")

        with pytest.raises(DeviceNotConnected):
            adapter.delete_file("test.wav")


# ---------------------------------------------------------------------------
# Phase 1 test: app worker thread must survive ConnectionError from adapter
# ---------------------------------------------------------------------------

class TestWorkerSurvivesConnectionError:
    """The worker thread in app.py must not crash when the device raises
    ConnectionError during scan. It should transition to IDLE_DISCONNECTED
    and remain alive for the next attach."""

    def test_worker_recovers_from_connection_error_during_scan(self, tmp_path: Path):
        files = [MockFile(name="REC_A.wav", content=make_wav_bytes(),
                          device_mtime=datetime(2026, 4, 12, 10, 0, 0))]
        mock = ConnectionErrorDevice(files=files)
        archive = tmp_path / "arch"
        (archive / ".state").mkdir(parents=True, exist_ok=True)
        (archive / ".tmp").mkdir(parents=True, exist_ok=True)
        bus = EventBus()
        events: list[Event] = []
        bus.subscribe(events.append)
        store = StateStore(archive / ".state" / "offload_state.json")
        watcher = FakeWatcher()
        offloader = Offloader(
            adapter=mock, store=store, bus=bus,
            archive_dir=archive, tmp_dir=archive / ".tmp",
            delete_after_offload=False, sleep=lambda *a, **k: None,
        )
        app = App(
            adapter=mock, watcher=watcher, offloader=offloader,
            store=store, bus=bus, poll_interval_seconds=1,
            sleep=lambda *a, **k: None,
        )
        runner = threading.Thread(target=app.run, daemon=True)
        runner.start()
        try:
            _wait_until(lambda: watcher.started)
            watcher.fire_attach()
            _wait_for_state(app, AppState.CONNECTED_IDLE, timeout=5.0)

            mock._fail_on.add("list_files")
            # Wait for the worker to hit the error and recover
            _wait_for_state(app, AppState.IDLE_DISCONNECTED, timeout=5.0)

            # Worker thread must still be alive
            assert app._worker is not None and app._worker.is_alive(), \
                "worker thread died after ConnectionError"
        finally:
            app.stop()
            runner.join(timeout=3.0)


# ---------------------------------------------------------------------------
# Phase 1 tests: adapter.connect() must translate ConnectionError, and the
# worker loop must survive a ConnectionError raised from _handle_attach.
# Bug report: planning/bug_report_connect_connectionerror_kills_worker.md
# ---------------------------------------------------------------------------

class FakeJensenFailingConnect(FakeJensen):
    """FakeJensen that raises ConnectionError from connect()."""

    def connect(self, **kw):
        raise ConnectionError("USB backend not available to HiDockJensen class.")


class FakeJensenFailingInfo(FakeJensen):
    """FakeJensen that connects OK but raises ConnectionError from get_device_info()."""

    def get_device_info(self):
        raise ConnectionError("Device not connected.")


class TestAdapterConnectTranslatesConnectionError:
    """JensenDeviceAdapter.connect() must translate builtin ConnectionError
    from Jensen's connect() or get_device_info() into DeviceNotConnected."""

    def _patch_enumerate(self, monkeypatch):
        monkeypatch.setattr(
            JensenDeviceAdapter, "enumerate_attached",
            staticmethod(lambda: [(0x10D6, 0xAF0C)]),
        )

    def test_connect_connection_error_from_jensen_connect(self, monkeypatch):
        self._patch_enumerate(monkeypatch)
        adapter = JensenDeviceAdapter()
        adapter._backend = object()
        adapter._jensen = FakeJensenFailingConnect()

        with pytest.raises(DeviceNotConnected) as excinfo:
            adapter.connect()

        assert "USB backend not available" in str(excinfo.value)

    def test_connect_connection_error_from_get_device_info(self, monkeypatch):
        self._patch_enumerate(monkeypatch)
        adapter = JensenDeviceAdapter()
        adapter._backend = object()
        adapter._jensen = FakeJensenFailingInfo()

        with pytest.raises(DeviceNotConnected) as excinfo:
            adapter.connect()

        assert "Device not connected" in str(excinfo.value)


class TestWorkerSurvivesConnectionErrorDuringAttach:
    """_handle_attach must not let a ConnectionError from adapter.connect()
    kill the worker thread. The app must publish a WARNING and stay in
    IDLE_DISCONNECTED, then successfully connect on the next attach."""

    def test_worker_survives_connection_error_during_attach(self, tmp_path: Path):
        from hidock_direct.events import Severity

        mock = ConnectionErrorDevice(files=[])
        mock._fail_on.add("connect")

        archive = tmp_path / "arch"
        (archive / ".state").mkdir(parents=True, exist_ok=True)
        (archive / ".tmp").mkdir(parents=True, exist_ok=True)
        bus = EventBus()
        events: list[Event] = []
        bus.subscribe(events.append)
        store = StateStore(archive / ".state" / "offload_state.json")
        watcher = FakeWatcher()
        offloader = Offloader(
            adapter=mock, store=store, bus=bus,
            archive_dir=archive, tmp_dir=archive / ".tmp",
            delete_after_offload=False, sleep=lambda *a, **k: None,
        )
        app = App(
            adapter=mock, watcher=watcher, offloader=offloader,
            store=store, bus=bus, poll_interval_seconds=1,
            sleep=lambda *a, **k: None,
        )
        runner = threading.Thread(target=app.run, daemon=True)
        runner.start()
        try:
            _wait_until(lambda: watcher.started)
            watcher.fire_attach()

            # A WARNING event with context="connect" must be published,
            # confirming _handle_attach caught the failure instead of
            # letting it kill the worker.
            def has_connect_warning():
                from hidock_direct.events import Error
                return any(
                    isinstance(e, Error)
                    and e.severity == Severity.WARNING
                    and e.context == "connect"
                    and "Device not connected" in e.message
                    for e in events
                )
            _wait_until(has_connect_warning, timeout=3.0,
                        msg="no connect-failure WARNING was published")

            # State must still be IDLE_DISCONNECTED.
            assert app.state == AppState.IDLE_DISCONNECTED

            # Worker must still be alive.
            assert app._worker is not None and app._worker.is_alive(), \
                "worker thread died after ConnectionError during attach"

            # Next attach (device recovers) must succeed -- proves the worker
            # is not just alive but still consuming attach events.
            mock._fail_on.discard("connect")
            watcher.fire_attach()
            _wait_for_state(app, AppState.CONNECTED_IDLE, timeout=3.0)
        finally:
            app.stop()
            runner.join(timeout=3.0)
