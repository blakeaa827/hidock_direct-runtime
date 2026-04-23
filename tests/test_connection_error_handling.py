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
    DeviceError,
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


# ---------------------------------------------------------------------------
# Phase 1 tests: competing-WebUSB-claim error translation + spurious-detach
# suppression.
# Bug report: planning/bug_report_first_plug_usb_state_unrecoverable.md
# ---------------------------------------------------------------------------

class _FailingConnectAdapter(MockDevice):
    """MockDevice whose connect() raises a configurable DeviceError message.

    Used to drive the app through the competing-claim code path without
    touching pyusb or hardware.
    """

    def __init__(self, *, error_message: str, **kw):
        super().__init__(**kw)
        self._error_message = error_message

    def connect(self) -> DeviceInfo:
        raise DeviceError(self._error_message)


def _run_app_with_failing_connect(tmp_path: Path, error_message: str):
    """Spin up the app with a failing adapter, fire one attach, collect events.

    Returns (app, runner, watcher, events) so the caller can inspect and then
    clean up. Always fire detach / stop from the caller.
    """
    mock = _FailingConnectAdapter(error_message=error_message)
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
    _wait_until(lambda: watcher.started)
    watcher.fire_attach()
    return app, runner, watcher, events


class TestCompetingClaimErrorTranslation:
    """errno 13 / errno 19 / 'Access denied' / 'No such device' during
    adapter.connect() must be translated into a WARNING that names the
    likely culprit (HiDock web interface / browser tab) and the remediation
    (close it). A raw libusb string leaves the operator with no actionable
    guidance -- that is the bug."""

    def _get_connect_warning(self, events):
        from hidock_direct.events import Error, Severity
        matches = [
            e for e in events
            if isinstance(e, Error) and e.severity == Severity.WARNING and e.context == "connect"
        ]
        assert matches, "no connect WARNING was published"
        return matches[-1]

    def test_access_denied_translates_to_actionable_message(self, tmp_path):
        app, runner, watcher, events = _run_app_with_failing_connect(
            tmp_path,
            "Access denied to device. It might be in use by another app or require admin rights.",
        )
        try:
            _wait_until(lambda: any(
                getattr(e, "context", None) == "connect" for e in events
            ), timeout=3.0, msg="no connect warning")
            warn = self._get_connect_warning(events)
            msg = warn.message

            # Semantic pin (Gate 1 step 4): the translated message must
            # name *the cause* AND *the action*. A mention-check on any
            # single token would pass against a message that only named
            # the cause without remediation, or vice versa. The conjunction
            # is the correctness check.
            assert "HiDock web interface" in msg, f"cause not named: {msg!r}"
            assert "browser tab" in msg, f"browser tab not mentioned: {msg!r}"
            assert "close" in msg.lower(), f"action not named: {msg!r}"
        finally:
            app.stop()
            runner.join(timeout=3.0)

    def test_no_such_device_translates_to_actionable_message(self, tmp_path):
        app, runner, watcher, events = _run_app_with_failing_connect(
            tmp_path,
            "Failed to connect to device: USB Error: [Errno 19] No such device (it may have been disconnected)",
        )
        try:
            _wait_until(lambda: any(
                getattr(e, "context", None) == "connect" for e in events
            ), timeout=3.0, msg="no connect warning")
            warn = self._get_connect_warning(events)
            msg = warn.message

            assert "HiDock web interface" in msg, f"cause not named: {msg!r}"
            assert "browser tab" in msg, f"browser tab not mentioned: {msg!r}"
            assert "close" in msg.lower(), f"action not named: {msg!r}"
        finally:
            app.stop()
            runner.join(timeout=3.0)

    def test_unknown_device_error_is_passed_through(self, tmp_path):
        """Negative case: a DeviceError that isn't in the competing-claim
        class must not receive the web-interface translation. Prevents
        over-translation."""
        raw = "Firmware protocol error: bad response header"
        app, runner, watcher, events = _run_app_with_failing_connect(tmp_path, raw)
        try:
            _wait_until(lambda: any(
                getattr(e, "context", None) == "connect" for e in events
            ), timeout=3.0, msg="no connect warning")
            warn = self._get_connect_warning(events)
            msg = warn.message

            # Must not translate: no web-interface text.
            assert "HiDock web interface" not in msg, \
                f"over-translated non-claim error: {msg!r}"
            assert "browser tab" not in msg, \
                f"over-translated non-claim error: {msg!r}"
            # Must surface the original error text so the operator sees
            # the real failure.
            assert raw in msg, f"raw error not surfaced: {msg!r}"
        finally:
            app.stop()
            runner.join(timeout=3.0)


class TestSpuriousDetachSuppressed:
    """When the watcher fires detach but no attach ever successfully
    connected (_device_key is None), DeviceDetached must not be published.
    Publishing it as serial='unknown' confuses the operator by making a
    benign watcher poll look like a real device event."""

    def test_no_detach_event_when_no_prior_attach_succeeded(self, tmp_path):
        from hidock_direct.events import DeviceDetached

        app, runner, watcher, events = _run_app_with_failing_connect(
            tmp_path,
            "Access denied to device. It might be in use by another app or require admin rights.",
        )
        try:
            # Wait for the connect WARNING so we know the attach was processed.
            _wait_until(lambda: any(
                getattr(e, "context", None) == "connect" for e in events
            ), timeout=3.0, msg="no connect warning")

            # Fire detach. Since no attach ever succeeded, _device_key is None.
            watcher.fire_detach()

            # Give the watcher callback a moment to run (it's synchronous
            # in FakeWatcher.fire_detach, but the bus subscriber runs on
            # the publish path which is also synchronous -- small sleep
            # guards against scheduling weirdness).
            time.sleep(0.2)

            detach_events = [e for e in events if isinstance(e, DeviceDetached)]
            assert not detach_events, \
                f"DeviceDetached should be suppressed when _device_key is None; got: {detach_events}"
        finally:
            app.stop()
            runner.join(timeout=3.0)

    def test_detach_still_published_after_successful_attach(self, tmp_path):
        """Positive case: once a real attach has succeeded and set
        _device_key, a subsequent detach MUST still publish DeviceDetached
        -- the suppression is narrow, not a blanket silencing."""
        from hidock_direct.events import DeviceDetached

        mock = MockDevice()
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
            _wait_for_state(app, AppState.CONNECTED_IDLE, timeout=3.0)

            watcher.fire_detach()
            _wait_until(
                lambda: any(isinstance(e, DeviceDetached) for e in events),
                timeout=2.0, msg="DeviceDetached was not published after real attach",
            )
            detach_events = [e for e in events if isinstance(e, DeviceDetached)]
            # Must use the real serial, not 'unknown'.
            assert any(e.serial == "SN-TEST-1234" for e in detach_events), \
                f"DeviceDetached did not carry the real serial; got: {detach_events}"
        finally:
            app.stop()
            runner.join(timeout=3.0)
