"""State machine and lifecycle controller.

Drives the app between the PRD §2.1 states:

    IDLE_DISCONNECTED -> CONNECTED_IDLE -> SCANNING -> DRAINING -> CONNECTED_IDLE

A detach event at any point cancels the in-flight transfer (via
`cancel_event`), rolls back staged bytes, and returns to IDLE_DISCONNECTED.
The watcher, scan/drain worker, and offload pipeline are all decoupled by
the event bus — the TUI never holds business logic.
"""

from __future__ import annotations

import threading
import time
from enum import Enum
from typing import Callable, Optional

from .device import DeviceAdapter, DeviceError, DeviceNotConnected, TransferAborted
from .events import (
    DeviceAttached,
    DeviceDetached,
    Error,
    EventBus,
    IdleWaiting,
    Severity,
)
from .offload import Offloader
from .state import DeviceKey, StateStore
from .usb_watcher import USBWatcherProtocol


class AppState(str, Enum):
    IDLE_DISCONNECTED = "IDLE_DISCONNECTED"
    CONNECTED_IDLE = "CONNECTED_IDLE"
    SCANNING = "SCANNING"
    DRAINING = "DRAINING"


class App:
    """Orchestrator. `run()` blocks until `stop()` is called (signal handler, Ctrl-C)."""

    def __init__(
        self,
        *,
        adapter: DeviceAdapter,
        watcher: USBWatcherProtocol,
        offloader: Offloader,
        store: StateStore,
        bus: EventBus,
        poll_interval_seconds: int = 10,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self._adapter = adapter
        self._watcher = watcher
        self._offloader = offloader
        self._store = store
        self._bus = bus
        self._poll_interval = max(1, int(poll_interval_seconds))
        self._sleep = sleep

        self._state: AppState = AppState.IDLE_DISCONNECTED
        self._state_lock = threading.RLock()

        self._attach_signal = threading.Event()
        self._detach_signal = threading.Event()
        self._stop_signal = threading.Event()
        self._cancel_transfer = threading.Event()

        self._worker: Optional[threading.Thread] = None
        self._device_key: Optional[DeviceKey] = None

    @property
    def state(self) -> AppState:
        with self._state_lock:
            return self._state

    def _transition(self, new: AppState) -> None:
        with self._state_lock:
            if self._state != new:
                self._state = new
                self._bus.publish(IdleWaiting(state=new.value))

    # -- lifecycle ------------------------------------------------------

    def run(self) -> None:
        self._offloader.ensure_dirs()
        self._offloader.clean_stale_partials()
        self._store.load()

        self._watcher.on_attach(self._on_attach)
        self._watcher.on_detach(self._on_detach)
        self._watcher.start()
        # Force an initial IdleWaiting publish even though `_state` already
        # holds IDLE_DISCONNECTED — subscribers (TUI) use this as the first
        # sync point for their render loop.
        self._bus.publish(IdleWaiting(state=self._state.value))

        self._worker = threading.Thread(target=self._worker_loop, name="hidock-worker", daemon=True)
        self._worker.start()
        # Seed: if a device is already present at launch, the watcher should
        # fire its attach callback within the first poll — we don't need to
        # enumerate here.

        try:
            while not self._stop_signal.is_set():
                self._stop_signal.wait(0.5)
        finally:
            self.stop()

    def stop(self) -> None:
        if self._stop_signal.is_set() and self._worker is None:
            return
        self._stop_signal.set()
        self._cancel_transfer.set()
        self._attach_signal.set()
        try:
            self._watcher.stop()
        except Exception:
            pass
        if self._worker is not None:
            self._worker.join(timeout=3.0)
            self._worker = None
        if self._adapter.is_connected():
            try:
                self._adapter.disconnect()
            except DeviceError:
                pass

    # -- watcher callbacks ---------------------------------------------

    def _on_attach(self, vid: int, pid: int) -> None:  # noqa: ARG002
        self._cancel_transfer.clear()
        self._detach_signal.clear()
        self._attach_signal.set()

    def _on_detach(self, vid: int, pid: int) -> None:  # noqa: ARG002
        self._cancel_transfer.set()
        self._detach_signal.set()
        serial = self._device_key.serial if self._device_key else "unknown"
        self._bus.publish(DeviceDetached(serial=serial))

    # -- worker loop ----------------------------------------------------

    def _worker_loop(self) -> None:
        while not self._stop_signal.is_set():
            if self._detach_signal.is_set():
                self._handle_disconnect()
                continue

            if self.state == AppState.IDLE_DISCONNECTED:
                if not self._attach_signal.wait(timeout=0.5):
                    continue
                self._attach_signal.clear()
                if self._stop_signal.is_set():
                    return
                self._handle_attach()
                continue

            # Connected: periodic scan + drain.
            try:
                self._run_scan_and_drain()
            except DeviceNotConnected:
                self._handle_disconnect()
                continue
            except DeviceError as exc:
                self._bus.publish(Error(message=str(exc), severity=Severity.ERROR, context="worker_loop"))
                self._handle_disconnect()
                continue

            if self._detach_signal.is_set():
                self._handle_disconnect()
                continue

            # Sleep until next poll, wake early on detach or shutdown.
            for _ in range(self._poll_interval):
                if self._stop_signal.is_set() or self._detach_signal.is_set():
                    break
                self._sleep(1.0)

    def _handle_attach(self) -> None:
        try:
            info = self._adapter.connect()
        except DeviceError as exc:
            self._bus.publish(Error(message=str(exc), severity=Severity.WARNING, context="connect"))
            self._transition(AppState.IDLE_DISCONNECTED)
            return
        self._device_key = DeviceKey(model=info.model, serial=info.serial)
        self._store.register_device(self._device_key)
        self._bus.publish(DeviceAttached(model=info.model, serial=info.serial))
        self._transition(AppState.CONNECTED_IDLE)

    def _handle_disconnect(self) -> None:
        if self._adapter.is_connected():
            try:
                self._adapter.disconnect()
            except DeviceError:
                pass
        self._cancel_transfer.clear()
        self._detach_signal.clear()
        self._device_key = None
        self._transition(AppState.IDLE_DISCONNECTED)

    def _run_scan_and_drain(self) -> None:
        if self._device_key is None:
            raise DeviceNotConnected()
        self._transition(AppState.SCANNING)
        new_files = self._offloader.scan_new_files(self._device_key)
        if not new_files:
            self._transition(AppState.CONNECTED_IDLE)
            return

        self._transition(AppState.DRAINING)
        for file in new_files:
            if self._cancel_transfer.is_set() or self._stop_signal.is_set():
                return
            try:
                self._offloader.offload(
                    device_key=self._device_key,
                    file=file,
                    cancel_event=self._cancel_transfer,
                )
            except TransferAborted as exc:
                self._bus.publish(
                    Error(
                        message=f"Transfer aborted: {exc.reason}",
                        severity=Severity.WARNING,
                        context=file.name,
                    )
                )
                return
            except DeviceError as exc:
                self._bus.publish(
                    Error(message=str(exc), severity=Severity.ERROR, context=file.name)
                )
                return
        self._transition(AppState.CONNECTED_IDLE)
