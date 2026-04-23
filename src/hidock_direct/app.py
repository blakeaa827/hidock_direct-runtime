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

from .classify import RecordingKind
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


# libusb error fragments that mean "another userland process holds the USB
# interface claim." The canonical culprit in this project is the HiDock web
# interface (Chrome/WebUSB); other HiDock-aware apps can also trigger it.
# Errno 13 = EACCES, 16 = EBUSY, 19 = ENODEV (mid-transfer reset race).
_COMPETING_CLAIM_MARKERS = (
    "Access denied",
    "No such device",
    "Errno 19",
    "Errno 13",
    "Errno 16",
    "Resource busy",
    "Device busy",
)

# Jensen raises `ConnectionError("Device health check failed")` when the first
# get_device_info probe after a successful interface claim times out or returns
# empty. Empirically this happens when the device was just released by another
# app (HiDock web interface) and hasn't fully re-settled, or after a stale
# session left the firmware in an odd state. A physical replug resolves it.
_UNSETTLED_DEVICE_MARKERS = (
    "health check failed",
    "Health check failed",
)


def _translate_connect_error(raw: str) -> str:
    """Rewrite libusb/Jensen connect failures into operator-actionable messages.

    Two translation classes today:
    - competing-claim: another userland process holds the USB interface
    - unsettled-device: Jensen claimed the interface but the first probe failed

    Non-matching errors pass through unchanged so the real failure text still
    reaches the TUI.
    """
    if any(marker in raw for marker in _COMPETING_CLAIM_MARKERS):
        return (
            "HiDock USB interface is claimed by another process "
            "(likely the HiDock web interface open in a browser tab). "
            "Close that browser tab (or any other HiDock-aware app) "
            "and replug the device. Raw error: " + raw
        )
    if any(marker in raw for marker in _UNSETTLED_DEVICE_MARKERS):
        return (
            "HiDock did not respond to the startup health check. "
            "This usually clears with a physical replug -- especially right "
            "after another app (like the HiDock web interface) was using the "
            "device. Unplug the HiDock, wait ~5 seconds, and plug it back in. "
            "Raw error: " + raw
        )
    return raw


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

        # Latest classified scan buckets — read by the TUI for the footer
        # whisper/unknown counts and the modal/prompt handlers. Cleared on
        # disconnect so a new device doesn't see stale entries.
        self._pending_whispers: list = []
        self._pending_unknowns: list = []

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
        if self._device_key is None:
            # No attach ever completed -- publishing DeviceDetached with a
            # placeholder serial confuses the operator into thinking a real
            # device event happened. The failed attach already published its
            # own WARNING; the spurious detach adds noise, not signal.
            return
        self._bus.publish(DeviceDetached(serial=self._device_key.serial))

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
            except (DeviceNotConnected, ConnectionError):
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
        except (DeviceError, ConnectionError) as exc:
            self._bus.publish(Error(
                message=_translate_connect_error(str(exc)),
                severity=Severity.WARNING,
                context="connect",
            ))
            self._transition(AppState.IDLE_DISCONNECTED)
            return
        self._device_key = DeviceKey(model=info.model, serial=info.serial)
        self._store.register_device(self._device_key)
        self._bus.publish(DeviceAttached(model=info.model, serial=info.serial))
        self._transition(AppState.CONNECTED_IDLE)

    # -- whisper / unknown routing (TUI entry points) -------------------

    def offload_whisper(self, device_filename: str) -> bool:
        """Offload a single whisper by name from the pending bucket.

        Returns True on success, False if the filename is no longer in the
        pending bucket or the pipeline aborted. Publishes an operator-
        actionable `Error` on failure per PRD §2.7.
        """
        return self._offload_pending(
            device_filename,
            bucket=self._pending_whispers,
            kind=RecordingKind.WHISPER,
            failure_context="whisper_offload",
        )

    def route_unknown(self, device_filename: str, as_kind: RecordingKind) -> bool:
        """Route a file in the unknown bucket as MEETING or WHISPER."""
        return self._offload_pending(
            device_filename,
            bucket=self._pending_unknowns,
            kind=as_kind,
            failure_context="unknown_route",
        )

    def _offload_pending(self, device_filename: str, *, bucket: list, kind: RecordingKind, failure_context: str) -> bool:
        if self._device_key is None:
            return False
        target = next((f for f in bucket if f.name == device_filename), None)
        if target is None:
            return False
        try:
            self._offloader.offload_one(
                device_key=self._device_key,
                file=target,
                kind=kind,
                cancel_event=self._cancel_transfer,
            )
        except TransferAborted as exc:
            # A transfer-aborted mid-stream is the only class that leaves the
            # file on the device in a retriable state. The operator should be
            # told what failed and what to do next, not the raw adapter text.
            reason = getattr(exc, "reason", None) or str(exc) or "transfer aborted"
            action = "meeting offload" if kind is RecordingKind.MEETING else "whisper offload"
            self._bus.publish(Error(
                message=(
                    f"{action.capitalize()} failed: {reason}. "
                    f"{device_filename} remains on the device -- retry from the "
                    "TUI, or check the USB connection and replug if needed."
                ),
                severity=Severity.WARNING,
                context=failure_context,
            ))
            return False
        except DeviceError as exc:
            action = "meeting offload" if kind is RecordingKind.MEETING else "whisper offload"
            self._bus.publish(Error(
                message=(
                    f"{action.capitalize()} failed: {exc}. "
                    f"{device_filename} remains on the device. "
                    "Check that the HiDock is still connected; if another app "
                    "took the USB interface, close it and replug."
                ),
                severity=Severity.ERROR,
                context=failure_context,
            ))
            return False
        # Success -- remove the file from the pending bucket so the TUI's
        # footer count reflects the remaining queue.
        try:
            bucket.remove(target)
        except ValueError:
            pass
        return True

    def _handle_disconnect(self) -> None:
        if self._adapter.is_connected():
            try:
                self._adapter.disconnect()
            except DeviceError:
                pass
        self._cancel_transfer.clear()
        self._detach_signal.clear()
        self._device_key = None
        self._pending_whispers = []
        self._pending_unknowns = []
        self._transition(AppState.IDLE_DISCONNECTED)

    def _run_scan_and_drain(self) -> None:
        if self._device_key is None:
            raise DeviceNotConnected()
        self._transition(AppState.SCANNING)
        scan = self._offloader.scan_pending_files(self._device_key)
        # Expose the classified scan for the TUI's whisper/unknown modals.
        # Stored unconditionally (including empty lists) so the footer can
        # clear stale counts when a device is re-scanned.
        self._pending_whispers = scan.whispers
        self._pending_unknowns = scan.unknowns
        if not scan.meetings:
            self._transition(AppState.CONNECTED_IDLE)
            return

        self._transition(AppState.DRAINING)
        for file in scan.meetings:
            if self._cancel_transfer.is_set() or self._stop_signal.is_set():
                return
            try:
                self._offloader.offload(
                    device_key=self._device_key,
                    file=file,
                    cancel_event=self._cancel_transfer,
                    kind=RecordingKind.MEETING,
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
