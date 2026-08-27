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
from contextlib import contextmanager
from enum import Enum
from typing import Callable, Iterator, Optional

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
        # Last-observed device file count. `get_file_count` is cheap (~0.2s
        # vs ~27s for `list_files` on a 1201-file P1) so we poll the count
        # every tick and run a full classified scan only when it changes
        # (or on first-after-attach, signalled by `None`). Leaves the app
        # in CONNECTED_IDLE continuously, keeping `w`/`u` keys responsive.
        self._last_known_count: Optional[int] = None
        # Device mutual exclusion with a live transcription session. An Event,
        # not a counter -- see `suspend_device_polling` for why the difference
        # is load-bearing. Set from the live controller's thread and read from
        # the worker thread, which is what an Event is for.
        self._polling_suspended = threading.Event()
        # In-flight Jensen command tracking. `_polling_suspended` stops the
        # *next* command from being issued; it says nothing about one already
        # on the wire. `get_file_count` is issued from CONNECTED_IDLE, before
        # the transition to SCANNING, so the state machine alone reports the
        # endpoint as free for that command's whole duration.
        #
        # A depth counter behind a Condition, not an Event: commands are issued
        # from two threads (the worker loop, and the TUI thread via
        # `_offload_pending`), so a single flag cleared by whichever finishes
        # first would declare the endpoint free while the other still holds it.
        # Every increment is paired with a decrement in a `finally`, so the
        # counter cannot drift the way an unbalanced suspend/resume pair would.
        self._device_command_cv = threading.Condition()
        self._device_command_depth = 0

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
        self._watcher.on_degraded(self._on_degraded)
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

    # -- device mutual exclusion (live transcription) -------------------

    @contextmanager
    def _device_command(self) -> Iterator[None]:
        """Mark the Jensen endpoint as held for the duration of one adapter call.

        Wraps every command this app issues to the device, from either thread,
        so `device_busy` covers commands that are on the wire *now* and not only
        the states that imply a long-running one. The decrement is in a
        `finally`: a raising command still releases the endpoint, otherwise one
        `DeviceError` would report the device as permanently busy and refuse
        every live session for the life of the process.
        """
        with self._device_command_cv:
            self._device_command_depth += 1
        try:
            yield
        finally:
            with self._device_command_cv:
                self._device_command_depth -= 1
                if self._device_command_depth <= 0:
                    self._device_command_depth = 0
                    self._device_command_cv.notify_all()

    def suspend_device_polling(self, timeout: float = 5.0) -> bool:
        """Stop issuing Jensen commands from the worker loop for the duration
        of a live transcription session (live-surface PRD FR-6.1).

        `_run_scan_and_drain` calls `adapter.get_file_count()` and a live
        `RealtimeSession` issues CMD 33/34 through `adapter._jensen` -- the
        same USB endpoint on the same device. Two threads issuing Jensen
        commands concurrently interleave request/response pairs, so exactly
        one consumer drives the device at a time.

        A flag, not a nesting counter. Suspend and resume are each called from
        more than one path (session start, session stop, the capture pump's
        `finally`), and an unbalanced pair on a counter leaves the worker
        suspended for the life of the process -- a failure whose only symptom
        is the *absence* of offloads, which is the hardest kind to notice.
        Both directions are therefore idempotent by construction. (The
        `_device_command_depth` counter below is a different quantity -- one
        in-flight command, always released in a `finally` -- and does not make
        the suspension itself re-entrant.)

        The loop itself keeps running: attach/detach handling, shutdown, and
        the state machine are untouched. Only the device poll is skipped.

        Setting the flag is not sufficient on its own. The flag is read at the
        top of `_run_scan_and_drain`, so it stops the *next* command; a command
        already on the wire -- typically the `get_file_count` issued from
        CONNECTED_IDLE -- runs to completion regardless. Fire-and-forget
        suspension therefore still permits CMD 32 START to interleave with an
        outstanding poll on the same endpoint. So this sets the flag first (no
        new command can start) and then waits for the in-flight one to drain.

        The wait is bounded, never indefinite: pressing `l` must not hang the
        operator's UI behind a transfer that could take minutes. On timeout the
        suspension still stands -- releasing it would be strictly worse -- and
        the operator is told on the bus, because a silent overlap surfaces later
        as an unexplained capture failure.

        Returns True when the endpoint was confirmed idle before returning,
        False when the bounded wait expired (suspended either way).
        """
        self._polling_suspended.set()
        with self._device_command_cv:
            drained = self._device_command_cv.wait_for(
                lambda: self._device_command_depth == 0, timeout=timeout
            )
        if not drained:
            self._bus.publish(Error(
                message=(
                    "Live session claimed the HiDock while a device command was "
                    f"still in flight (waited {timeout:g}s). Offload polling is "
                    "suspended, but the first moments of live capture may "
                    "interleave with that command. If capture fails to start, "
                    "stop the live session, let the transfer finish, and start "
                    "it again."
                ),
                severity=Severity.WARNING,
                context="live_session",
            ))
        return drained

    def resume_device_polling(self) -> None:
        """Release the FR-6.1 suspension. Idempotent, and safe when nothing was
        ever suspended: the live controller calls this from a `finally` that
        also runs when the session failed *before* suspending (FR-6.3), so a
        guard that raised there would invert the guarantee it protects.
        """
        self._polling_suspended.clear()

    @property
    def device_busy(self) -> bool:
        """True while this worker is holding the Jensen endpoint (FR-6.2).

        SCANNING runs `list_files` (~27 s on a 1201-file P1) and DRAINING
        streams a file off the device; both own the endpoint for their whole
        duration, so a live session started in either would interleave with an
        in-flight transfer. CONNECTED_IDLE holds nothing *between* polls and
        IDLE_DISCONNECTED has no device at all -- refusing there would make
        live transcription unusable in the app's normal state.

        The state alone is not the whole predicate. `get_file_count` is issued
        from CONNECTED_IDLE, before the transition to SCANNING, and the single-
        file `_offload_pending` path (the `w`/`u` keys) streams from the TUI
        thread without transitioning at all. Both hold the endpoint in a state
        this table calls free, so the in-flight command depth is read too --
        it is what makes "between polls" mean between and not during.

        A property rather than a stored flag, because the live controller holds
        `lambda: app.device_busy`: a value captured at wiring time would report
        the state at launch, forever.
        """
        if self.state in (AppState.SCANNING, AppState.DRAINING):
            return True
        with self._device_command_cv:
            return self._device_command_depth > 0

    # -- watcher callbacks ---------------------------------------------

    def _on_attach(self, vid: int, pid: int) -> None:  # noqa: ARG002
        self._cancel_transfer.clear()
        self._detach_signal.clear()
        self._attach_signal.set()

    def _on_degraded(self) -> None:
        """The P1 is on the bus as an audio device but its data interface is not.

        Without this the app publishes nothing at all in this state: the Jensen
        snapshot is empty, so no attach fires and `_translate_connect_error` is
        never reached. The operator sees a dead app while macOS shows the P1 as
        their active microphone. Name the remedy explicitly — a replug does NOT
        clear this; the device needs a power cycle.
        """
        self._bus.publish(Error(
            message=(
                "HiDock detected as an audio device, but its data interface is "
                "not responding — offload and transcription are unavailable. "
                "Power cycle the device (a replug is not sufficient)."
            ),
            severity=Severity.WARNING,
            context="presence",
        ))

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
            with self._device_command():
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
        # FR-5.5 / FR-6.1, the other direction. `_run_scan_and_drain` already
        # refuses to poll while a live session holds the endpoint, but this is
        # the app's second consumer of it: the `w` and `u` keys stream a file
        # off the device from the TUI thread, on the same Jensen endpoint, and
        # nothing in the state machine stands between them and a live capture.
        # An exclusion that only holds one way is not an exclusion.
        #
        # Refused, not queued: a silent queue means the transfer starts minutes
        # later with no operator action, which is exactly the surprise FR-6.2
        # rejects at the other door. Returning False (rather than raising)
        # leaves the bucket untouched, so the file stays listed and the footer
        # count still matches what is actually on the device.
        if self._polling_suspended.is_set():
            action = "meeting offload" if kind is RecordingKind.MEETING else "whisper offload"
            self._bus.publish(Error(
                message=(
                    f"{action.capitalize()} refused: a live transcription session "
                    "is holding the HiDock's USB interface, and the device can "
                    f"only be driven by one at a time. {device_filename} was not "
                    "transferred and is still on the device. Stop the live "
                    "session (press l), then offload it again."
                ),
                severity=Severity.WARNING,
                context=failure_context,
            ))
            return False
        if self._device_key is None:
            return False
        target = next((f for f in bucket if f.name == device_filename), None)
        if target is None:
            return False
        try:
            with self._device_command():
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
        with self._device_command():
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
        self._last_known_count = None
        self._transition(AppState.IDLE_DISCONNECTED)

    def _run_scan_and_drain(self) -> None:
        # FR-6.1: a live transcription session holds the Jensen endpoint. The
        # check lives HERE, immediately above the command that would be issued,
        # rather than in the loop's scaffolding -- a flag consulted only by the
        # caller still lets every other entry into this method interleave USB
        # traffic with live capture. Returning (rather than raising
        # DeviceNotConnected) keeps the state machine where it is: the device
        # is present and healthy, it is simply claimed by someone else.
        if self._polling_suspended.is_set():
            return
        if self._device_key is None:
            raise DeviceNotConnected()
        # Count-delta gate. `get_file_count` is cheap; run the expensive
        # `list_files` scan only when something actually changed on the
        # device (or on first-after-attach when `_last_known_count` is None).
        with self._device_command():
            current_count = self._adapter.get_file_count()
        if self._last_known_count is not None and current_count == self._last_known_count:
            # Nothing new on the device. Stay in CONNECTED_IDLE so key
            # bindings remain active. No bus events -- the last scan's
            # pending buckets are still valid.
            return
        self._last_known_count = current_count
        self._transition(AppState.SCANNING)
        with self._device_command():
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
                with self._device_command():
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
