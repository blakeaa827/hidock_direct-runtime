"""Regression: the app must not go silently blind when only the P1's AUDIO
identity is enumerated.

The HiDock P1 presents as TWO independent USB devices behind an internal hub:

    0x10D6:0xB00E   vendor-specific (class 255)  -- Jensen control + realtime
    0x1395:0x005D   audio (class 1) + HID (3)    -- UAC mic / speaker

They enumerate independently. Observed live 2026-08-26: after a replug the audio
identity came up and the Jensen identity did not, for 48+ seconds. macOS showed
HiDock P1 as the default input AND output the whole time, while both of our
detection paths reported nothing -- and because the watcher's enumerator returned
an empty snapshot, no attach fired, so the app published NO event at all.

The defect is the silence. A fix that classifies the state correctly but still
emits nothing has not fixed the bug --
`test_half_enumerated_state_publishes_an_operator_message` is the load-bearing
assertion here.

Bug report: projects/hidock_direct/planning/bug_report_app_blind_to_half_enumerated_device.md
"""

from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import List, Tuple

import pytest

from hidock_direct.app import App
from hidock_direct.device import (
    HIDOCK_AUDIO_VENDOR_IDS,
    Presence,
    probe_presence,
)
from hidock_direct.events import Error, Event, EventBus
from hidock_direct.jensen import ALL_VENDOR_IDS
from hidock_direct.offload import Offloader
from hidock_direct.state import StateStore
from hidock_direct.usb_watcher import PollingUSBWatcher

from tests.fixtures.mock_device import MockDevice

JENSEN = (0x10D6, 0xB00E)   # Actions Semiconductor -- control interface
AUDIO = (0x1395, 0x005D)    # audio chipset -- UAC mic/speaker


def _enumerating(*pairs: Tuple[int, int]):
    """A stand-in for a full, UNFILTERED usb.core enumeration."""
    return lambda: list(pairs)


# -- presence probe -----------------------------------------------------


def test_half_enumerated_device_is_not_reported_absent():
    """Audio identity up, Jensen identity down: present-but-unreachable.

    This is the test that fails before the fix -- the old code had no notion of
    this state and collapsed it into 'nothing attached'.
    """
    assert probe_presence(_enumerating(AUDIO)) is Presence.HALF_ENUMERATED


def test_fully_enumerated_device_is_ready():
    """Guards against over-correcting into never reporting READY."""
    assert probe_presence(_enumerating(JENSEN, AUDIO)) is Presence.READY


def test_no_device_is_absent():
    """The null case. Without it, a probe that always returns HALF_ENUMERATED
    would satisfy the first test and still be wrong."""
    assert probe_presence(_enumerating()) is Presence.ABSENT
    assert probe_presence(_enumerating((0x05AC, 0x0503))) is Presence.ABSENT


def test_jensen_only_is_ready_even_without_the_audio_identity():
    """The audio identity is not required for reachability -- the P1 exposes it
    only after BlueCatch connects (FAQ Q2), so a plain USB plug-in is READY."""
    assert probe_presence(_enumerating(JENSEN)) is Presence.READY


def test_presence_matches_on_vendor_id_only_not_product_id():
    """A future P1 revision with a different audio PID must still register as
    present. Pins the bug report's 'match by vendor ID only' constraint."""
    assert probe_presence(_enumerating((0x1395, 0xBEEF))) is Presence.HALF_ENUMERATED


# -- the defect is silence ----------------------------------------------


class _DegradableWatcher:
    """FakeWatcher that can also fire the degraded (half-enumerated) callback."""

    def __init__(self):
        self._attach = []
        self._detach = []
        self._degraded = []
        self.started = False

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        pass

    def on_attach(self, fn) -> None:
        self._attach.append(fn)

    def on_detach(self, fn) -> None:
        self._detach.append(fn)

    def on_degraded(self, fn) -> None:
        self._degraded.append(fn)

    def fire_degraded(self) -> None:
        for fn in self._degraded:
            fn()


def _build_app(tmp_path: Path):
    archive = tmp_path / "archive"
    archive.mkdir(parents=True, exist_ok=True)
    events: List[Event] = []
    bus = EventBus()
    bus.subscribe(events.append)
    store = StateStore(tmp_path / "state.json")
    mock = MockDevice(files=[])
    watcher = _DegradableWatcher()
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
    return app, events, watcher


def test_half_enumerated_state_publishes_an_operator_message(tmp_path: Path):
    """THE LOAD-BEARING TEST.

    The bug is not 'the app computes the wrong state' -- it is 'the app says
    nothing'. Assert an operator-visible event actually reaches the bus, and
    that it names the remedy (power-cycle), because a replug is NOT sufficient
    to recover this state.
    """
    app, events, watcher = _build_app(tmp_path)
    runner = threading.Thread(target=app.run, daemon=True)
    runner.start()
    deadline = time.time() + 3.0
    while time.time() < deadline and not watcher.started:
        time.sleep(0.02)
    assert watcher.started, "app never started the watcher"

    watcher.fire_degraded()

    deadline = time.time() + 3.0
    errs: List[Error] = []
    while time.time() < deadline:
        errs = [e for e in events if isinstance(e, Error)]
        if errs:
            break
        time.sleep(0.02)

    assert errs, "half-enumerated device produced NO operator-visible event (the bug)"
    msg = errs[0].message.lower()
    assert "power" in msg and "cycle" in msg, (
        f"message must name the power-cycle remedy; got: {errs[0].message!r}"
    )


def test_app_registers_a_degraded_handler_with_the_watcher(tmp_path: Path):
    """Wiring pin: if App stops registering the handler, the message above can
    never fire in production even though the unit test above still passes."""
    app, _events, watcher = _build_app(tmp_path)
    runner = threading.Thread(target=app.run, daemon=True)
    runner.start()
    deadline = time.time() + 3.0
    while time.time() < deadline and not watcher.started:
        time.sleep(0.02)
    assert watcher._degraded, "App.run() did not register an on_degraded handler"


# -- the audio VID must not leak into the Jensen connect path ------------


def test_audio_vendor_id_is_not_in_the_jensen_connect_path():
    """Adding 0x1395 to ALL_VENDOR_IDS would point connect() at a device it
    cannot speak to -- turning a clean 'not present' into a confusing open
    failure. It would also be reverted by refresh_jensen.sh, since that
    constant lives in the vendored tree. Pin the decision so it cannot be
    undone by inspection."""
    assert 0x1395 not in ALL_VENDOR_IDS
    assert set(HIDOCK_AUDIO_VENDOR_IDS).isdisjoint(set(ALL_VENDOR_IDS))


def test_watcher_does_not_emit_attach_for_the_audio_identity():
    """The watcher must not treat the audio device as an attachable HiDock --
    a bogus attach would drive connect() against an unreachable device."""
    attaches: List[Tuple[int, int]] = []
    degraded: List[bool] = []
    watcher = PollingUSBWatcher(
        poll_interval_seconds=0.05,
        enumerate_fn=lambda: [],            # Jensen-filtered view: nothing
        enumerate_all_fn=_enumerating(AUDIO),  # unfiltered: audio present
    )
    watcher.on_attach(lambda vid, pid: attaches.append((vid, pid)))
    watcher.on_degraded(lambda: degraded.append(True))
    watcher.start()
    try:
        deadline = time.time() + 3.0
        while time.time() < deadline and not degraded:
            time.sleep(0.02)
    finally:
        watcher.stop()

    assert degraded, "watcher never reported the half-enumerated state"
    assert attaches == [], f"watcher emitted a bogus attach: {attaches}"


def test_watcher_reports_degraded_once_not_every_poll():
    """The state persists for as long as the device stays half-enumerated. If
    the watcher fired every tick it would flood the activity log -- the same
    class of surface the log-crop fix (971a2a8) was about."""
    degraded: List[bool] = []
    watcher = PollingUSBWatcher(
        poll_interval_seconds=0.05,
        enumerate_fn=lambda: [],
        enumerate_all_fn=_enumerating(AUDIO),
    )
    watcher.on_degraded(lambda: degraded.append(True))
    watcher.start()
    try:
        time.sleep(0.6)  # ~12 poll ticks
    finally:
        watcher.stop()

    assert len(degraded) == 1, f"expected one report, got {len(degraded)}"


# -- convention agreement (Gate 2 step 8) --------------------------------


def test_both_enumerators_filter_to_the_same_jensen_vendor_set():
    """`device.enumerate_attached` and `PollingUSBWatcher._default_enumerate`
    are two implementations of one rule: 'which vendor IDs are Jensen-reachable'.

    They agree today only because both happen to read ALL_VENDOR_IDS. Nothing
    fails if a future edit widens one and not the other -- and the copy that is
    not being edited is the one that stays wrong. Assert the invariant rather
    than trusting the convention.
    """
    seen_by_device: List[Tuple[int, int]] = []
    seen_by_watcher: List[Tuple[int, int]] = []

    class _Dev:
        def __init__(self, vid, pid):
            self.idVendor, self.idProduct = vid, pid

    # Every vendor either enumerator asks about, recorded.
    def make_find(sink):
        def _find(find_all=False, idVendor=None, **kw):
            if idVendor is not None:
                sink.append(idVendor)
            return []
        return _find

    import usb.core

    real = usb.core.find
    dev_vids: List[int] = []
    watch_vids: List[int] = []
    try:
        from hidock_direct.device import JensenDeviceAdapter
        usb.core.find = make_find(dev_vids)
        JensenDeviceAdapter.enumerate_attached()
        usb.core.find = make_find(watch_vids)
        PollingUSBWatcher._default_enumerate()
    finally:
        usb.core.find = real

    assert dev_vids, "device enumerator asked about no vendor IDs"
    assert set(dev_vids) == set(watch_vids), (
        f"the two Jensen enumerators disagree: device={sorted(set(dev_vids))} "
        f"watcher={sorted(set(watch_vids))}"
    )
    # And neither may drift into treating the audio identity as Jensen.
    assert set(HIDOCK_AUDIO_VENDOR_IDS).isdisjoint(set(dev_vids))
