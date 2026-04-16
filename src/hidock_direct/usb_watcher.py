"""USB attach/detach watcher.

PRD §2.1 calls for IOKit via pyobjc, but IOKit framework bindings aren't a
standalone PyPI package — the pyobjc umbrella is ~200 MB and pulls in dozens
of unrelated frameworks. A polling watcher over pyusb hits the same
behavioral contract with zero extra dependencies: it enumerates USB devices
on an interval and fires attach/detach events on set changes. This is the
intended shape of a long-running macOS CLI that's already polling for files
every N seconds.

Tests never touch `PollingUSBWatcher` directly; they wire `app.App` with a
synthetic watcher implementing the same protocol.
"""

from __future__ import annotations

import threading
from typing import Callable, Iterable, List, Optional, Protocol, Set, Tuple

from .jensen import ALL_VENDOR_IDS, HIDOCK_PRODUCT_IDS


AttachCallback = Callable[[int, int], None]  # (vid, pid)
DetachCallback = Callable[[int, int], None]  # (vid, pid)


class USBWatcherProtocol(Protocol):
    def start(self) -> None: ...
    def stop(self) -> None: ...
    def on_attach(self, fn: AttachCallback) -> None: ...
    def on_detach(self, fn: DetachCallback) -> None: ...


class PollingUSBWatcher:
    """Poll pyusb on an interval; diff against the previous snapshot to emit events.

    The `enumerate_fn` hook exists so tests can drive enumeration synthetically
    without mocking `usb.core` — real callers should leave it as the default.
    """

    def __init__(
        self,
        *,
        vendor_ids: Iterable[int] = ALL_VENDOR_IDS,
        product_ids: Iterable[int] = tuple(HIDOCK_PRODUCT_IDS),
        poll_interval_seconds: float = 1.0,
        enumerate_fn: Optional[Callable[[], List[Tuple[int, int]]]] = None,
    ):
        self._vendor_ids = set(vendor_ids)
        self._product_ids = set(product_ids)
        self._poll_interval = max(0.1, poll_interval_seconds)
        self._enumerate = enumerate_fn or self._default_enumerate
        self._attach_handlers: List[AttachCallback] = []
        self._detach_handlers: List[DetachCallback] = []
        self._present: Set[Tuple[int, int]] = set()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def on_attach(self, fn: AttachCallback) -> None:
        self._attach_handlers.append(fn)

    def on_detach(self, fn: DetachCallback) -> None:
        self._detach_handlers.append(fn)

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="usb-watcher", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                snapshot = {pair for pair in self._enumerate() if self._matches(pair)}
            except Exception:
                snapshot = set()
            attached = snapshot - self._present
            detached = self._present - snapshot
            self._present = snapshot
            for vid, pid in attached:
                self._fire(self._attach_handlers, vid, pid)
            for vid, pid in detached:
                self._fire(self._detach_handlers, vid, pid)
            self._stop.wait(self._poll_interval)

    def _matches(self, pair: Tuple[int, int]) -> bool:
        vid, pid = pair
        return vid in self._vendor_ids and pid in self._product_ids

    @staticmethod
    def _fire(handlers: List[Callable[[int, int], None]], vid: int, pid: int) -> None:
        for fn in handlers:
            try:
                fn(vid, pid)
            except BaseException:
                pass

    @staticmethod
    def _default_enumerate() -> List[Tuple[int, int]]:
        try:
            import usb.core  # lazy so module imports stay lightweight
        except ImportError:
            return []
        hits: List[Tuple[int, int]] = []
        for vid in ALL_VENDOR_IDS:
            found = usb.core.find(find_all=True, idVendor=vid)
            if not found:
                continue
            for dev in found:
                try:
                    hits.append((int(dev.idVendor), int(dev.idProduct)))
                except AttributeError:
                    continue
        return hits
