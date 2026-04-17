"""Stable device surface over the vendored Jensen protocol.

The rest of the app (offload pipeline, state machine, TUI) talks to this
module's abstract `DeviceAdapter` protocol — never directly to the vendored
`HiDockJensen` class. This is the seam that lets us mock the device in tests
without pulling in pyusb.
"""

from __future__ import annotations

import re
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, List, Optional, Protocol

from .jensen import (
    ALL_VENDOR_IDS,
    DEFAULT_VENDOR_ID,
    HIDOCK_PRODUCT_IDS,
    HiDockJensen,
    PRODUCT_ID_MODEL_MAP,
)


SAFE_DEVICE_FILENAME = re.compile(r"^[A-Za-z0-9._-]+$")


class DeviceError(RuntimeError):
    """Raised for any device-layer failure that the app cares about."""


class DeviceNotConnected(DeviceError):
    pass


class TransferAborted(DeviceError):
    """Raised when a download stream does not complete OK (cancelled, detach, comms failure)."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class DeviceInfo:
    model: str
    serial: str


@dataclass(frozen=True)
class DeviceFile:
    name: str
    size: int
    # Device-reported recording time. May be None if neither the filename nor
    # metadata yields a timestamp; callers must fall back to file mtime.
    device_mtime: Optional[datetime]


class DeviceAdapter(Protocol):
    """The surface offload.py and app.py depend on."""

    def connect(self) -> DeviceInfo: ...
    def disconnect(self) -> None: ...
    def is_connected(self) -> bool: ...
    def list_files(self) -> List[DeviceFile]: ...
    def download_file(
        self,
        name: str,
        size: int,
        *,
        on_chunk: Callable[[bytes], None],
        on_progress: Optional[Callable[[int, int], None]] = None,
        cancel_event: Optional[threading.Event] = None,
    ) -> None: ...
    def delete_file(self, name: str) -> None: ...


def sanitize_device_filename(name: str) -> str:
    """Validate a device-reported filename is safe to use in paths.

    Whitelist of `[A-Za-z0-9._-]+` per PRD §7. Rejects `..`, slashes, shell
    metachars, and anything else that could escape the staging or archive
    directories.
    """
    if not name or not SAFE_DEVICE_FILENAME.fullmatch(name):
        raise ValueError(f"Unsafe device filename: {name!r}")
    if ".." in name:
        raise ValueError(f"Unsafe device filename (contains '..'): {name!r}")
    return name


def model_from_pid(pid: int) -> str:
    return PRODUCT_ID_MODEL_MAP.get(pid, f"hidock-unknown-{pid:#06x}")


class JensenDeviceAdapter:
    """Concrete adapter: wraps `HiDockJensen` and discovers the device via pyusb.

    Intentionally lazy about the USB backend so the module imports cleanly in
    test environments where libusb or pyusb may not be installed.
    """

    def __init__(self, usb_backend=None):
        self._backend = usb_backend
        self._jensen: Optional[HiDockJensen] = None
        self._info: Optional[DeviceInfo] = None
        self._vid: Optional[int] = None
        self._pid: Optional[int] = None

    @staticmethod
    def _discover_backend():
        import usb.backend.libusb1  # lazy

        backend = usb.backend.libusb1.get_backend()
        if backend is None:
            raise DeviceError(
                "libusb backend unavailable. Install libusb via Homebrew: brew install libusb"
            )
        return backend

    @staticmethod
    def enumerate_attached() -> List[tuple[int, int]]:
        """Return (vid, pid) pairs for any attached HiDock device."""
        try:
            import usb.core  # lazy
        except ImportError as exc:
            raise DeviceError("pyusb not installed") from exc
        hits: List[tuple[int, int]] = []
        for vid in ALL_VENDOR_IDS:
            for dev in usb.core.find(find_all=True, idVendor=vid) or []:
                if dev.idProduct in HIDOCK_PRODUCT_IDS:
                    hits.append((vid, dev.idProduct))
        return hits

    def connect(self) -> DeviceInfo:
        if self._jensen is None:
            if self._backend is None:
                self._backend = self._discover_backend()
            self._jensen = HiDockJensen(self._backend)
        attached = self.enumerate_attached()
        if not attached:
            raise DeviceNotConnected("No HiDock device attached")
        vid, pid = attached[0]
        self._vid, self._pid = vid, pid
        ok, err = self._jensen.connect(target_interface_number=0, vid=vid, pid=pid)
        if not ok:
            raise DeviceError(f"Failed to connect to device: {err}")
        info_dict = self._jensen.get_device_info() or {}
        serial = info_dict.get("sn") or "UNKNOWN"
        self._info = DeviceInfo(model=model_from_pid(pid), serial=serial)
        return self._info

    def disconnect(self) -> None:
        if self._jensen is not None:
            try:
                self._jensen.disconnect()
            finally:
                self._jensen = None
                self._info = None

    def is_connected(self) -> bool:
        return self._jensen is not None and self._jensen.is_connected()

    def _require(self) -> HiDockJensen:
        if self._jensen is None or not self._jensen.is_connected():
            raise DeviceNotConnected()
        return self._jensen

    def list_files(self) -> List[DeviceFile]:
        jensen = self._require()
        try:
            result = jensen.list_files() or {}
        except ConnectionError as exc:
            raise DeviceNotConnected(str(exc)) from exc
        if result.get("error"):
            raise DeviceError(f"list_files failed: {result['error']}")
        out: List[DeviceFile] = []
        for f in result.get("files", []):
            try:
                name = sanitize_device_filename(f["name"])
            except ValueError:
                # Skip unparseable/unsafe names rather than raising — the
                # device may return transient junk entries. Log via event bus
                # at a higher layer if needed.
                continue
            out.append(
                DeviceFile(
                    name=name,
                    size=int(f.get("length", 0)),
                    device_mtime=f.get("time"),
                )
            )
        return out

    def download_file(
        self,
        name: str,
        size: int,
        *,
        on_chunk: Callable[[bytes], None],
        on_progress: Optional[Callable[[int, int], None]] = None,
        cancel_event: Optional[threading.Event] = None,
    ) -> None:
        jensen = self._require()
        safe = sanitize_device_filename(name)
        try:
            status = jensen.stream_file(
                safe,
                size,
                data_callback=on_chunk,
                progress_callback=on_progress,
                cancel_event=cancel_event,
            )
        except ConnectionError as exc:
            raise DeviceNotConnected(str(exc)) from exc
        if status != "OK":
            raise TransferAborted(status)

    def delete_file(self, name: str) -> None:
        jensen = self._require()
        safe = sanitize_device_filename(name)
        try:
            result = jensen.delete_file(safe)
        except ConnectionError as exc:
            raise DeviceNotConnected(str(exc)) from exc
        if isinstance(result, dict) and result.get("error"):
            raise DeviceError(f"delete_file failed: {result['error']}")
