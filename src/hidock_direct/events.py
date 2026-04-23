"""Typed events and an in-process event bus.

Load-bearing decoupling between the offload pipeline (producer) and the TUI
(consumer). Any phase-2 consumer (menu-bar app, log tee, tests) subscribes to
the same bus — the pipeline never talks to a specific UI.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from threading import RLock
from typing import Callable, List, Optional

from .classify import RecordingKind

Subscriber = Callable[["Event"], None]


class Severity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass(frozen=True)
class Event:
    """Base class. Never published directly."""


@dataclass(frozen=True)
class DeviceAttached(Event):
    model: str
    serial: str


@dataclass(frozen=True)
class DeviceDetached(Event):
    serial: str


@dataclass(frozen=True)
class ScanStarted(Event):
    pass


@dataclass(frozen=True)
class ScanComplete(Event):
    new_file_count: int


@dataclass(frozen=True)
class FileDiscovered(Event):
    device_filename: str
    size_bytes: int


@dataclass(frozen=True)
class DownloadStarted(Event):
    device_filename: str
    target_path: str


@dataclass(frozen=True)
class DownloadProgress(Event):
    device_filename: str
    bytes_done: int
    bytes_total: int


@dataclass(frozen=True)
class DownloadComplete(Event):
    device_filename: str
    archive_path: str
    sha256: str


@dataclass(frozen=True)
class TransferAborted(Event):
    device_filename: str
    reason: str


@dataclass(frozen=True)
class TranscribeSkipped(Event):
    device_filename: str
    reason: str


@dataclass(frozen=True)
class TranscribeStarted(Event):
    device_filename: str


@dataclass(frozen=True)
class TranscribeComplete(Event):
    device_filename: str
    drive_file_id: Optional[str]


@dataclass(frozen=True)
class TranscribeFailed(Event):
    device_filename: str
    reason: str


@dataclass(frozen=True)
class IdleWaiting(Event):
    state: str


@dataclass(frozen=True)
class WhispersDetected(Event):
    count: int


@dataclass(frozen=True)
class UnknownsDetected(Event):
    count: int
    filenames: List[str]


@dataclass(frozen=True)
class WhisperOffloadRequested(Event):
    device_filename: str


@dataclass(frozen=True)
class UnknownRouted(Event):
    device_filename: str
    as_kind: RecordingKind


@dataclass(frozen=True)
class Error(Event):
    message: str
    severity: Severity
    context: Optional[str] = None


class EventBus:
    """Fan-out bus. A subscriber that raises does not disrupt the others."""

    def __init__(self, on_subscriber_error: Optional[Callable[[BaseException, Subscriber], None]] = None):
        self._subs: List[Subscriber] = []
        self._lock = RLock()
        self._on_subscriber_error = on_subscriber_error

    def subscribe(self, fn: Subscriber) -> None:
        with self._lock:
            self._subs.append(fn)

    def unsubscribe(self, fn: Subscriber) -> None:
        with self._lock:
            try:
                self._subs.remove(fn)
            except ValueError:
                pass

    def publish(self, event: Event) -> None:
        with self._lock:
            subs = list(self._subs)
        for sub in subs:
            try:
                sub(event)
            except BaseException as exc:
                if self._on_subscriber_error is not None:
                    try:
                        self._on_subscriber_error(exc, sub)
                    except BaseException:
                        pass
