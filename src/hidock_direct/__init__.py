"""hidock-direct — local USB offload for HiDock devices.

Public surface:

    from hidock_direct import App, EventBus, __version__

See `app.py` for the state machine and `offload.py` for the per-file pipeline.
"""

from .app import App, AppState
from .events import (
    DeviceAttached,
    DeviceDetached,
    DownloadComplete,
    DownloadProgress,
    DownloadStarted,
    Error,
    Event,
    EventBus,
    FileDiscovered,
    IdleWaiting,
    ScanComplete,
    ScanStarted,
    Severity,
    TransferAborted,
)

__version__ = "0.1.0"

__all__ = [
    "App",
    "AppState",
    "EventBus",
    "Event",
    "DeviceAttached",
    "DeviceDetached",
    "DownloadComplete",
    "DownloadProgress",
    "DownloadStarted",
    "Error",
    "FileDiscovered",
    "IdleWaiting",
    "ScanComplete",
    "ScanStarted",
    "Severity",
    "TransferAborted",
    "__version__",
]
