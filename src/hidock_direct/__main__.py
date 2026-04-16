"""Entry point for `python -m hidock_direct` and the `hidock-direct` script.

Wires config -> lock -> store -> adapter -> watcher -> offloader -> app -> TUI
and blocks until SIGINT/SIGTERM.
"""

from __future__ import annotations

import signal
import sys

from .app import App
from .config import load_config
from .device import JensenDeviceAdapter
from .events import EventBus
from .locks import FileLock, LockHeld
from .offload import Offloader
from .state import StateStore
from .tui import TUI
from .usb_watcher import IOKitUSBWatcher


def main(argv: list[str] | None = None) -> int:  # noqa: ARG001 — argv kept for future flags
    try:
        config = load_config()
    except ValueError as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        return 2

    config.archive_dir.mkdir(parents=True, exist_ok=True)
    config.state_dir.mkdir(parents=True, exist_ok=True)
    config.tmp_dir.mkdir(parents=True, exist_ok=True)

    lock = FileLock(config.lock_path)
    try:
        lock.acquire()
    except LockHeld as exc:
        print(str(exc), file=sys.stderr)
        return 1

    bus = EventBus()
    store = StateStore(config.state_path)
    adapter = JensenDeviceAdapter()
    watcher = IOKitUSBWatcher()
    offloader = Offloader(
        adapter=adapter,
        store=store,
        bus=bus,
        archive_dir=config.archive_dir,
        tmp_dir=config.tmp_dir,
        delete_after_offload=config.delete_from_device_after_offload,
    )
    app = App(
        adapter=adapter,
        watcher=watcher,
        offloader=offloader,
        store=store,
        bus=bus,
        poll_interval_seconds=config.poll_interval_seconds,
    )
    tui = TUI(bus=bus)

    def _shutdown(signum, frame):  # noqa: ARG001
        app.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    tui.start()
    try:
        app.run()
    finally:
        tui.stop()
        lock.release()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
