"""Entry point for `python -m hidock_direct` and the `hidock-direct` script.

Wires config -> lock -> store -> adapter -> watcher -> offloader -> app -> TUI
and blocks until SIGINT/SIGTERM.
"""

from __future__ import annotations

import signal
import sys

# Run BEFORE importing any editable-installed sibling (diarize_audio,
# blake_commons). iCloud's UF_HIDDEN on __editable__*.pth makes site.py
# skip them silently; this reinserts the paths into sys.path.
from . import _pth_bootstrap

_pth_bootstrap.bootstrap()

from .app import App
from .config import load_config
from .device import JensenDeviceAdapter
from .events import EventBus, TranscribeSkipped
from .locks import FileLock, LockHeld
from .offload import Offloader
from .state import StateStore
from .tui import TUI
from .usb_watcher import PollingUSBWatcher


def _preflight_transcribe(bus: EventBus) -> None:
    """Fail loud if `TRANSCRIBE_ON_OFFLOAD=true` but diarize_audio is missing.

    Publishes a `TranscribeSkipped` so the TUI surfaces it, and prints a
    banner to stderr so the operator sees it even before the TUI takes
    over the screen.
    """
    try:
        import diarize_audio  # noqa: F401
        return
    except ImportError as exc:
        msg = (
            "⚠️  TRANSCRIBE_ON_OFFLOAD=true but `diarize_audio` is not importable "
            f"({exc}).\n   Offloads will succeed but will NOT be transcribed.\n"
            "   Likely cause: iCloud flagged __editable__*.pth UF_HIDDEN.\n"
            "   Remediation: re-run `pip install -e` or `chflags nohidden` on the .pth files."
        )
        print(msg, file=sys.stderr, flush=True)
        bus.publish(
            TranscribeSkipped(
                device_filename="(startup)",
                reason="diarize_audio not importable at startup",
            )
        )


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
    watcher = PollingUSBWatcher()
    offloader = Offloader(
        adapter=adapter,
        store=store,
        bus=bus,
        archive_dir=config.archive_dir,
        tmp_dir=config.tmp_dir,
        delete_after_offload=config.delete_from_device_after_offload,
        transcribe_on_offload=config.transcribe_on_offload,
    )
    app = App(
        adapter=adapter,
        watcher=watcher,
        offloader=offloader,
        store=store,
        bus=bus,
        poll_interval_seconds=config.poll_interval_seconds,
    )
    tui = TUI(
        bus=bus,
        app=app,
        pending_whispers_provider=lambda: app._pending_whispers,
        pending_unknowns_provider=lambda: app._pending_unknowns,
    )

    if config.transcribe_on_offload:
        _preflight_transcribe(bus)

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
