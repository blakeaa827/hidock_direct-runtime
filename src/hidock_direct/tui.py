"""Rich-based TUI. Presenter only — no business logic.

Subscribes to the event bus, maintains a tiny view model (current device,
in-flight downloads, recent log, session counters), and renders via
`rich.live.Live`. Shut down cleanly when `stop()` is called.
"""

from __future__ import annotations

import os
import threading
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, TextColumn
from rich.table import Table
from rich.text import Text

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
    TranscribeComplete,
    TranscribeFailed,
    TranscribeSkipped,
    TranscribeStarted,
    TransferAborted,
)


RECENT_LOG_LIMIT = 10


def _home_relative(path: str) -> str:
    """Render an absolute path as ~-relative for the TUI (PRD §7 info-leakage)."""
    try:
        home = Path.home()
        resolved = Path(path).expanduser()
        if str(resolved).startswith(str(home)):
            return "~" + str(resolved)[len(str(home)):]
    except OSError:
        pass
    return str(path)


class TUI:
    def __init__(self, *, bus: EventBus, console: Optional[Console] = None, refresh_hz: float = 4.0):
        self._bus = bus
        self._console = console or Console()
        self._refresh_interval = 1.0 / max(1.0, refresh_hz)
        self._lock = threading.RLock()
        self._state = "IDLE_DISCONNECTED"
        self._device: Optional[tuple[str, str]] = None
        self._progress: Dict[str, tuple[int, int]] = {}
        self._log: deque[tuple[datetime, str, Severity]] = deque(maxlen=RECENT_LOG_LIMIT)
        self._session_files = 0
        self._session_bytes = 0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

        self._bus.subscribe(self._on_event)

    # -- event handler --------------------------------------------------

    def _on_event(self, event: Event) -> None:
        with self._lock:
            if isinstance(event, IdleWaiting):
                self._state = event.state
            elif isinstance(event, DeviceAttached):
                self._device = (event.model, event.serial)
                self._log.append((datetime.now(), f"Attached: {event.model} ({event.serial})", Severity.INFO))
            elif isinstance(event, DeviceDetached):
                self._device = None
                self._log.append((datetime.now(), f"Detached: {event.serial}", Severity.WARNING))
            elif isinstance(event, ScanStarted):
                self._log.append((datetime.now(), "Scanning…", Severity.INFO))
            elif isinstance(event, ScanComplete):
                self._log.append(
                    (datetime.now(), f"Scan complete: {event.new_file_count} new", Severity.INFO)
                )
            elif isinstance(event, FileDiscovered):
                self._log.append(
                    (datetime.now(), f"Discovered {event.device_filename} ({event.size_bytes} B)", Severity.INFO)
                )
            elif isinstance(event, DownloadStarted):
                self._progress[event.device_filename] = (0, 0)
                self._log.append(
                    (datetime.now(), f"→ {event.device_filename} -> {_home_relative(event.target_path)}", Severity.INFO)
                )
            elif isinstance(event, DownloadProgress):
                self._progress[event.device_filename] = (event.bytes_done, event.bytes_total)
            elif isinstance(event, DownloadComplete):
                self._progress.pop(event.device_filename, None)
                try:
                    sz = os.path.getsize(event.archive_path)
                except OSError:
                    sz = 0
                self._session_files += 1
                self._session_bytes += sz
                self._log.append(
                    (datetime.now(), f"✓ {event.device_filename}  sha {event.sha256[:12]}", Severity.INFO)
                )
            elif isinstance(event, TransferAborted):
                self._progress.pop(event.device_filename, None)
                self._log.append(
                    (datetime.now(), f"✗ {event.device_filename}: {event.reason}", Severity.WARNING)
                )
            elif isinstance(event, TranscribeStarted):
                self._log.append(
                    (datetime.now(), f"⎋ transcribe → {event.device_filename}", Severity.INFO)
                )
            elif isinstance(event, TranscribeComplete):
                drive = event.drive_file_id or "(no drive id)"
                self._log.append(
                    (datetime.now(), f"⎋ transcribe ✓ {event.device_filename} drive={drive}", Severity.INFO)
                )
            elif isinstance(event, TranscribeSkipped):
                self._log.append(
                    (datetime.now(), f"⎋ transcribe SKIPPED {event.device_filename}: {event.reason}", Severity.WARNING)
                )
            elif isinstance(event, TranscribeFailed):
                self._log.append(
                    (datetime.now(), f"⎋ transcribe FAILED {event.device_filename}: {event.reason}", Severity.ERROR)
                )
            elif isinstance(event, Error):
                ctx = f" [{event.context}]" if event.context else ""
                self._log.append((datetime.now(), f"ERROR{ctx}: {event.message}", event.severity))

    # -- rendering ------------------------------------------------------

    def _render(self) -> Layout:
        with self._lock:
            state = self._state
            device = self._device
            progress_snapshot = dict(self._progress)
            log_snapshot = list(self._log)
            files = self._session_files
            bytes_ = self._session_bytes

        layout = Layout()
        layout.split_column(
            Layout(self._render_header(state, device), name="header", size=3),
            Layout(self._render_transfers(progress_snapshot), name="transfers", ratio=2),
            Layout(self._render_log(log_snapshot), name="log", ratio=3),
            Layout(self._render_footer(files, bytes_), name="footer", size=3),
        )
        return layout

    @staticmethod
    def _render_header(state: str, device: Optional[tuple[str, str]]) -> Panel:
        if device is None:
            msg = Text("Waiting for device…", style="bold yellow")
        else:
            model, serial = device
            msg = Text(f"Connected: {model} ({serial})", style="bold green")
        msg.append(f"   [{state}]", style="dim")
        return Panel(msg, title="hidock-direct", border_style="cyan")

    @staticmethod
    def _render_transfers(progress: Dict[str, tuple[int, int]]) -> Panel:
        if not progress:
            body: Group | Text = Text("Idle — no transfers in progress.", style="dim")
        else:
            bar = Progress(
                TextColumn("{task.description}"),
                BarColumn(bar_width=None),
                TextColumn("{task.completed}/{task.total} B"),
            )
            for name, (done, total) in progress.items():
                task_total = max(total, done, 1)
                bar.add_task(name, total=task_total, completed=done)
            body = Group(bar)
        return Panel(body, title="Transfers", border_style="blue")

    @staticmethod
    def _render_log(log: list[tuple[datetime, str, Severity]]) -> Panel:
        table = Table.grid(expand=True)
        table.add_column("time", style="dim", no_wrap=True)
        table.add_column("msg")
        for when, msg, sev in log:
            color = {
                Severity.INFO: "white",
                Severity.WARNING: "yellow",
                Severity.ERROR: "red",
            }.get(sev, "white")
            table.add_row(when.strftime("%H:%M:%S"), Text(msg, style=color))
        return Panel(table, title="Recent activity", border_style="magenta")

    @staticmethod
    def _render_footer(files: int, bytes_: int) -> Panel:
        mb = bytes_ / (1024 * 1024)
        msg = Text(f"Session: pulled {files} files, {mb:.1f} MB total.", style="dim")
        return Panel(msg, title="Session", border_style="cyan")

    # -- lifecycle ------------------------------------------------------

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="hidock-tui", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None
        try:
            self._bus.unsubscribe(self._on_event)
        except Exception:
            pass

    def _run(self) -> None:
        with Live(self._render(), console=self._console, refresh_per_second=max(1, int(1.0 / self._refresh_interval)), screen=False) as live:
            while not self._stop.is_set():
                live.update(self._render())
                time.sleep(self._refresh_interval)
