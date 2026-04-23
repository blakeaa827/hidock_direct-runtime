"""Rich-based TUI. Presenter only — no business logic.

Subscribes to the event bus, maintains a tiny view model (current device,
in-flight downloads, recent log, session counters), and renders via
`rich.live.Live`. Shut down cleanly when `stop()` is called.

Keyboard input: when stdin is a TTY, a `KeyboardReader` thread puts the
terminal in cbreak mode and dispatches single keypresses to the TUI. Keys
open the whisper selector modal (`w`) or the unknown-file prompt (`u`),
both of which call into `App.offload_whisper` / `App.route_unknown`. When
stdin is NOT a TTY (tests, piped input), the reader no-ops silently.
"""

from __future__ import annotations

import os
import sys
import threading
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Protocol

from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, TextColumn
from rich.table import Table
from rich.text import Text

from .classify import RecordingKind
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
    UnknownsDetected,
    WhispersDetected,
)
from .tui_handlers import (
    WhisperSelectionState,
    handle_unknown_prompt,
    handle_whisper_selection,
    keys_active_in_state,
)


class _AppHandle(Protocol):
    """Subset of `App` that the TUI calls. Declared here to avoid a circular
    import; tests supply a fake that implements these two methods.
    """

    def offload_whisper(self, device_filename: str) -> bool: ...
    def route_unknown(self, device_filename: str, as_kind: RecordingKind) -> bool: ...


class KeyboardReader:
    """Single-keystroke stdin reader. Runs in its own thread; dispatches each
    raw character to `on_key`. No-ops when stdin is not a TTY (tests, piping).

    Uses `termios` + `tty` to put the terminal in cbreak mode so keystrokes
    arrive without the operator hitting Enter. The terminal settings are
    restored on `stop()` even if the thread exits via exception.
    """

    def __init__(self, on_key):
        self._on_key = on_key
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._old_settings = None
        self._is_tty = False
        try:
            self._is_tty = sys.stdin is not None and sys.stdin.isatty()
        except (AttributeError, ValueError):
            self._is_tty = False

    def start(self) -> None:
        if not self._is_tty or self._thread is not None:
            return
        try:
            import termios
            import tty
            self._old_settings = termios.tcgetattr(sys.stdin.fileno())
            tty.setcbreak(sys.stdin.fileno())
        except (ImportError, OSError):
            self._is_tty = False
            self._old_settings = None
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="hidock-keys", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)
            self._thread = None
        if self._old_settings is not None:
            try:
                import termios
                termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, self._old_settings)
            except (ImportError, OSError):
                pass
            self._old_settings = None

    def _run(self) -> None:
        import select
        while not self._stop.is_set():
            # Poll with a short timeout so `stop()` returns promptly.
            try:
                ready, _, _ = select.select([sys.stdin], [], [], 0.1)
            except (OSError, ValueError):
                return
            if not ready:
                continue
            try:
                ch = sys.stdin.read(1)
            except (OSError, ValueError):
                return
            if not ch:
                continue
            try:
                self._on_key(ch)
            except Exception:
                # A crashing key handler must never kill the reader — the
                # TUI log has already surfaced the error via `Error` events
                # from the App layer.
                continue


RECENT_LOG_LIMIT = 10


def format_pending_footer(whisper_count: int, unknown_count: int) -> str:
    """Render the whisper/unknown count line for the TUI footer (PRD §2.6).

    Pure function -- empty when nothing is pending, otherwise joins the
    fragments that apply. Separated from the rich layout so tests assert
    the exact operator-visible string.
    """
    parts: list[str] = []
    if whisper_count > 0:
        label = "whisper" if whisper_count == 1 else "whispers"
        parts.append(f"{whisper_count} {label} on device [press w to pick]")
    if unknown_count > 0:
        label = "unknown" if unknown_count == 1 else "unknowns"
        parts.append(f"{unknown_count} {label} [press u to review]")
    return "   ".join(parts)


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
    def __init__(
        self,
        *,
        bus: EventBus,
        app: Optional[_AppHandle] = None,
        pending_whispers_provider=None,
        pending_unknowns_provider=None,
        console: Optional[Console] = None,
        refresh_hz: float = 4.0,
        keyboard: Optional[KeyboardReader] = None,
    ):
        self._bus = bus
        self._app = app
        # Callables returning the current pending lists (names + sizes). Wired
        # to `lambda: app._pending_whispers` in production; tests pass fakes.
        self._pending_whispers_provider = pending_whispers_provider or (lambda: [])
        self._pending_unknowns_provider = pending_unknowns_provider or (lambda: [])
        self._console = console or Console()
        self._refresh_interval = 1.0 / max(1.0, refresh_hz)
        self._lock = threading.RLock()
        self._state = "IDLE_DISCONNECTED"
        self._device: Optional[tuple[str, str]] = None
        self._progress: Dict[str, tuple[int, int]] = {}
        self._log: deque[tuple[datetime, str, Severity]] = deque(maxlen=RECENT_LOG_LIMIT)
        self._session_files = 0
        self._session_bytes = 0
        self._whisper_count = 0
        self._unknown_count = 0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # Modal state: exactly one of (None, whisper selector, unknown prompt)
        # is active at a time. Mutated only on the keyboard thread.
        self._whisper_modal: Optional[WhisperSelectionState] = None
        self._unknown_queue: List[str] = []

        self._keyboard = keyboard if keyboard is not None else KeyboardReader(self._on_key)

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
            elif isinstance(event, WhispersDetected):
                self._whisper_count = event.count
                if event.count > 0:
                    self._log.append(
                        (datetime.now(), f"{event.count} whisper(s) on device — press w to review", Severity.INFO)
                    )
            elif isinstance(event, UnknownsDetected):
                self._unknown_count = event.count
                if event.count > 0:
                    self._log.append(
                        (datetime.now(), f"{event.count} unknown file(s) — press u to route", Severity.WARNING)
                    )
            elif isinstance(event, Error):
                ctx = f" [{event.context}]" if event.context else ""
                self._log.append((datetime.now(), f"ERROR{ctx}: {event.message}", event.severity))

    # -- keyboard input -------------------------------------------------

    def _on_key(self, ch: str) -> None:
        """Dispatch a single keystroke. Runs on the keyboard-reader thread.

        Routing:
          - If a whisper modal is open → modal keys (`j`/`k` nav, space toggle,
            `a` toggle-all, enter commit, `q` / esc cancel).
          - Else if an unknown prompt is open → `m`/`w`/`s` / esc per
            `handle_unknown_prompt`, then advance to the next unknown or close.
          - Else at top level → `w` opens whisper selector, `u` opens unknown
            prompt. Both ignored unless `keys_active_in_state(self._state)`
            passes per PRD §2.6.
        """
        with self._lock:
            current_state = self._state
            in_whisper = self._whisper_modal is not None
            in_unknown = bool(self._unknown_queue)

        if in_whisper:
            self._handle_whisper_modal_key(ch)
            return
        if in_unknown:
            self._handle_unknown_prompt_key(ch)
            return
        if not keys_active_in_state(current_state):
            return
        if ch == "w":
            self._open_whisper_modal()
        elif ch == "u":
            self._open_unknown_prompt()

    def _open_whisper_modal(self) -> None:
        names = [f.name for f in self._pending_whispers_provider()]
        if not names:
            return
        with self._lock:
            self._whisper_modal = WhisperSelectionState(filenames=names)
            self._log.append((datetime.now(), f"Whisper selector: {len(names)} file(s). j/k to move, space to toggle, a=all, enter=offload, q=cancel.", Severity.INFO))

    def _close_whisper_modal(self) -> None:
        with self._lock:
            self._whisper_modal = None

    def _open_unknown_prompt(self) -> None:
        names = [f.name for f in self._pending_unknowns_provider()]
        if not names:
            return
        with self._lock:
            self._unknown_queue = list(names)
            self._log.append((datetime.now(), f"Unknown review: {len(names)} file(s). m=meeting, w=whisper, s=skip, q=cancel.", Severity.INFO))

    def _handle_whisper_modal_key(self, ch: str) -> None:
        with self._lock:
            state = self._whisper_modal
        if state is None:
            return
        if ch in ("q", "\x1b"):
            self._close_whisper_modal()
            return
        if ch == "j":
            state.move(1); return
        if ch == "k":
            state.move(-1); return
        if ch == " ":
            state.toggle_current(); return
        if ch == "a":
            state.toggle_all(); return
        if ch in ("\r", "\n"):
            selected = list(state.selected)
            self._close_whisper_modal()
            if self._app is None or not selected:
                return
            n = handle_whisper_selection(self._app, selected)
            with self._lock:
                self._log.append(
                    (datetime.now(), f"Whisper offload complete: {n}/{len(selected)} succeeded.", Severity.INFO)
                )
            return
        # Unknown key in modal — ignore.

    def _handle_unknown_prompt_key(self, ch: str) -> None:
        with self._lock:
            if not self._unknown_queue:
                return
            current = self._unknown_queue[0]
        if ch == "q":
            with self._lock:
                self._unknown_queue.clear()
            return
        if self._app is None:
            return
        result = handle_unknown_prompt(self._app, current, ch)
        if result == "ignore":
            return
        # meeting / whisper / skip / cancel all advance past this file.
        with self._lock:
            if self._unknown_queue and self._unknown_queue[0] == current:
                self._unknown_queue.pop(0)
            if result == "cancel":
                self._unknown_queue.clear()

    # -- rendering ------------------------------------------------------

    def _render(self) -> Layout:
        with self._lock:
            state = self._state
            device = self._device
            progress_snapshot = dict(self._progress)
            log_snapshot = list(self._log)
            files = self._session_files
            bytes_ = self._session_bytes
            whispers = self._whisper_count
            unknowns = self._unknown_count
            modal = self._whisper_modal
            unknown_queue = list(self._unknown_queue)

        # Center panel: modal overlay takes priority over the log.
        if modal is not None:
            center = self._render_whisper_modal(modal)
        elif unknown_queue:
            center = self._render_unknown_prompt(unknown_queue)
        else:
            center = self._render_log(log_snapshot)

        layout = Layout()
        layout.split_column(
            Layout(self._render_header(state, device), name="header", size=3),
            Layout(self._render_transfers(progress_snapshot), name="transfers", ratio=2),
            Layout(center, name="center", ratio=3),
            Layout(self._render_footer(files, bytes_, whispers, unknowns), name="footer", size=3),
        )
        return layout

    @staticmethod
    def _render_whisper_modal(state: WhisperSelectionState) -> Panel:
        table = Table.grid(expand=True)
        table.add_column("", no_wrap=True, style="bold")
        table.add_column("filename")
        for i, name in enumerate(state.filenames):
            marker = "[x]" if name in state.selected else "[ ]"
            row_style = "reverse" if i == state.cursor else ""
            table.add_row(Text(marker, style=row_style), Text(name, style=row_style))
        hint = Text(
            "j/k move · space toggle · a toggle-all · enter offload · q cancel",
            style="dim",
        )
        return Panel(Group(table, hint), title="Whisper selector", border_style="yellow")

    @staticmethod
    def _render_unknown_prompt(queue: List[str]) -> Panel:
        current = queue[0]
        remaining = len(queue)
        body = Text.assemble(
            ("Unknown recording: ", "bold"),
            (current, "bold yellow"),
            "\n",
            (f"({remaining} remaining in queue)\n\n", "dim"),
            ("[m] ", "bold green"), "offload as meeting   ",
            ("[w] ", "bold cyan"), "offload as whisper   ",
            ("[s] ", "dim"), "skip   ",
            ("[q] ", "dim"), "cancel queue",
        )
        return Panel(body, title="Unknown file", border_style="yellow")

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
    def _render_footer(files: int, bytes_: int, whisper_count: int, unknown_count: int) -> Panel:
        mb = bytes_ / (1024 * 1024)
        lines: list[Text] = [Text(f"Session: pulled {files} files, {mb:.1f} MB total.", style="dim")]
        pending = format_pending_footer(whisper_count, unknown_count)
        if pending:
            lines.append(Text(pending, style="bold yellow"))
        return Panel(Group(*lines), title="Session", border_style="cyan")

    # -- lifecycle ------------------------------------------------------

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="hidock-tui", daemon=True)
        self._thread.start()
        # Keyboard reader starts AFTER the render thread so its cbreak-mode
        # setup doesn't race with a concurrent render that might still be
        # reading stdin state. Reader no-ops when stdin is not a TTY (tests).
        self._keyboard.start()

    def stop(self) -> None:
        self._keyboard.stop()
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
