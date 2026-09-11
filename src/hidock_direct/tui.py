"""Rich-based TUI. Presenter only — no business logic.

Subscribes to the event bus, maintains a tiny view model (current device,
in-flight downloads, recent log, session counters), and renders via
`rich.live.Live`. Shut down cleanly when `stop()` is called.

Keyboard input: when stdin is a TTY, a `KeyboardReader` thread puts the
terminal in cbreak mode and dispatches single keypresses to the TUI. Keys
open the whisper selector modal (`w`) or the unknown-file prompt (`u`),
both of which call into `App.offload_whisper` / `App.route_unknown`; `r`
opens the retry confirm and `l` opens the speaker-count prompt that starts
(or, while one is running, plainly stops) the live-transcription session
through the injected controller. When stdin is NOT a TTY (tests, piped
input), the reader no-ops silently.
"""

from __future__ import annotations

import os
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass
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
from .config import SPEAKER_COUNT_MAX, SPEAKER_COUNT_MIN, parse_speaker_count
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
    RetryCandidatesDetected,
    RetryFinished,
    RetryProgress,
    UnknownsDetected,
    WhispersDetected,
)
from .tui_handlers import (
    WhisperSelectionState,
    handle_unknown_prompt,
    handle_whisper_selection,
    keys_active_in_state,
    retry_key_active_in_state,
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


# The log label follows an event's SEVERITY. `Error` is the bus's general
# operator-message type and carries a severity so it can also say INFO and
# WARNING; hardcoding "ERROR" made every routine live-session line announce
# itself as a failure, which spends the word on messages that are not.
_SEVERITY_LABEL = {
    Severity.INFO: "INFO",
    Severity.WARNING: "WARN",
    Severity.ERROR: "ERROR",
}

RECENT_LOG_LIMIT = 10

# AssemblyAI's documented hard cap on `max_speakers` is `config.SPEAKER_COUNT_MIN`
# / `SPEAKER_COUNT_MAX`, and the check that enforces it is
# `config.parse_speaker_count`. Both are IMPORTED, never restated here: the
# loader admits the value that prepopulates this prompt, so a range this module
# defined for itself could admit a default the prompt then refuses — Enter alone
# would never start a session and the operator would have no way to learn why.
# A second copy of a vocabulary kept in agreement by convention is also what
# produced the 2026-08-20 `Invalid API key` defect and the `10fba18` ledger
# defect; the prompt does not get to be the third.

# The widest entry the range admits, DERIVED from the vendor's cap. A third
# digit can never be valid, so it is refused at the keystroke like any other
# unusable key — which keeps the value on screen and the value that would commit
# the same thing at every moment. Two digits are still accepted into the buffer
# and refused at confirmation ("11"), because refusing mid-number would make the
# two-digit ceiling untypable.
_SPEAKER_ENTRY_MAX_CHARS = len(str(SPEAKER_COUNT_MAX))


def _speaker_refusal(attempted: str) -> str:
    """The parser's own words about `attempted`, for the prompt to display.

    Used at the keystroke and at confirmation, so the operator reads one
    vocabulary in both places — and it is the vocabulary that actually decides,
    not a string kept in step with it by hand. `attempted` is what the entry
    WOULD have become had the key been accepted, so the refusal names the thing
    the operator tried rather than the thing still on screen.
    """
    _value, reason = parse_speaker_count(attempted)
    if reason:
        return reason
    # `parse_speaker_count` returns an EMPTY reason when the text parses, and
    # the two-digit-ceiling guard calls this with entries that can parse (any
    # leading zero, e.g. "08"). Returning "" there dropped the keystroke with no
    # message and no log line — a key that does nothing, with nothing anywhere
    # saying why, which is the exact failure the surrounding comment claims this
    # design avoids.
    return (
        f"{attempted!r} is more digits than this field takes — "
        f"{SPEAKER_COUNT_MIN}-{SPEAKER_COUNT_MAX}, so at most two."
    )


# The vendor's own guidance, on screen, because the trade is not guessable from
# the number alone and is counter-intuitive in one direction: too HIGH is also
# wrong. "Give the model a little headroom above the number of speakers you
# expect; setting it too high can cause over-splitting."
SPEAKER_COUNT_GUIDANCE = (
    f"Range {SPEAKER_COUNT_MIN}-{SPEAKER_COUNT_MAX}. Give the model a little "
    "headroom above the number of speakers you expect — too high over-splits "
    "one person across several labels, and past the cap extra speakers are "
    "merged into the closest existing one."
)


@dataclass
class SpeakerPrompt:
    """The transient state of the speaker-count prompt `l` opens.

    Discarded on confirm and on cancel, exactly like `_retry_confirm` — the
    value applies to ONE session, so the next `l` prepopulates from config and
    never from the previous answer (FR-2.5).

    `value` is the text on screen, not a parsed int: what is displayed and what
    would commit have to be the same thing at every moment, and an int cannot
    represent the empty field that a Backspace leaves.

    `pristine` is True while the field still holds the prepopulated default, so
    the first digit REPLACES it — a prepopulated field behaves like one whose
    contents are selected. Appending instead would read `81` when the operator
    pressed `1` on a default of `8`, and refuse them for typing the number they
    wanted.
    """

    value: str
    message: str = ""
    pristine: bool = True

    def __post_init__(self) -> None:
        # The default arrives from config as an int; the field is text.
        self.value = str(self.value)


def format_pending_footer(
    whisper_count: int, unknown_count: int, failed_count: int = 0
) -> str:
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
    if failed_count > 0:
        label = "transcription" if failed_count == 1 else "transcriptions"
        parts.append(f"{failed_count} failed {label} [press r to retry]")
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
        retry_candidates_provider=None,
        retry_runner=None,
        live_controller=None,
        console: Optional[Console] = None,
        refresh_hz: float = 4.0,
        keyboard: Optional[KeyboardReader] = None,
    ):
        self._bus = bus
        self._app = app
        # Callables returning the current pending lists (names + sizes). Wired
        # to `lambda: app._pending_whispers` in production; tests pass fakes.
        self._retry_candidates_provider = retry_candidates_provider or (lambda: [])
        # Injected by __main__; None in tests that never start a batch.
        self._retry_runner = retry_runner
        # The live-transcription session controller (`l`). Injected by __main__;
        # None only in tests that never press `l`. A None here in a real build is
        # the `ad98cbc` shape — a surface built, tested, and reachable from no
        # keystroke — so `_on_key` says so out loud rather than no-opping.
        self._live_controller = live_controller
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
        self._failed_count = 0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # Modal state: exactly one of (None, whisper selector, unknown prompt)
        # is active at a time. Mutated only on the keyboard thread.
        self._whisper_modal: Optional[WhisperSelectionState] = None
        self._unknown_queue: List[str] = []
        self._retry_confirm = None
        # The speaker-count prompt `l` opens (live_speaker_count_prompt_prd.md).
        # None whenever it is closed; a `SpeakerPrompt` while the operator is
        # deciding. Nothing is started, claimed or suspended while it is open.
        self._speaker_prompt: Optional[SpeakerPrompt] = None
        # FR-11: the run region. Persistent — deliberately NOT the log,
        # which holds ten entries and would evict a long run's own summary.
        self._retry_progress = None
        self._retry_summary = None

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
            elif isinstance(event, RetryCandidatesDetected):
                self._failed_count = event.count
            elif isinstance(event, RetryProgress):
                # A new run supersedes the previous run's summary — otherwise the
                # region shows last run's totals while this one is still going.
                self._retry_progress = event
                self._retry_summary = None
            elif isinstance(event, RetryFinished):
                self._retry_progress = None
                self._retry_summary = event
            elif isinstance(event, UnknownsDetected):
                self._unknown_count = event.count
                if event.count > 0:
                    self._log.append(
                        (datetime.now(), f"{event.count} unknown file(s) — press u to route", Severity.WARNING)
                    )
            elif isinstance(event, Error):
                # The label follows the event's SEVERITY, not its class name.
                # `Error` is the bus's general operator-message type and carries
                # a severity precisely so it can also say INFO and WARNING — the
                # colour at `_SEVERITY_STYLE` has always honoured that. The
                # label was hardcoded "ERROR", so every routine live-session
                # line ("Live transcription is running…", "Live window: opened
                # in Google Chrome.") announced itself as a failure. An operator
                # scanning for real problems cannot afford a log where the word
                # ERROR carries no information.
                ctx = f" [{event.context}]" if event.context else ""
                label = _SEVERITY_LABEL.get(event.severity, "ERROR")
                self._log.append((datetime.now(), f"{label}{ctx}: {event.message}", event.severity))

    def _log_key_ignored(self, message: str) -> None:
        with self._lock:
            self._log.append((datetime.now(), message, Severity.WARNING))

    def _open_retry_confirm(self) -> None:
        """Build the retry confirm from the ledger and show it.

        Runs on the keyboard thread and only reads — the batch itself is started
        from the confirm handler.
        """
        try:
            candidates = self._retry_candidates_provider()
        except Exception as exc:  # LedgerUnavailable and anything below it
            self._log_key_ignored(f"retry unavailable: {exc}")
            return
        if not candidates:
            self._log_key_ignored("no failed transcriptions to retry")
            return
        with self._lock:
            self._retry_confirm = candidates

    def _handle_retry_confirm_key(self, ch: str) -> None:
        """`y` / `f` / `esc` on the open confirm (FR-10).

        `f` force-includes what the default set excluded — but only files still
        on disk, because a missing file cannot be retried by anyone.
        """
        with self._lock:
            candidates = list(self._retry_confirm or [])

        if ch == "\x1b":
            with self._lock:
                self._retry_confirm = None
            self._log_key_ignored("retry cancelled")
            return

        if ch not in ("y", "f"):
            self._log_key_ignored(
                f"key {ch!r} ignored: the retry confirm is open (y / f / esc)"
            )
            return

        if ch == "f":
            selected = [c for c in candidates if c.exists_on_disk]
        else:
            selected = [c for c in candidates if c.default_selected]

        with self._lock:
            self._retry_confirm = None

        if not selected:
            self._log_key_ignored("nothing to retry: every candidate is missing from the archive")
            return
        self._start_retry_batch(selected)

    def _handle_speaker_prompt_key(self, ch: str) -> None:
        """Digits / Backspace / Enter / Esc on the open speaker-count prompt.

        Follows `_handle_retry_confirm_key`: a state field, one handler, and the
        same logging convention for a key that did nothing — "I pressed a key and
        nothing happened" otherwise has several indistinguishable causes.

        Nothing here ever CLAMPS. An entry outside the vendor's 1-10 is refused
        with a reason and the prompt stays open, because a clamp starts a metered
        session under a ceiling the operator neither chose nor saw — and past the
        cap the vendor merges additional speakers into the closest existing
        label, destroying the distinction rather than degrading it.

        Refusal happens at the KEYSTROKE for anything that is not a usable digit
        (the terminal is in cbreak mode, so every stray key in the app lands
        here) and at CONFIRMATION for a digit string outside the range. Both use
        the same message, so the displayed value and the value that would commit
        are the same thing at every moment.
        """
        with self._lock:
            prompt = self._speaker_prompt
        if prompt is None:
            return

        if ch == "\x1b":
            with self._lock:
                self._speaker_prompt = None
            self._log_key_ignored("live session cancelled — nothing was started")
            return

        if ch in ("\r", "\n"):
            # cbreak terminals send `\r`; the whisper modal accepts both and a
            # prompt that took only one would look dead on the other terminal.
            value, reason = parse_speaker_count(prompt.value)
            if value is None:
                # An empty field lands here too: backspacing the value away and
                # pressing Enter is refused, not quietly re-defaulted to config
                # — the operator cleared it deliberately, and a session that
                # starts under a number they just deleted is the surprise this
                # prompt exists to remove.
                with self._lock:
                    prompt.message = reason
                return
            with self._lock:
                self._speaker_prompt = None
            # The prompt's own value, never the configured default: the field is
            # editable, so committing the default would make the edit theatre.
            self._live_controller.toggle(max_speakers=value)
            return

        if ch in ("\x7f", "\x08"):
            # Terminals send DEL for Backspace; some send BS. Handling one leaves
            # the operator unable to correct a typo on their own terminal. It is
            # an edit, so it consumes the prepopulated value rather than leaving
            # a field the operator believes they emptied — and it is guarded,
            # because an IndexError on the keyboard thread is swallowed by
            # `KeyboardReader._run` and reaches the operator as a dead prompt.
            with self._lock:
                prompt.value = prompt.value[:-1]
                prompt.pristine = False
                prompt.message = ""
            return

        if ch.isascii() and ch.isdigit():
            with self._lock:
                if prompt.pristine:
                    prompt.value = ch
                elif len(prompt.value) >= _SPEAKER_ENTRY_MAX_CHARS:
                    prompt.message = _speaker_refusal(prompt.value + ch)
                    return
                else:
                    prompt.value += ch
                prompt.pristine = False
                # The refusal was about one keystroke, not about the prompt's
                # state: a latched message leaves the operator reading a
                # complaint about a key they already corrected.
                prompt.message = ""
            return

        with self._lock:
            prompt.message = _speaker_refusal(ch)
        self._log_key_ignored(
            f"key {ch!r} ignored: the speaker-count prompt is open "
            f"(digits / backspace / enter / esc)"
        )

    def _start_retry_batch(self, selected) -> None:
        """Run the batch off the keyboard thread.

        `run_retry_batch` uploads audio and waits on AssemblyAI; running it
        inline would freeze every other key for the length of the run —
        including the `esc` that dismisses the result.
        """
        thread = threading.Thread(
            target=self._run_retry_batch, args=(selected,), name="hidock-retry", daemon=True
        )
        thread.start()

    def _run_retry_batch(self, selected) -> None:
        """The batch itself. Publishes progress and the summary onto the bus."""
        if self._retry_runner is None:
            self._log_key_ignored(
                "retry is not wired to a runner — this build cannot start a batch"
            )
            return
        try:
            self._retry_runner(selected)
        except Exception as exc:
            # Redacted: this message is rendered in the activity log, and the
            # exception can carry AAI text including an account-scoped upload
            # URL (FR-12 — "anything rendered", not just the abort reason).
            from .retry import redact

            self._bus.publish(
                Error(
                    message=f"retry batch failed: {redact(str(exc))}",
                    severity=Severity.ERROR,
                    context="retry",
                )
            )
        # Refresh the badge from the ledger — the contract `RetryCandidatesDetected`
        # documents is "at startup and after every retry batch". Without this the
        # footer keeps advertising the pre-run count, so a batch that fixed
        # everything still reads as N failed. Runs after a failed batch too: the
        # count is whatever the ledger now says, which is the point.
        try:
            self._bus.publish(
                RetryCandidatesDetected(count=len(self._retry_candidates_provider()))
            )
        except Exception as exc:  # LedgerUnavailable and anything below it
            self._log_key_ignored(f"retry count not refreshed: {exc}")

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
            passes per PRD §2.6. `r` (retry) and `l` (live session) are
            dispatched ahead of that gate — see below.
        """
        with self._lock:
            current_state = self._state
            in_whisper = self._whisper_modal is not None
            in_unknown = bool(self._unknown_queue)
            in_retry_confirm = self._retry_confirm is not None
            in_speaker_prompt = self._speaker_prompt is not None

        if in_whisper:
            self._handle_whisper_modal_key(ch)
            return
        if in_unknown:
            self._handle_unknown_prompt_key(ch)
            return
        if in_retry_confirm:
            self._handle_retry_confirm_key(ch)
            return
        if in_speaker_prompt:
            # Routed with the other modals, after them and ahead of every
            # top-level binding: an open prompt owns the keyboard (a `w` here
            # must not open the whisper selector behind it), and a modal that is
            # already open owns `l` (a prompt opened from a keystroke aimed
            # elsewhere is one Enter away from a metered session).
            self._handle_speaker_prompt_key(ch)
            return
        # Top-level key. Log EVERY received keystroke + the reason it was
        # accepted or ignored. Without this, "I pressed `w` and nothing
        # happened" has three indistinguishable causes: the key never reached
        # the TUI, the state gate rejected it, or the pending bucket was empty.
        display = ch if ch.isprintable() else f"\\x{ord(ch):02x}"
        whisper_count = len(self._pending_whispers_provider())
        unknown_count = len(self._pending_unknowns_provider())
        # `r` is dispatched ahead of the whisper/unknown gate on purpose: those
        # bindings need a device on the bus, retry does not, and its primary
        # state (IDLE_DISCONNECTED) is one the gate below rejects. Routing it
        # after would log "keys active only in CONNECTED_IDLE" for the one key
        # that is supposed to work there.
        if ch == "\x1b":
            with self._lock:
                had_region = self._retry_progress is not None or self._retry_summary is not None
                self._retry_progress = None
                self._retry_summary = None
            if had_region:
                self._log_key_ignored("retry result dismissed")
            return

        if ch == "r":
            if not retry_key_active_in_state(current_state):
                self._log_key_ignored(
                    f"key 'r' ignored: state={current_state} "
                    f"(retry is unavailable while a transfer is draining)"
                )
                return
            self._open_retry_confirm()
            return

        # `l` is dispatched ahead of the whisper/unknown gate for the same reason
        # `r` is, plus one of its own: the live session's refusal condition is
        # FR-6.2 ("a transfer is in flight"), and the controller owns that
        # message because only it can name the offload. Routing `l` after the
        # gate would answer "keys active only in CONNECTED_IDLE" for exactly the
        # states — DRAINING, SCANNING — where the operator most needs the real
        # reason. `toggle()` is contracted never to raise: a refusal reaches the
        # operator as an `Error` on the bus, because an exception on the
        # keyboard thread is swallowed by `KeyboardReader._run`.
        if ch == "l":
            if self._live_controller is None:
                self._log_key_ignored(
                    "key 'l' ignored: this build has no live-session controller "
                    "wired — live transcription is unreachable"
                )
                return
            if self._live_controller.is_live:
                # Stopping asks nothing. `max_speakers` is a START parameter —
                # it lives on `StreamingParameters`, not the updateable session
                # parameters — and making the operator answer a question to end
                # a call would be a prompt in front of the one direction that
                # has nothing to decide (FR-5.2 of the surface PRD survives).
                self._live_controller.toggle()
                return
            # FR-ERR-2: the busy refusal is raised BEFORE the prompt opens.
            # Refusing after the operator has chosen a number wastes the
            # decision and reads as though the number caused the failure. The
            # controller owns the words because only it can name the offload.
            refusal = self._live_controller.start_refusal()
            if refusal:
                self._log_key_ignored(refusal)
                return
            # FR-1.1: opening the prompt starts NOTHING — no session, no device
            # claim, no suspended offload polling, no browser window. FR-1.2:
            # prepopulated from config so Enter alone starts.
            with self._lock:
                self._speaker_prompt = SpeakerPrompt(
                    value=self._live_controller.default_max_speakers
                )
            return

        if not keys_active_in_state(current_state):
            with self._lock:
                self._log.append((
                    datetime.now(),
                    f"key '{display}' ignored: state={current_state} (keys active only in CONNECTED_IDLE)",
                    Severity.WARNING,
                ))
            return
        if ch == "w":
            if whisper_count == 0:
                with self._lock:
                    self._log.append((
                        datetime.now(),
                        "key 'w' ignored: no whispers pending on device",
                        Severity.WARNING,
                    ))
                return
            self._open_whisper_modal()
        elif ch == "u":
            if unknown_count == 0:
                with self._lock:
                    self._log.append((
                        datetime.now(),
                        "key 'u' ignored: no unknown files pending",
                        Severity.WARNING,
                    ))
                return
            self._open_unknown_prompt()
        else:
            # Unmapped key at top level. Log it so the operator sees the
            # keystroke reached the dispatcher even if there's no binding.
            with self._lock:
                self._log.append((
                    datetime.now(),
                    f"key '{display}' received but has no binding at top level",
                    Severity.INFO,
                ))

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
            failed = self._failed_count
            retry_confirm = self._retry_confirm
            speaker_prompt = self._speaker_prompt
            retry_progress = self._retry_progress
            retry_summary = self._retry_summary
            modal = self._whisper_modal
            unknown_queue = list(self._unknown_queue)

        # Center panel: modal overlay takes priority over the log.
        if modal is not None:
            center = self._render_whisper_modal(modal)
        elif unknown_queue:
            center = self._render_unknown_prompt(unknown_queue)
        elif retry_confirm is not None:
            center = self._render_retry_confirm(retry_confirm)
        elif speaker_prompt is not None:
            # Replaces the log rather than stacking above it: the panel is the
            # only place the operator can read what they are about to commit,
            # and a renderable taller than its pane is cropped from the bottom
            # with no sign that anything was lost — the defect that shipped in
            # the activity log (`971a2a8`) and this app is routinely run in a
            # small window. A ten-entry log stacked on top would crop the prompt
            # away at ordinary terminal heights.
            center = self._render_speaker_prompt(speaker_prompt)
        elif retry_progress is not None or retry_summary is not None:
            # Stacked, not replaced: the run region must persist without
            # hiding the activity log it deliberately does not live in.
            center = Group(
                self._render_retry_run(retry_progress, retry_summary),
                self._render_log(log_snapshot),
            )
        else:
            center = self._render_log(log_snapshot)

        # The footer is measured, not given a constant height. Its pending line
        # joins every fragment into one string that wraps, so the panel is 4, 5
        # or 6 rows deep depending on terminal width — a constant is correct at
        # one width and crops the badge at every other. The header is genuinely
        # fixed (one line, always), so it keeps its size.
        footer = self._render_footer(files, bytes_, whispers, unknowns, failed)

        layout = Layout()
        layout.split_column(
            Layout(self._render_header(state, device), name="header", size=3),
            Layout(self._render_transfers(progress_snapshot), name="transfers", ratio=2),
            Layout(center, name="center", ratio=3),
            Layout(footer, name="footer", size=self._rendered_height(footer)),
        )
        return layout

    def _rendered_height(self, renderable) -> int:
        """Rows `renderable` needs at the console it will be drawn on.

        Asking rich rather than computing it: the arithmetic that looks right
        ("one row per pending fragment") is wrong, because the fragments are
        joined into a single line whose wrapping depends on width.
        """
        options = self._console.options.update(height=None)
        return len(self._console.render_lines(renderable, options, pad=False))

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
    def _render_retry_confirm(candidates) -> Panel:
        """The confirm the operator reads before spending money (FR-10)."""
        from .retry import format_retry_confirm, summarize

        lines = format_retry_confirm(summarize(candidates))
        body = Group(
            Text(lines[0], style="bold"),
            *[Text(line) for line in lines[1:-1]],
            Text(lines[-1], style="bold cyan"),
        )
        return Panel(body, title="Retry failed transcriptions", border_style="yellow")

    @staticmethod
    def _render_retry_run(progress, summary) -> Panel:
        """Progress and summary, in a region the log cannot evict (FR-11).

        Every rendered fragment of AAI error text goes through `redact` — the
        exposure is *introduced* by surfacing these errors at all, and the
        operator screenshots this app against a public repo (FR-12).
        """
        from .retry import redact

        rows: list[Text] = []
        if progress is not None:
            rows.append(
                Text(
                    f"{progress.index}/{progress.total}  {progress.filename}",
                    style="bold",
                )
            )
            rows.append(Text("transcribing — long files take a few minutes", style="dim"))
        if summary is not None:
            rows.append(
                Text(
                    f"{summary.succeeded} transcribed   "
                    f"{summary.re_rendered} re-rendered   "
                    f"{summary.failed} failed   "
                    f"{summary.not_attempted} not attempted",
                    style="bold",
                )
            )
            if summary.aborted_reason:
                rows.append(Text(f"stopped: {redact(summary.aborted_reason)}", style="yellow"))
            rows.append(Text("esc to dismiss", style="dim"))
        return Panel(Group(*rows), title="Retry", border_style="green")

    @staticmethod
    def _render_speaker_prompt(prompt: SpeakerPrompt) -> Panel:
        """The number the operator is about to commit, the range, and the trade.

        All three are on screen together on purpose: the cap is the vendor's and
        is not inferable, the modal's two exits are not discoverable on a
        cbreak-mode keyboard with no other affordance, and the guidance is
        counter-intuitive in one direction — an operator avoiding the merge
        failure walks straight into the over-splitting one without it.
        """
        # Row order is crop order. This panel is a MODAL that consumes every
        # key, and a `rich` panel taller than its pane is cropped from the
        # BOTTOM — which had put "esc cancel" first in line to disappear,
        # leaving an operator trapped in a prompt with nothing on screen saying
        # how to leave it. The exit affordance now sits above the guidance, so
        # the line that is lost first is the one you can most afford to lose.
        rows: list[Text] = [
            Text.assemble(
                ("People on this call, including you: ", "bold"),
                (prompt.value or "—", "bold cyan"),
            )
        ]
        if prompt.message:
            rows.append(Text(prompt.message, style="bold yellow"))
        rows.append(
            Text(
                "digits edit · backspace delete · enter start · esc cancel",
                style="dim",
            )
        )
        # Last, and therefore first to be cropped: useful, but the operator can
        # act without it. The value, any refusal, and the way out cannot.
        rows.append(Text(SPEAKER_COUNT_GUIDANCE, style="dim"))
        return Panel(
            Group(*rows), title="Live session — speaker count", border_style="yellow"
        )

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
        """Newest first, and the panel says how many entries it holds.

        Both properties exist because this pane can be shorter than its content
        and `rich` crops an overflowing renderable from the BOTTOM. Rendered
        oldest-first — the conventional order for a scrolling feed — the newest
        entry was the first one lost, while the oldest stayed pinned at the top.
        That convention assumes the viewport follows the tail; this viewport is a
        fixed pane that does not scroll. Reversing costs a change in reading
        order and is correct at every height with no arithmetic, which matters:
        predicting the pane's row budget was measured wrong on 27 of 30
        height/region combinations.

        The held-count goes in the TITLE because a title renders on the top
        border and therefore survives cropping at every height — verified down to
        a 16-row terminal, where no log rows render at all and the count is the
        only thing left. It reports what is HELD rather than how many were
        dropped: the number dropped depends on the pane height, which is not
        knowable from here, so a `... N earlier` marker would have stated a false
        count. Held-versus-visible is exact and answers the same question — it is
        what makes a cropped list distinguishable from a genuinely short one.
        """
        table = Table.grid(expand=True)
        table.add_column("time", style="dim", no_wrap=True)
        table.add_column("msg")
        for when, msg, sev in reversed(log):
            color = {
                Severity.INFO: "white",
                Severity.WARNING: "yellow",
                Severity.ERROR: "red",
            }.get(sev, "white")
            table.add_row(when.strftime("%H:%M:%S"), Text(msg, style=color))
        return Panel(
            table,
            title=f"Recent activity ({len(log)} held)",
            border_style="magenta",
        )

    @staticmethod
    def _render_footer(
        files: int, bytes_: int, whisper_count: int, unknown_count: int, failed_count: int
    ) -> Panel:
        # Every input arrives as a parameter, snapshotted under the lock by the
        # caller. `failed_count` is no exception: it is mutated on the event
        # thread (`_on_event`), so reading it from here would be an
        # unsynchronized cross-thread read even if `self` were in scope.
        mb = bytes_ / (1024 * 1024)
        lines: list[Text] = [Text(f"Session: pulled {files} files, {mb:.1f} MB total.", style="dim")]
        pending = format_pending_footer(whisper_count, unknown_count, failed_count)
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
        # Surface the keyboard-reader status to the operator so pressing `w`
        # with no response is diagnosable without re-reading the source.
        # Without this, a non-TTY stdin or a failed termios setup is invisible.
        if getattr(self._keyboard, "_is_tty", False) and getattr(self._keyboard, "_thread", None) is not None:
            msg = "Keyboard input active. Press w for whispers, u for unknowns (only in CONNECTED_IDLE)."
            sev = Severity.INFO
        else:
            msg = (
                "Keyboard input NOT active -- stdin is not a TTY (e.g., launched "
                "from an IDE run panel, piped, or under `nohup`). Relaunch in a "
                "real terminal (Terminal.app, iTerm2) to enable `w`/`u` keys."
            )
            sev = Severity.WARNING
        with self._lock:
            self._log.append((datetime.now(), msg, sev))

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
