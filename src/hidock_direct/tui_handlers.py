"""Pure-function handlers for the whisper selector and unknown prompt.

Factored out of `tui.py` per PRD §5.5: rich's `Live` render loop is hard to
unit-test end-to-end, so the selection state, key dispatch, and action
translation are implemented here as plain functions that take an `App` (or
any object exposing `offload_whisper` / `route_unknown`) plus a set of
pending filenames, and return booleans / counts. The TUI reads keystrokes
and calls into this module.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Protocol

from .classify import RecordingKind


class _AppLike(Protocol):
    def offload_whisper(self, device_filename: str) -> bool: ...
    def route_unknown(self, device_filename: str, as_kind: RecordingKind) -> bool: ...


@dataclass
class WhisperSelectionState:
    """Mutable selection model for the whisper modal.

    `filenames` is the full list the operator sees; `cursor` is the
    highlight position; `selected` is the set of chosen filenames.
    """

    filenames: List[str]
    cursor: int = 0
    selected: set = field(default_factory=set)

    def move(self, delta: int) -> None:
        if not self.filenames:
            return
        self.cursor = (self.cursor + delta) % len(self.filenames)

    def toggle_current(self) -> None:
        if not self.filenames:
            return
        name = self.filenames[self.cursor]
        if name in self.selected:
            self.selected.discard(name)
        else:
            self.selected.add(name)

    def toggle_all(self) -> None:
        if set(self.filenames) <= self.selected:
            self.selected.clear()
        else:
            self.selected.update(self.filenames)


def handle_whisper_selection(
    app: _AppLike,
    selected: Iterable[str],
) -> int:
    """Offload every selected whisper via `app.offload_whisper`. Returns the
    count of successful offloads. Failures (bus `Error` already published
    by the App) are skipped — the caller can re-run the modal to retry
    remaining items.
    """
    ok = 0
    for name in list(selected):
        if app.offload_whisper(name):
            ok += 1
    return ok


def handle_unknown_prompt(
    app: _AppLike,
    device_filename: str,
    key: str,
) -> str:
    """Dispatch a single unknown-file prompt keystroke.

    Returns one of:
      - "meeting": routed as MEETING via `App.route_unknown`
      - "whisper": routed as WHISPER via `App.route_unknown`
      - "skip":   no-op, file stays on device and resurfaces next session
      - "cancel": exit the unknown-review loop
      - "ignore": unknown key; caller should re-prompt
    """
    k = key.lower()
    if k == "m":
        app.route_unknown(device_filename, RecordingKind.MEETING)
        return "meeting"
    if k == "w":
        app.route_unknown(device_filename, RecordingKind.WHISPER)
        return "whisper"
    if k == "s":
        return "skip"
    if k in ("\x1b", "escape", "esc"):
        return "cancel"
    return "ignore"


def keys_active_in_state(state: str) -> bool:
    """Whisper/unknown key bindings activate in CONNECTED_IDLE or SCANNING.

    Originally (PRD §2.6) restricted to CONNECTED_IDLE to avoid USB
    contention. Relaxed 2026-04-23: Jensen's `_usb_lock` already serializes
    adapter calls, so an offload triggered mid-scan just waits behind the
    current `list_files` -- no correctness problem. And since the new
    count-delta scan architecture keeps the app in CONNECTED_IDLE almost
    continuously (SCANNING fires only on count change), the old gate made
    the feature nearly inaccessible on large devices (~27s list_files).

    DRAINING stays excluded: an active download should not compete with
    a new operator-initiated transfer on the same adapter.
    """
    return state in ("CONNECTED_IDLE", "SCANNING")
