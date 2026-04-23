"""PRD §5.5 TUI handler tests. Pure functions, no rich/Live involvement."""

from __future__ import annotations

from typing import List, Tuple

from hidock_direct.classify import RecordingKind
from hidock_direct.tui import format_pending_footer
from hidock_direct.tui_handlers import (
    WhisperSelectionState,
    handle_unknown_prompt,
    handle_whisper_selection,
    keys_active_in_state,
)


class FakeApp:
    def __init__(self, succeed_whispers: set = None, succeed_routes: set = None):
        self.whispered: List[str] = []
        self.routed: List[Tuple[str, RecordingKind]] = []
        self._succeed_whispers = succeed_whispers  # None = all succeed
        self._succeed_routes = succeed_routes

    def offload_whisper(self, name: str) -> bool:
        self.whispered.append(name)
        if self._succeed_whispers is None:
            return True
        return name in self._succeed_whispers

    def route_unknown(self, name: str, as_kind: RecordingKind) -> bool:
        self.routed.append((name, as_kind))
        if self._succeed_routes is None:
            return True
        return name in self._succeed_routes


# ---- footer rendering -----------------------------------------------------


def test_footer_empty_when_nothing_pending() -> None:
    assert format_pending_footer(0, 0) == ""


def test_footer_whispers_only() -> None:
    assert format_pending_footer(3, 0) == "3 whispers on device [press w to pick]"


def test_footer_single_unknown_singularizes() -> None:
    assert format_pending_footer(0, 1) == "1 unknown [press u to review]"


def test_footer_both() -> None:
    text = format_pending_footer(3, 1)
    assert "3 whispers on device [press w to pick]" in text
    assert "1 unknown [press u to review]" in text


# ---- whisper selection state ---------------------------------------------


def test_whisper_state_move_wraps() -> None:
    s = WhisperSelectionState(filenames=["a", "b", "c"])
    s.move(1)
    assert s.cursor == 1
    s.move(-2)
    assert s.cursor == 2  # wrap


def test_whisper_state_toggle_current() -> None:
    s = WhisperSelectionState(filenames=["a", "b"])
    s.toggle_current()  # select a
    assert s.selected == {"a"}
    s.toggle_current()  # deselect a
    assert s.selected == set()


def test_whisper_state_toggle_all() -> None:
    s = WhisperSelectionState(filenames=["a", "b"])
    s.toggle_all()
    assert s.selected == {"a", "b"}
    s.toggle_all()  # all→none
    assert s.selected == set()


# ---- whisper selection dispatch ------------------------------------------


def test_handle_whisper_selection_empty() -> None:
    app = FakeApp()
    assert handle_whisper_selection(app, []) == 0
    assert app.whispered == []


def test_handle_whisper_selection_offloads_each() -> None:
    app = FakeApp()
    n = handle_whisper_selection(app, ["a", "b", "c"])
    assert n == 3
    assert app.whispered == ["a", "b", "c"]


def test_handle_whisper_selection_skips_failures() -> None:
    app = FakeApp(succeed_whispers={"a", "c"})
    n = handle_whisper_selection(app, ["a", "b", "c"])
    assert n == 2
    # All three attempted; only two counted successful.
    assert app.whispered == ["a", "b", "c"]


# ---- unknown prompt dispatch ---------------------------------------------


def test_unknown_prompt_meeting() -> None:
    app = FakeApp()
    assert handle_unknown_prompt(app, "weird.hda", "m") == "meeting"
    assert app.routed == [("weird.hda", RecordingKind.MEETING)]


def test_unknown_prompt_whisper() -> None:
    app = FakeApp()
    assert handle_unknown_prompt(app, "weird.hda", "w") == "whisper"
    assert app.routed == [("weird.hda", RecordingKind.WHISPER)]


def test_unknown_prompt_skip_no_route() -> None:
    app = FakeApp()
    assert handle_unknown_prompt(app, "weird.hda", "s") == "skip"
    assert app.routed == []


def test_unknown_prompt_cancel() -> None:
    app = FakeApp()
    assert handle_unknown_prompt(app, "weird.hda", "\x1b") == "cancel"
    assert app.routed == []


def test_unknown_prompt_unknown_key_ignored() -> None:
    app = FakeApp()
    assert handle_unknown_prompt(app, "weird.hda", "x") == "ignore"
    assert app.routed == []


# ---- state gating --------------------------------------------------------


def test_keys_inactive_during_scanning() -> None:
    assert keys_active_in_state("SCANNING") is False
    assert keys_active_in_state("DRAINING") is False
    assert keys_active_in_state("IDLE_DISCONNECTED") is False


def test_keys_active_in_connected_idle() -> None:
    assert keys_active_in_state("CONNECTED_IDLE") is True
