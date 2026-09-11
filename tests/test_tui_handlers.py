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


def test_keys_inactive_during_draining_and_disconnected() -> None:
    # DRAINING stays excluded: an active download shouldn't compete with a
    # new operator-initiated transfer on the same adapter.
    assert keys_active_in_state("DRAINING") is False
    assert keys_active_in_state("IDLE_DISCONNECTED") is False


def test_keys_active_in_connected_idle_and_scanning() -> None:
    # SCANNING was relaxed on 2026-04-23 — see keys_active_in_state docstring.
    assert keys_active_in_state("CONNECTED_IDLE") is True
    assert keys_active_in_state("SCANNING") is True


def test_retry_key_active_without_a_device() -> None:
    """`r` must work in IDLE_DISCONNECTED — the state the app sits in when the
    operator comes back after topping up their AssemblyAI balance. This is why
    it gets its own predicate instead of widening keys_active_in_state."""
    from hidock_direct.tui_handlers import retry_key_active_in_state

    assert retry_key_active_in_state("IDLE_DISCONNECTED") is True
    assert retry_key_active_in_state("CONNECTED_IDLE") is True
    assert retry_key_active_in_state("SCANNING") is True
    assert retry_key_active_in_state("DRAINING") is False


def test_whisper_unknown_gate_is_unchanged_by_the_retry_binding() -> None:
    """The device-dependent bindings must NOT have been widened as a side
    effect of making `r` device-independent."""
    assert keys_active_in_state("IDLE_DISCONNECTED") is False
    assert keys_active_in_state("DRAINING") is False


def test_the_log_label_follows_severity_not_the_event_class():
    """Observed live 2026-08-27: every routine live line said ERROR.

    `Error` is the bus's general operator-message type and carries a severity
    so it can also say INFO and WARNING — the COLOUR always honoured that, only
    the label was hardcoded. So "Live transcription is running — <url>" and
    "Live window: opened in Google Chrome." both announced themselves as
    failures, which spends the word ERROR on messages that are not errors.

    MUTATION: put the literal "ERROR" back in the f-string at tui.py:353 and
    this test fails on the INFO and WARNING cases.
    """
    from hidock_direct.events import Error, EventBus, Severity
    from hidock_direct.tui import TUI

    bus = EventBus()
    tui = TUI(bus=bus, keyboard=_NoKeyboard())

    bus.publish(Error(message="running", severity=Severity.INFO, context="live"))
    bus.publish(Error(message="old sdk", severity=Severity.WARNING, context="live"))
    bus.publish(Error(message="it broke", severity=Severity.ERROR, context="live"))

    rendered = [msg for _when, msg, _sev in tui._log]
    assert rendered[0].startswith("INFO [live]: "), rendered[0]
    assert rendered[1].startswith("WARN [live]: "), rendered[1]
    assert rendered[2].startswith("ERROR [live]: "), rendered[2]


class _NoKeyboard:
    """Keyboard seam stub — the TUI must not touch a real TTY under test."""
    def start(self) -> None: ...
    def stop(self) -> None: ...


# ==========================================================================
# Per-session speaker count — the prompt `l` opens
# (live_speaker_count_prompt_prd.md §5 U-1..U-4, U-7..U-11, U-13)
#
# WHY THIS PROMPT EXISTS, because it shapes every assertion below.
#
# One parameter, two OPPOSITE failures, both observed on real calls days apart:
# a 1:1 call under a ceiling of 6 split one person across Speaker A and B and
# flipped between them; a 10-person call under the same 6 collapsed two or three
# people into one label. AssemblyAI documents both — the ceiling is "a strict
# limit, not a hint", and past it "additional speakers are merged into the
# closest existing label", while "setting it too high can cause over-splitting".
#
# So the value is per-call and only the operator holds it, and `max_speakers`
# lives on `StreamingParameters` rather than the updateable
# `StreamingSessionParameters` — it can ONLY be set as the session opens, which
# is the moment `l` is pressed.
#
# Two properties are non-negotiable and each has its own tests:
#
#   * `l` starts NOTHING until the prompt is confirmed. No session, no device
#     claim, no offload-polling suspension, no browser window. (U-1)
#   * Nothing is ever silently clamped. An out-of-range entry is refused with a
#     reason and the prompt stays open, because a clamp starts a METERED session
#     under a ceiling the operator neither chose nor saw. (U-7 / U-8)
#
# These tests drive `TUI._on_key` — the operator's real entry point — rather
# than the handlers behind it, per /implement-prd Gate 4 step 5. Calling the
# handler directly is what let the retry surface look tested while `r` did
# nothing (`ad98cbc`).
# ==========================================================================

import inspect
import io
import re
from types import SimpleNamespace

import pytest
from rich.console import Console

from hidock_direct.events import EventBus, Severity
from hidock_direct.live_server import LiveSessionController
from hidock_direct.tui import TUI

ENTER = "\r"
ESC = "\x1b"
BACKSPACE = "\x7f"


def _dashes(text: str) -> str:
    """Normalise en/em dashes: the tests pin that a message NAMES the range
    `1-10`, not which dash character the implementer typed."""
    return text.replace("–", "-").replace("—", "-")


def _text(renderable, width=100, height=None) -> str:
    """Render through rich exactly as the live app does, then flatten.

    Box-drawing characters and runs of whitespace are collapsed so an assertion
    reads the operator-visible WORDS and does not depend on where a panel border
    or a line wrap happened to fall.
    """
    buf = io.StringIO()
    Console(file=buf, width=width, height=height).print(renderable)
    return _dashes(re.sub(r"\s+", " ", re.sub(r"[│╭╮╰╯─]", " ", buf.getvalue())))


def _bind_against(real, *args, **kwargs) -> None:
    """Raise TypeError unless this call binds against the real method signature.

    `real` is an unbound method, so `self` is supplied positionally as None.
    The doubles here are deliberately NOT `**kwargs`-permissive: a double more
    forgiving than production cannot fail on a contract disagreement, and seven
    such doubles hid the 2026-08-20 `transcribe_file` defect.
    """
    inspect.signature(real).bind(None, *args, **kwargs)


class _RecordingLiveController:
    """Faithful stand-in for `LiveSessionController` at the TUI seam.

    Records what it was asked to do and with what ceiling, so a test can assert
    both that a session started AND that it started with the operator's number —
    the two are different claims and only the second one falsifies a clamp.
    """

    def __init__(self, *, default_max_speakers: int = 8, refusal: str = None):
        self.default = default_max_speakers
        self.refusal = refusal
        self.live = False
        self.toggles: list = []          # every max_speakers `l` committed
        self.refusal_checks = 0

    @property
    def default_max_speakers(self) -> int:
        """What the prompt prepopulates with — the CONFIGURED value, and never
        the previous session's answer (U-9)."""
        _bind_against(LiveSessionController.default_max_speakers.fget)
        return self.default

    @property
    def is_live(self) -> bool:
        return self.live

    def start_refusal(self):
        _bind_against(LiveSessionController.start_refusal)
        self.refusal_checks += 1
        return self.refusal

    def toggle(self, max_speakers=None) -> None:
        _bind_against(LiveSessionController.toggle, max_speakers=max_speakers)
        self.toggles.append(max_speakers)
        self.live = not self.live

    def stop(self, reason: str = "stopped") -> None:
        _bind_against(LiveSessionController.stop, reason=reason)
        self.live = False


def _prompt_tui(controller=None, state: str = "CONNECTED_IDLE") -> TUI:
    tui = TUI(bus=EventBus(), live_controller=controller, keyboard=_NoKeyboard())
    tui._state = state
    return tui


def _log(tui) -> str:
    return _dashes(" ".join(message for _when, message, _sev in tui._log))


def _type(tui, text: str) -> None:
    for ch in text:
        tui._on_key(ch)


# -- U-1: `l` opens the prompt and starts nothing ---------------------------


def test_pressing_l_opens_the_prompt_and_starts_no_session():
    """U-1 / FR-1.1, and the whole point of the PRD. Starting a live session
    claims the USB endpoint, suspends offload polling and opens a browser window
    — all of it metered, third-party egress. None of that may happen while the
    operator is still deciding on a number.

    MUTATION: leave the `l` branch calling `self._live_controller.toggle()` and
    open the prompt afterwards. The session then starts under the CONFIG ceiling
    and the prompt is decoration.
    """
    controller = _RecordingLiveController()
    tui = _prompt_tui(controller)

    tui._on_key("l")

    assert tui._speaker_prompt is not None, "`l` did not open the speaker prompt"
    assert controller.toggles == [], "a session started before the prompt was confirmed"
    assert controller.is_live is False


def test_the_prompt_is_dispatched_ahead_of_the_connected_idle_gate():
    """`l`'s refusal condition is an in-flight offload, which the controller owns
    and names. Routing `l` after `keys_active_in_state` would answer "keys active
    only in CONNECTED_IDLE" for exactly the states — DRAINING, SCANNING — where
    the operator most needs the real reason. This is the dispatch-ordering defect
    already fixed once for `r`.

    MUTATION: move the `l` branch below the `keys_active_in_state` early return.
    """
    tui = _prompt_tui(_RecordingLiveController(), state="DRAINING")

    tui._on_key("l")

    assert tui._speaker_prompt is not None
    assert "keys active only in CONNECTED_IDLE" not in _log(tui)


def test_l_while_a_session_is_running_stops_it_without_prompting():
    """FR-5.2 of the surface PRD survives: one key, both directions. A prompt on
    the STOP half would make the operator answer a question to end a call.

    MUTATION: open the prompt unconditionally, without consulting `is_live`.
    """
    controller = _RecordingLiveController()
    controller.live = True
    tui = _prompt_tui(controller)

    tui._on_key("l")

    assert tui._speaker_prompt is None, "stopping a session must not ask for a count"
    assert controller.toggles == [None], "the session was not stopped"
    assert controller.is_live is False


# -- U-2: prepopulated, so Enter alone starts -------------------------------


def test_the_prompt_is_prepopulated_with_the_configured_default():
    """U-2 / FR-1.2. The operator's requirement, verbatim: "prepopulated with a
    default of 8 so the user can just hit enter if they choose not to override
    it."

    MUTATION: `SpeakerPrompt(value="")` — an empty field. Enter alone is then
    refused and every session costs the operator a number.
    """
    tui = _prompt_tui(_RecordingLiveController(default_max_speakers=8))

    tui._on_key("l")

    assert tui._speaker_prompt.value == "8"


def test_enter_alone_starts_the_session_with_the_prepopulated_value():
    """U-2's second half — that the prepopulated value is what actually commits,
    not merely what is displayed.

    MUTATION: `toggle()` with no argument on the Enter branch. The controller
    then falls back to its own configured ceiling, which is identical to the
    prepopulated value in THIS case and differs the moment the operator types.
    """
    controller = _RecordingLiveController(default_max_speakers=8)
    tui = _prompt_tui(controller)

    tui._on_key("l")
    tui._on_key(ENTER)

    assert controller.toggles == [8]
    assert tui._speaker_prompt is None, "the prompt must close once it is committed"


@pytest.mark.parametrize("key", ["\r", "\n"])
def test_both_newline_forms_confirm(key):
    """cbreak-mode terminals send `\\r`; the existing whisper modal accepts both
    and a prompt that took only one would appear dead on a terminal that sends
    the other.

    MUTATION: `if ch == "\\n"` alone on the confirm branch.
    """
    controller = _RecordingLiveController(default_max_speakers=8)
    tui = _prompt_tui(controller)

    tui._on_key("l")
    tui._on_key(key)

    assert controller.toggles == [8]


def test_a_typed_value_overrides_the_prepopulated_one():
    """The 1:1 call: the operator types 2 and the flipping stops.

    MUTATION: commit `self._live_controller.default_max_speakers` on Enter
    instead of the prompt's own value — the field becomes editable theatre.
    """
    controller = _RecordingLiveController(default_max_speakers=8)
    tui = _prompt_tui(controller)

    tui._on_key("l")
    _type(tui, "2")
    tui._on_key(ENTER)

    assert controller.toggles == [2]


# -- U-3: digits edit, backspace deletes ------------------------------------


def test_the_first_digit_replaces_the_prepopulated_value():
    """U-3 / FR-1.3. The field arrives prepopulated and therefore behaves like a
    field whose contents are SELECTED: the first digit replaces.

    Appending instead would make the ten-person call — the case that motivated
    this PRD — unreachable without backspacing first: pressing `1` on a
    prepopulated `8` would read `81`, and the operator would be refused for
    typing the number they wanted.

    MUTATION: `prompt.value += ch` unconditionally.
    """
    tui = _prompt_tui(_RecordingLiveController(default_max_speakers=8))

    tui._on_key("l")
    _type(tui, "1")

    assert tui._speaker_prompt.value == "1"


def test_digits_after_the_first_append_so_ten_is_typeable():
    """The 10-person call, which is the whole upper end of the vendor's range.

    MUTATION: `prompt.value = ch` on every digit — the value can then never
    exceed one character and 10 is unreachable from the keyboard.
    """
    controller = _RecordingLiveController(default_max_speakers=8)
    tui = _prompt_tui(controller)

    tui._on_key("l")
    _type(tui, "10")

    assert tui._speaker_prompt.value == "10"

    tui._on_key(ENTER)
    assert controller.toggles == [10]


@pytest.mark.parametrize("backspace", ["\x7f", "\x08"])
def test_backspace_deletes_the_last_character(backspace):
    """FR-1.3. Terminals send DEL (`\\x7f`) for Backspace; some send `\\x08`.
    Handling only one leaves the operator unable to correct a typo on their own
    terminal, with the prompt refusing an entry they cannot edit.

    MUTATION: handle only `"\\x7f"`, or make backspace clear the whole buffer.
    """
    tui = _prompt_tui(_RecordingLiveController(default_max_speakers=8))

    tui._on_key("l")
    _type(tui, "10")
    tui._on_key(backspace)

    assert tui._speaker_prompt.value == "1"


def test_backspace_on_the_prepopulated_value_clears_it():
    """Backspace is an edit, so it must consume the prepopulated value rather
    than leaving a field the operator believes they emptied.

    MUTATION: guard backspace with `if not prompt.pristine` — the displayed `8`
    then survives a Backspace and Enter starts a session the operator thought
    they had cleared.
    """
    tui = _prompt_tui(_RecordingLiveController(default_max_speakers=8))

    tui._on_key("l")
    tui._on_key(BACKSPACE)

    assert tui._speaker_prompt.value == ""


def test_backspace_on_an_empty_value_does_not_crash_or_close():
    """The keyboard reader swallows exceptions, so an IndexError here is a dead
    prompt with no message at all.

    MUTATION: `prompt.value = prompt.value[:-1]` replaced by an unguarded
    `del prompt.value[-1]`-style operation over an empty buffer.
    """
    tui = _prompt_tui(_RecordingLiveController(default_max_speakers=8))

    tui._on_key("l")
    tui._on_key(BACKSPACE)
    tui._on_key(BACKSPACE)

    assert tui._speaker_prompt is not None
    assert tui._speaker_prompt.value == ""


# -- U-4: esc cancels -------------------------------------------------------


def test_esc_closes_the_prompt_and_starts_nothing():
    """U-4 / FR-1.3. Esc is the operator deciding not to make a metered call.

    MUTATION: treat esc as a confirm (fall through to the Enter branch), which
    starts a session from the keystroke that means "no".
    """
    controller = _RecordingLiveController()
    tui = _prompt_tui(controller)

    tui._on_key("l")
    tui._on_key(ESC)

    assert tui._speaker_prompt is None
    assert controller.toggles == [], "esc started a session"
    assert controller.is_live is False


def test_esc_says_that_nothing_was_started():
    """A key that appears to do nothing has three indistinguishable causes — the
    same argument that put a message on every other ignored keystroke.

    MUTATION: close the prompt on esc without logging.
    """
    tui = _prompt_tui(_RecordingLiveController())

    tui._on_key("l")
    tui._on_key(ESC)

    assert "cancel" in _log(tui).lower()


# -- U-7 / U-8: refused, never clamped --------------------------------------


@pytest.mark.parametrize("typed", ["0", "11"])
def test_an_out_of_range_entry_is_refused_and_the_prompt_stays_open(typed):
    """U-7 / FR-2.2. `0` is not a call and `11` is past the vendor's hard cap.

    MUTATION: on the Enter branch, `value = min(10, max(1, parsed))` — the clamp.
    Both entries then start a session, `11` under a ceiling of 10 that the
    operator never saw, and every other assertion in this file still passes.
    """
    controller = _RecordingLiveController()
    tui = _prompt_tui(controller)

    tui._on_key("l")
    _type(tui, typed)
    tui._on_key(ENTER)

    assert tui._speaker_prompt is not None, "the prompt closed on a refused entry"
    assert tui._speaker_prompt.value == typed, "the refused entry is no longer editable"
    assert controller.toggles == [], f"{typed} started a session"


@pytest.mark.parametrize("typed", ["0", "11"])
def test_a_refusal_names_the_range_the_operator_must_type(typed):
    """U-7's second clause. 1-10 is the VENDOR's range and is not guessable from
    the number the operator typed, so the refusal has to carry it.

    MUTATION: `prompt.message = "invalid"` — refused, and unactionable.
    """
    tui = _prompt_tui(_RecordingLiveController())

    tui._on_key("l")
    _type(tui, typed)
    tui._on_key(ENTER)

    assert "1-10" in _dashes(tui._speaker_prompt.message), (
        f"the refusal does not name the range: {tui._speaker_prompt.message!r}"
    )


def test_nothing_is_ever_started_with_a_clamped_ceiling():
    """U-8, stated as an absence — the assertion the parametrized test above
    cannot make, because "no session" and "no CLAMPED session" are different
    claims and only this one names the value.

    A clamp is worse than a refusal in the direction that matters: past the
    ceiling the vendor MERGES additional speakers into the closest existing
    label, so a silently-clamped 11 destroys the distinction between two people
    rather than degrading it, and the operator has no way to know it happened.

    MUTATION: clamp on the Enter branch (`min(10, ...)`) — `11` then commits 10.
    """
    controller = _RecordingLiveController()
    tui = _prompt_tui(controller)

    tui._on_key("l")
    _type(tui, "11")
    tui._on_key(ENTER)
    tui._on_key(BACKSPACE)
    tui._on_key(BACKSPACE)
    _type(tui, "0")
    tui._on_key(ENTER)

    assert controller.toggles == [], (
        f"a ceiling the operator never entered reached the controller: "
        f"{controller.toggles}"
    )


def test_an_empty_entry_is_refused_rather_than_defaulted():
    """Backspacing the field empty and pressing Enter must not quietly re-apply
    the config default: the operator cleared it deliberately, and a session that
    starts under a number they just deleted is exactly the surprise this PRD
    exists to remove.

    MUTATION: `value = parsed or self._live_controller.default_max_speakers` on
    the Enter branch.
    """
    controller = _RecordingLiveController(default_max_speakers=8)
    tui = _prompt_tui(controller)

    tui._on_key("l")
    tui._on_key(BACKSPACE)
    tui._on_key(ENTER)

    assert controller.toggles == []
    assert tui._speaker_prompt is not None
    assert "1-10" in _dashes(tui._speaker_prompt.message)


@pytest.mark.parametrize("ch", ["a", "-", ".", " ", "z"])
def test_a_non_numeric_keystroke_is_refused_and_leaves_the_value_alone(ch):
    """U-7's non-numeric half, at the keystroke. The terminal is in cbreak mode,
    so every stray key in the app lands here; a buffer that accepted arbitrary
    text would show the operator a value they then have to unpick character by
    character before the prompt would accept anything.

    Refusing at the keystroke also keeps the displayed value and the committed
    value the same thing at every moment.

    MUTATION: `prompt.value += ch` for any printable character.
    """
    controller = _RecordingLiveController(default_max_speakers=8)
    tui = _prompt_tui(controller)

    tui._on_key("l")
    tui._on_key(ch)

    assert tui._speaker_prompt is not None, f"{ch!r} closed the prompt"
    assert tui._speaker_prompt.value == "8", f"{ch!r} was accepted into the value"
    assert "1-10" in _dashes(tui._speaker_prompt.message), (
        f"{ch!r} was refused without naming the range: "
        f"{tui._speaker_prompt.message!r}"
    )
    assert controller.toggles == []


def test_a_refused_keystroke_does_not_poison_the_next_valid_entry():
    """The refusal is about one keystroke, not about the prompt's state. An
    implementation that latched the message would leave the operator staring at
    a complaint about a key they already corrected.

    MUTATION: never clear `prompt.message` when a subsequent keystroke is
    accepted.
    """
    controller = _RecordingLiveController(default_max_speakers=8)
    tui = _prompt_tui(controller)

    tui._on_key("l")
    tui._on_key("x")
    _type(tui, "3")

    assert tui._speaker_prompt.value == "3"
    assert tui._speaker_prompt.message == "", (
        f"a stale refusal survived a valid keystroke: "
        f"{tui._speaker_prompt.message!r}"
    )

    tui._on_key(ENTER)
    assert controller.toggles == [3]


# -- U-9: the next call is prompted from config, never from the last answer --


def test_a_second_l_re_prompts_from_config_not_from_the_previous_answer():
    """U-9 / FR-2.5, and the failure it prevents is concrete: a 10-person call
    settings 10, the next call is a 1:1, and a silently-carried 10 splits the
    single remote voice across several labels. The value applies to ONE session.

    MUTATION: `self._last_speaker_count = value` on confirm and prepopulate from
    it. Every other test in this file still passes.
    """
    controller = _RecordingLiveController(default_max_speakers=8)
    tui = _prompt_tui(controller)

    tui._on_key("l")
    _type(tui, "10")
    tui._on_key(ENTER)          # session starts at 10
    tui._on_key("l")            # `l` while live stops it

    tui._on_key("l")            # the NEXT call

    assert tui._speaker_prompt is not None
    assert tui._speaker_prompt.value == "8", (
        "the next call was prepopulated from the previous answer, not from config"
    )


def test_a_cancelled_prompt_leaves_no_residue_for_the_next_one():
    """The same requirement on the cancel path: a half-typed entry must not
    reappear in the next prompt.

    MUTATION: keep `self._speaker_prompt` and merely hide it on esc.
    """
    tui = _prompt_tui(_RecordingLiveController(default_max_speakers=8))

    tui._on_key("l")
    _type(tui, "2")
    tui._on_key(ESC)

    tui._on_key("l")

    assert tui._speaker_prompt.value == "8"
    assert tui._speaker_prompt.message == ""


# -- U-10 / U-11: the refusals that precede the prompt ----------------------


def test_a_busy_device_is_refused_before_the_prompt_opens():
    """U-10 / FR-ERR-2. The live stream and an in-flight offload share one USB
    endpoint. Refusing AFTER the operator has chosen a number wastes the
    decision and reads as though the number caused the failure.

    MUTATION: open the prompt first and let `toggle()` raise the refusal on
    confirm.
    """
    refusal = (
        "cannot start live transcription while an offload transfer is in flight "
        "— press l again once the offload finishes"
    )
    controller = _RecordingLiveController(refusal=refusal)
    tui = _prompt_tui(controller)

    tui._on_key("l")

    assert tui._speaker_prompt is None, "the prompt opened over a refused start"
    assert controller.toggles == []
    assert controller.refusal_checks == 1, "the controller was never consulted"


def test_the_busy_refusal_shown_is_the_controllers_own_words():
    """FR-ERR-2 again: only the controller can name the in-flight offload, so the
    TUI must show what it was given rather than inventing a second message that
    drifts from the real condition.

    MUTATION: log a generic "live transcription unavailable" instead of the
    returned reason — the operator then cannot tell a busy device from a missing
    controller.
    """
    refusal = "cannot start live transcription while an offload transfer is in flight"
    tui = _prompt_tui(_RecordingLiveController(refusal=refusal))

    tui._on_key("l")

    assert refusal in _log(tui)


def test_l_without_a_controller_opens_no_prompt():
    """U-11 / FR-ERR-1. A build with no controller wired is the `ad98cbc` shape.
    The existing message stands; what must NOT happen is a prompt the operator
    can fill in and confirm into nothing.

    MUTATION: open the prompt before the `_live_controller is None` check — the
    operator then types a number, presses Enter, and the keystroke dies against
    a None.
    """
    tui = _prompt_tui(None)

    tui._on_key("l")  # must not raise

    assert tui._speaker_prompt is None
    _when, message, severity = list(tui._log)[-1]
    assert "live" in message.lower()
    assert severity is Severity.WARNING


# -- the prompt owns the keyboard while it is open --------------------------


def test_keys_do_not_leak_to_top_level_bindings_while_the_prompt_is_open():
    """A modal owns the keyboard, exactly as the retry confirm does. `w` while
    the prompt is open must not open the whisper selector behind it.

    The whispers matter: with an empty pending bucket the top-level `w` branch
    cannot open the selector even when the key DOES leak, so this test used to
    pass against the very dispatch order it names. One pending whisper is what
    makes the leak observable.

    MUTATION: dispatch the prompt AFTER the top-level `w`/`u` branches.
    """
    tui = _prompt_tui(_RecordingLiveController())
    tui._pending_whispers_provider = lambda: [SimpleNamespace(name="a.hda", size=1)]

    # The bucket is non-empty, so `w` at top level WOULD open the selector.
    tui._on_key("w")
    assert tui._whisper_modal is not None, (
        "the fixture cannot observe a leak: `w` does not open the selector even "
        "at top level, so this test would pass against any dispatch order"
    )
    tui._whisper_modal = None

    tui._on_key("l")
    tui._on_key("w")

    assert tui._whisper_modal is None, "`w` leaked past the open prompt"
    assert tui._speaker_prompt is not None, "an unhandled key closed the prompt"


def test_the_prompt_does_not_steal_keys_from_the_modals_already_open():
    """The reverse direction. `l` is free at top level, but the whisper modal
    owns its own key set, and a prompt opened from a keystroke aimed elsewhere
    is one Enter away from a metered session.

    MUTATION: dispatch the speaker prompt ahead of the modal routing at the top
    of `_on_key`.
    """
    controller = _RecordingLiveController()
    tui = _prompt_tui(controller)
    tui._whisper_modal = WhisperSelectionState(filenames=["a.hda"])

    tui._on_key("l")

    assert tui._speaker_prompt is None
    assert tui._whisper_modal is not None
    assert controller.toggles == []


# -- U-13: the presentation path, not the formatter -------------------------
#
# Rendered through `tui._render()` — the OUTERMOST entry point — rather than
# through the panel builder. The presentation-path rule exists because a
# formatter test cannot see a crash in composition, and cannot see a pane that
# crops the prompt away: `_render` is what the operator's screen actually shows.


def test_the_prompt_is_rendered_in_the_frame():
    """U-13. The defect this closes has shipped in this codebase before:
    `_retry_confirm` was set by the keystroke and read by nothing, so `r`
    appeared to do nothing at all (`f6d1333`).

    MUTATION: set `self._speaker_prompt` in `_on_key` and never add its branch
    to `_render`'s center chain. Every behavioural test above stays green and
    the operator presses `l` and sees nothing.
    """
    tui = _prompt_tui(_RecordingLiveController(default_max_speakers=8))

    tui._on_key("l")

    text = _text(tui._render())
    assert "8" in text
    assert "speaker" in text.lower(), f"the prompt does not name what it wants: {text}"


def test_the_rendered_prompt_states_the_range_and_both_exits():
    """FR-1.3 / FR-2.1 on screen. The operator is choosing a number under a cap
    they cannot infer, on a modal whose two exits are not otherwise discoverable
    — the keyboard is in cbreak mode and there is no other affordance.

    MUTATION: drop the hint line from the panel body.
    """
    tui = _prompt_tui(_RecordingLiveController())

    tui._on_key("l")
    text = _text(tui._render()).lower()

    assert "1-10" in text, f"the rendered prompt does not state the range: {text}"
    assert "enter" in text
    assert "esc" in text


def test_the_rendered_prompt_carries_the_vendors_own_guidance():
    """FR-1.4. The trade is not guessable from the number alone, and it is
    counter-intuitive in one direction: too HIGH is also wrong. AssemblyAI's own
    words are "give the model a little headroom above the number of speakers you
    expect" and "setting it too high can cause over-splitting" — without that on
    screen, an operator avoiding the merge failure walks straight into the split
    one, which is what happened on the 1:1 call.

    MUTATION: render the value and the range only.
    """
    tui = _prompt_tui(_RecordingLiveController())

    tui._on_key("l")
    text = _text(tui._render()).lower()

    assert "headroom" in text, f"the prompt omits the vendor's guidance: {text}"
    assert "split" in text, f"the prompt omits the over-splitting warning: {text}"


def test_the_rendered_prompt_tracks_the_edits():
    """U-3's display half, on the presentation path. A field whose displayed
    value lags the buffer would have the operator confirm a number other than
    the one on screen.

    `7` on purpose, and the absence check on purpose: the panel also states the
    range `1-10`, so `"10" in text` would be satisfied by the hint line while the
    field itself still displayed the config default. 7 appears nowhere else.

    MUTATION: render `self._live_controller.default_max_speakers` instead of the
    prompt's own value.
    """
    tui = _prompt_tui(_RecordingLiveController(default_max_speakers=8))

    tui._on_key("l")
    _type(tui, "7")

    text = _text(tui._render())
    assert re.search(r"\b7\b", text), f"the typed value is not on screen: {text}"
    assert re.search(r"\b8\b", text) is None, (
        f"the prepopulated value is still on screen after being replaced: {text}"
    )


def test_a_refusal_is_rendered_where_the_operator_is_looking():
    """U-7's message has to reach the SCREEN, not only the prompt object. The
    operator is looking at the modal they just pressed Enter on; a reason that
    only exists in a field, or only in the activity log below, is a prompt that
    silently refuses.

    Asserting `"1-10" in text` is NOT enough and used to be all this test did:
    the static guidance line carries that substring on every render, so deleting
    the refusal render entirely left this green and U-7's on-screen half
    untested. It now asserts the prompt's OWN message — the string the
    implementation actually produced — and separately proves that string is
    absent before the refusal, so a permanently-rendered line cannot satisfy it.

    MUTATION: set `prompt.message` and never render it.
    """
    tui = _prompt_tui(_RecordingLiveController())

    tui._on_key("l")
    before = _text(tui._render())

    _type(tui, "11")
    tui._on_key(ENTER)

    # `_text` normalises em/en dashes, so compare the message the same way the
    # rendered frame is compared — otherwise this fails on the dash character
    # rather than on whether the refusal reached the screen.
    message = _dashes(tui._speaker_prompt.message)
    assert message, "the implementation produced no refusal to render"
    assert message not in before, (
        "the refusal text is already on screen before anything was refused, so "
        f"asserting it proves nothing: {message!r}"
    )

    text = _text(tui._render())
    assert message in text, f"the refusal never reached the screen: {text}"


def test_the_prompt_survives_a_short_terminal():
    """The panel is the only place the operator can read what they are
    committing, and this app is routinely run in a small window — the activity
    log's own cropping defect (`971a2a8`) was found exactly there.

    MUTATION: render the prompt below a full-height log, so the pane crops it
    away at ordinary terminal heights.
    """
    tui = _prompt_tui(_RecordingLiveController(default_max_speakers=8))
    for i in range(20):
        tui._log_key_ignored(f"noise {i}")

    tui._on_key("l")

    text = _text(tui._render(), width=80, height=24)
    assert "8" in text
    assert "1-10" in text, f"the prompt was cropped out of a 24-row terminal: {text}"
