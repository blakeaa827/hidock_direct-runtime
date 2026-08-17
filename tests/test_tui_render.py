"""The render path — the surface that had no tests until it crashed on launch.

`f6d1333` shipped six green tests for the failed-transcription badge: three
covering the pure `format_pending_footer` formatter, three covering the `r`
binding and its handler. The step BETWEEN them — actually drawing the footer —
was never exercised, so a `@staticmethod` reaching for `self` passed a 221-test
suite and raised `NameError` on the first frame of every launch.

These tests exercise `_render()` itself. They are deliberately about the render
*path*, not about the badge alone: a test that only pinned the new parameter
would leave the other five `_render_*` helpers as uncovered as they were.
"""

from __future__ import annotations

import inspect
import io
import re
import threading

import pytest

from rich.console import Console
from rich.layout import Layout

from hidock_direct.events import (
    EventBus,
    RetryCandidatesDetected,
    UnknownsDetected,
    WhispersDetected,
)
from hidock_direct.tui import TUI, format_pending_footer
from hidock_direct.tui_handlers import WhisperSelectionState


def _text_of(renderable, width: int = 100, height: int = 40) -> str:
    """Render to plain text so assertions can read what an operator sees.

    Writes to a StringIO rather than using `Console(record=True).export_text()`:
    the recorder returns only the first panel for a split Layout, which silently
    hides the footer — the panel these tests are about.
    """
    buf = io.StringIO()
    console = Console(file=buf, width=width, height=height)
    console.print(renderable)
    return buf.getvalue()


def _tui_at_width(width: int, height: int = 40) -> tuple[TUI, io.StringIO]:
    """A TUI whose own console has a known size.

    The TUI must measure and render against the SAME console — in production
    that is the one `Live` draws with. Injecting it here keeps the test faithful
    and lets each case pick a width, which matters: the pending line wraps, so
    the footer's height is width-dependent.
    """
    buf = io.StringIO()
    tui = TUI(bus=EventBus(), console=Console(file=buf, width=width, height=height))
    return tui, buf


def _frame_text(tui: TUI) -> str:
    """The composed frame as the operator sees it — every pane, cropped as the
    layout crops it. Assertions about operator-visible strings go through here,
    never against a panel read out of the Layout: a panel-level assertion cannot
    see a pane too short to display its own content.

    Box-drawing characters are stripped and whitespace collapsed, so a phrase
    that wraps across a line boundary ("[press r" / "to retry]" at width 60)
    still matches. Without that, a narrow-terminal test fails on its own
    assertion while the frame is perfectly correct.
    """
    buf = io.StringIO()
    console = Console(file=buf, width=tui._console.width, height=tui._console.height)
    console.print(tui._render())
    stripped = re.sub(r"[│╭╮╰╯─]", " ", buf.getvalue())
    return re.sub(r"\s+", " ", stripped)


def test_render_produces_a_layout_in_the_default_state():
    """The regression test for the crash, and the test that should have existed
    before the badge was added. A freshly-constructed TUI must be able to draw a
    frame with no device, no archive and no events consumed — that is the state
    every launch starts in, and it is where the NameError fired."""
    tui = TUI(bus=EventBus())

    layout = tui._render()

    assert isinstance(layout, Layout)


def test_render_survives_every_reachable_center_panel():
    """`_render` branches three ways for the center panel. One passing branch
    says nothing about the other two, so drive each."""
    tui = TUI(bus=EventBus())

    # 1. log (no modal, no queue) — the default.
    assert isinstance(tui._render(), Layout)

    # 2. whisper modal takes priority over the log.
    tui._whisper_modal = WhisperSelectionState(filenames=["a.hda", "b.hda"])
    assert isinstance(tui._render(), Layout)

    # 3. unknown prompt, once the modal is dismissed.
    tui._whisper_modal = None
    tui._unknown_queue = ["mystery.hda"]
    assert isinstance(tui._render(), Layout)


def test_footer_shows_the_failed_badge_after_retry_candidates_detected():
    """The end-to-end assertion the six badge tests never made: they verified the
    pure formatter and the event handler on either side of the render step, and
    the render step was the broken one. Drive the event, draw the frame, read the
    operator-visible text."""
    tui, _ = _tui_at_width(100)

    tui._bus.publish(RetryCandidatesDetected(count=3))
    text = _frame_text(tui)

    assert "3 failed transcriptions" in text
    assert "press r to retry" in text


def test_footer_badge_is_singular_for_one_failure_and_absent_for_none():
    """Edge cases either side of the badge: the singular label, and the empty
    case where the footer must still render without a pending line at all."""
    tui, _ = _tui_at_width(100)

    tui._bus.publish(RetryCandidatesDetected(count=1))
    text = _frame_text(tui)
    assert "1 failed transcription " in text, "expected the singular label"

    tui._bus.publish(RetryCandidatesDetected(count=0))
    text = _frame_text(tui)
    assert "failed transcription" not in text
    assert "Session: pulled 0 files" in text, "footer must still render when nothing is pending"


def test_render_footer_takes_the_failed_count_as_a_parameter():
    """Locks the fix shape. `_render_footer` is a staticmethod and every other
    footer input arrives as a parameter; reaching for instance state from inside
    it is what broke, so pin the signature and prove it is callable unbound."""
    params = list(inspect.signature(TUI._render_footer).parameters)

    assert len(params) == 5, f"expected five parameters, got {params}"
    assert "self" not in params

    panel = TUI._render_footer(0, 0, 0, 0, 0)
    assert panel is not None


class _LockProbe:
    """Wraps an RLock and reports whether it is currently held."""

    def __init__(self, real: threading.RLock) -> None:
        self._real = real
        self._depth = 0

    def __enter__(self):
        self._real.acquire()
        self._depth += 1
        return self

    def __exit__(self, *exc):
        self._depth -= 1
        self._real.release()
        return False

    @property
    def held(self) -> bool:
        return self._depth > 0


class _ProbedTUI(TUI):
    """Records whether the lock was held on each read of `_failed_count`."""

    def __init__(self, **kwargs):
        self._probe_value = 0
        self._probe_reads: list[bool] = []
        super().__init__(**kwargs)

    @property
    def _failed_count(self) -> int:
        self._probe_reads.append(getattr(self._lock, "held", False))
        return self._probe_value

    @_failed_count.setter
    def _failed_count(self, value: int) -> None:
        self._probe_value = value


def test_render_reads_failed_count_under_the_lock():
    """The second defect at the same site. `_render` snapshots seven other shared
    fields under `self._lock` because they are mutated on the event thread;
    `_failed_count` is mutated there too (`_on_event`) but was never added to the
    snapshot. Making `self` merely reachable would trade a loud crash for an
    unsynchronized cross-thread read."""
    tui = _ProbedTUI(bus=EventBus())
    tui._lock = _LockProbe(tui._lock)

    tui._render()

    assert tui._probe_reads, "_render never read _failed_count at all"
    assert all(tui._probe_reads), (
        f"_failed_count was read outside the lock: {tui._probe_reads}"
    )


# -- the footer pane must be tall enough to display its own panel -------------
#
# Regression tests for `bug_report_footer_pane_clips_the_pending_badge_line.md`.
# The pane was hardcoded `size=3`; a rich Panel spends two rows on borders, so
# only one content line survived and every badge was cropped from the display.


@pytest.mark.parametrize(
    "event, fragment",
    [
        (WhispersDetected(count=2), "2 whispers on device [press w to pick]"),
        (UnknownsDetected(count=1, filenames=["mystery.hda"]), "1 unknown [press u to review]"),
        (RetryCandidatesDetected(count=3), "3 failed transcriptions [press r to retry]"),
    ],
    ids=["whispers", "unknowns", "failed"],
)
def test_pending_badge_is_visible_in_the_rendered_frame(event, fragment):
    """Every badge kind, asserted against the displayed frame. The whisper and
    unknown badges have been cropped since 2026-04-23, so this is not a
    retry-only regression — all three shipped invisible."""
    tui, _ = _tui_at_width(140)

    tui._bus.publish(event)

    assert fragment in _frame_text(tui)


@pytest.mark.parametrize("whispers", [0, 2], ids=["w0", "w2"])
@pytest.mark.parametrize("unknowns", [0, 1], ids=["u0", "u1"])
@pytest.mark.parametrize("failed", [0, 3], ids=["f0", "f3"])
def test_footer_pane_is_tall_enough_for_every_fragment_combination(whispers, unknowns, failed):
    """The invariant, across all 8 present/absent combinations: whatever
    `format_pending_footer` produced must survive into the frame.

    Asserted as an invariant rather than against a literal height so it still
    holds when a fourth fragment is added — the failure mode that produced this
    bug was a constant that stopped matching the content."""
    tui, _ = _tui_at_width(140)
    tui._bus.publish(WhispersDetected(count=whispers))
    tui._bus.publish(UnknownsDetected(count=unknowns, filenames=["m.hda"] * unknowns))
    tui._bus.publish(RetryCandidatesDetected(count=failed))

    text = _frame_text(tui)
    pending = format_pending_footer(whispers, unknowns, failed)

    assert "Session: pulled 0 files" in text, "the session line must always render"
    if pending:
        for fragment in pending.split("   "):
            assert fragment in text, f"cropped from the frame: {fragment!r}"


@pytest.mark.parametrize("width", [60, 80, 100, 140, 200], ids=lambda w: f"w{w}")
def test_badge_survives_narrow_terminals_where_the_pending_line_wraps(width):
    """The case both fixed-height proposals get wrong.

    The three fragments are joined into ONE line, so `3 + len(fragments)` is not
    the height; the real driver is wrapping. That line is 115 characters, so the
    panel needs 6 rows at width 60, 5 at 80-100 and 4 at 140+. A constant is
    correct at one width and wrong at the others, which is why the pane is
    measured against the console it will be drawn on."""
    tui, _ = _tui_at_width(width)
    tui._bus.publish(WhispersDetected(count=2))
    tui._bus.publish(UnknownsDetected(count=1, filenames=["m.hda"]))
    tui._bus.publish(RetryCandidatesDetected(count=3))

    text = _frame_text(tui)

    # The last fragment is the one a too-short pane drops first.
    assert "press r to retry" in text


def test_footer_pane_does_not_waste_rows_when_nothing_is_pending():
    """The cost side of measuring: an idle footer must not reserve rows for a
    badge that is not there. Guards against 'just make it size=5'."""
    tui, _ = _tui_at_width(140)

    layout = tui._render()
    panel = layout["footer"].renderable
    options = tui._console.options.update(height=None)
    needed = len(tui._console.render_lines(panel, options, pad=False))

    assert layout["footer"].size == needed, (
        f"footer pane reserves {layout['footer'].size} rows for a {needed}-row panel"
    )
