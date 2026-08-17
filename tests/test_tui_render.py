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
import threading

from rich.console import Console
from rich.layout import Layout

from hidock_direct.events import EventBus, RetryCandidatesDetected
from hidock_direct.tui import TUI
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


def _footer_text_of(layout: Layout) -> str:
    """Text of the footer panel produced by a full `_render()` pass.

    Reads the panel out of the composed Layout rather than the whole frame, so
    the assertion exercises the real path (snapshot under lock -> parameter ->
    formatter -> panel) without depending on the frame's footer pane being tall
    enough to display it. It is not: the pane is fixed at `size=3`, which fits
    one content line, so the badge's second line is clipped from the live
    display. That is a separate, pre-existing defect — it hides the whisper and
    unknown badges too, and predates the retry work — tracked at
    `bug_report_footer_pane_clips_the_pending_badge_line.md`.
    """
    return _text_of(layout["footer"].renderable)


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
    bus = EventBus()
    tui = TUI(bus=bus)

    bus.publish(RetryCandidatesDetected(count=3))
    text = _footer_text_of(tui._render())

    assert "3 failed transcriptions" in text
    assert "press r to retry" in text


def test_footer_badge_is_singular_for_one_failure_and_absent_for_none():
    """Edge cases either side of the badge: the singular label, and the empty
    case where the footer must still render without a pending line at all."""
    bus = EventBus()
    tui = TUI(bus=bus)

    bus.publish(RetryCandidatesDetected(count=1))
    text = _footer_text_of(tui._render())
    assert "1 failed transcription " in text, "expected the singular label"

    bus.publish(RetryCandidatesDetected(count=0))
    text = _footer_text_of(tui._render())
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
