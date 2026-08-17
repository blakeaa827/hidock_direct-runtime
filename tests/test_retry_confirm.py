"""The retry confirm and its run region — PRD v3 FR-10, FR-11, FR-12.

`f6d1333` shipped the `r` binding and left the confirm unbuilt: pressing `r`
loaded candidates into `_retry_confirm` and nothing read it, so the key appeared
to do nothing at all. These cover the three requirements that close it — the
confirm panel and its y/f/esc handling (FR-10), a progress and summary region
that outlives the ten-entry log deque (FR-11), and redaction of upload URLs in
anything rendered (FR-12).
"""

from __future__ import annotations

import io
import re
import threading
import time

import pytest
from rich.console import Console

from hidock_direct.events import Error, EventBus, RetryFinished, RetryProgress, Severity
from hidock_direct.retry import RetryCandidate, format_retry_confirm, summarize
from hidock_direct.tui import TUI


def _cand(
    key="2026/08/a.mp3",
    *,
    exists=True,
    paid=False,
    status="error",
    error="connection reset",
    minutes=30.0,
):
    from pathlib import Path

    from hidock_direct.retry import classify_failure

    return RetryCandidate(
        state_key=key,
        path=Path("/archive") / key,
        exists_on_disk=exists,
        has_paid_transcript=paid,
        status=status,
        retry_count=0,
        error=error,
        error_class=classify_failure(error),
        recorded_at=None,
        duration_minutes=minutes,
    )


def _text(renderable, width=100, height=None) -> str:
    buf = io.StringIO()
    Console(file=buf, width=width, height=height).print(renderable)
    return re.sub(r"\s+", " ", re.sub(r"[│╭╮╰╯─]", " ", buf.getvalue()))


# -- FR-10: the confirm ------------------------------------------------------


def test_confirm_shows_count_hours_cost_and_excluded_breakdown():
    """The PRD's worked example: how many, how long, what it costs, and what is
    being left out — the operator is about to spend money."""
    candidates = [
        _cand("2026/08/a.mp3", minutes=60.0),
        _cand("2026/08/b.mp3", minutes=30.0),
        _cand("2026/08/c.mp3", paid=True, minutes=45.0),
        _cand("2026/08/d.mp3", error="no spoken audio", minutes=5.0),
        _cand("2026/08/e.mp3", exists=False, minutes=0.0),
    ]

    lines = format_retry_confirm(summarize(candidates))
    text = " ".join(lines)

    assert "Retry 5 failed transcriptions?" in text
    assert "2 to transcribe" in text
    assert "1.5 h" in text
    assert "$0.32" in text, "1.5h at the `best` rate of $0.21/h"
    assert "1 already paid" in text and "$0" in text
    assert "2 excluded" in text
    assert "retrying will not help" in text and "file missing from archive" in text
    assert "[y] start" in text and "[f] force-include" in text and "[esc] cancel" in text


def test_confirm_renders_unknown_duration_as_unknown_never_as_zero_dollars():
    """PRD §4 test 10, called out explicitly: an unreadable duration must render
    `unknown`, never `$0.00`. A confident zero reads as 'this is free'."""
    candidates = [_cand("2026/08/a.mp3", minutes=0.0)]

    text = " ".join(format_retry_confirm(summarize(candidates)))

    assert "unknown" in text
    assert "$0.00" not in text


def test_confirm_omits_the_excluded_line_when_nothing_is_excluded():
    text = " ".join(format_retry_confirm(summarize([_cand(minutes=60.0)])))

    assert "excluded" not in text
    assert "1 to transcribe" in text


def test_confirm_singularizes_one_failed_transcription():
    text = " ".join(format_retry_confirm(summarize([_cand(minutes=60.0)])))

    assert "Retry 1 failed transcription?" in text


# -- FR-10: the keys ---------------------------------------------------------


def _tui_with_confirm(candidates, started):
    tui = TUI(bus=EventBus(), retry_candidates_provider=lambda: candidates)
    tui._state = "IDLE_DISCONNECTED"
    tui._start_retry_batch = lambda sel: started.append(list(sel))  # type: ignore[method-assign]
    tui._on_key("r")
    assert tui._retry_confirm is not None, "confirm did not open"
    return tui


def test_y_starts_the_batch_with_the_default_selected_set_only():
    """Default set excludes what retrying cannot fix (FR-4). `y` honors it."""
    started: list = []
    candidates = [
        _cand("2026/08/a.mp3"),
        _cand("2026/08/b.mp3", error="no spoken audio"),
        _cand("2026/08/c.mp3", exists=False),
    ]
    tui = _tui_with_confirm(candidates, started)

    tui._on_key("y")

    assert len(started) == 1
    assert [c.state_key for c in started[0]] == ["2026/08/a.mp3"]
    assert tui._retry_confirm is None, "confirm must close once the batch starts"


def test_f_force_includes_excluded_candidates_that_are_still_on_disk():
    """`f` is the operator overriding the default set — but a file that is not on
    disk cannot be retried by anyone, so it stays out."""
    started: list = []
    candidates = [
        _cand("2026/08/a.mp3"),
        _cand("2026/08/b.mp3", error="no spoken audio"),
        _cand("2026/08/c.mp3", exists=False),
    ]
    tui = _tui_with_confirm(candidates, started)

    tui._on_key("f")

    assert [c.state_key for c in started[0]] == ["2026/08/a.mp3", "2026/08/b.mp3"]


def test_esc_cancels_the_confirm_without_starting_anything():
    started: list = []
    tui = _tui_with_confirm([_cand()], started)

    tui._on_key("\x1b")

    assert started == [], "esc must not start a batch"
    assert tui._retry_confirm is None


def test_keys_do_not_leak_to_top_level_bindings_while_the_confirm_is_open():
    """A modal owns the keyboard. `w` while the confirm is open must not open the
    whisper selector behind it."""
    started: list = []
    tui = _tui_with_confirm([_cand()], started)

    tui._on_key("w")

    assert tui._whisper_modal is None
    assert tui._retry_confirm is not None, "an unhandled key must not close the confirm"
    assert started == []


def test_confirm_is_rendered_in_the_frame():
    """The defect this closes: `_retry_confirm` was set and never read, so the
    key did nothing observable."""
    tui = _tui_with_confirm([_cand(minutes=60.0)], [])

    assert "Retry 1 failed transcription?" in _text(tui._render())


# -- FR-11: progress and summary outlive the log ----------------------------


def test_progress_shows_current_file_index_and_total():
    bus = EventBus()
    tui = TUI(bus=bus)

    bus.publish(RetryProgress(index=2, total=5, filename="2026/08/b.mp3"))

    text = _text(tui._render())
    assert "2/5" in text
    assert "2026/08/b.mp3" in text


def test_summary_survives_eviction_from_the_ten_entry_log():
    """The requirement's whole point: a 12-file run emits more events than the
    deque holds, so a summary written into the log would evict itself."""
    bus = EventBus()
    tui = TUI(bus=bus)

    bus.publish(RetryFinished(succeeded=9, re_rendered=2, failed=1, not_attempted=0))
    for i in range(20):
        bus.publish(Error(message=f"noise {i}", severity=Severity.INFO))

    # Height 40 = a normal terminal. Below ~25 rows the region pushes the
    # newest log line off the bottom; filed separately, not ratified here.
    text = _text(tui._render(), height=40)
    assert "9 transcribed" in text and "2 re-rendered" in text and "1 failed" in text
    assert "noise 19" in text, "the log itself must still render"


def test_esc_clears_the_run_region():
    bus = EventBus()
    tui = TUI(bus=bus)
    tui._state = "IDLE_DISCONNECTED"
    bus.publish(RetryFinished(succeeded=1, re_rendered=0, failed=0, not_attempted=0))
    assert "1 transcribed" in _text(tui._render())

    tui._on_key("\x1b")

    assert "1 transcribed" not in _text(tui._render())


def test_a_new_run_replaces_the_previous_summary():
    bus = EventBus()
    tui = TUI(bus=bus)
    bus.publish(RetryFinished(succeeded=9, re_rendered=0, failed=0, not_attempted=0))

    bus.publish(RetryProgress(index=1, total=3, filename="2026/08/z.mp3"))

    text = _text(tui._render())
    assert "9 transcribed" not in text, "a new run must clear the prior summary"
    assert "1/3" in text


# -- FR-12: redaction --------------------------------------------------------


def test_rendered_failure_text_redacts_upload_urls():
    """The exposure is introduced by surfacing AAI errors at all — the operator
    screenshots this app and the repo is public."""
    bus = EventBus()
    tui = TUI(bus=bus)
    url = "https://cdn.assemblyai.com/upload/9f3c-secret-account-token"

    bus.publish(
        RetryFinished(
            succeeded=0,
            re_rendered=0,
            failed=1,
            not_attempted=0,
            aborted_reason=f"upload failed for {url} after 2 tries",
        )
    )

    text = _text(tui._render(), width=200)
    assert "cdn.assemblyai.com/upload" not in text
    assert "<upload-url redacted>" in text


# -- the batch must not run on the keyboard thread ---------------------------


def test_batch_runs_off_the_keyboard_thread():
    """`run_retry_batch` uploads audio and waits on AssemblyAI. Running it inline
    would freeze every other key — including the esc that cancels it."""
    started = threading.Event()
    release = threading.Event()
    key_thread = threading.current_thread().name
    ran_on: list[str] = []

    def slow_batch(selected):
        ran_on.append(threading.current_thread().name)
        started.set()
        release.wait(timeout=5)

    tui = TUI(bus=EventBus(), retry_candidates_provider=lambda: [_cand()])
    tui._state = "IDLE_DISCONNECTED"
    tui._run_retry_batch = slow_batch  # type: ignore[method-assign]
    tui._on_key("r")

    tui._on_key("y")  # must return immediately, not block on slow_batch

    assert started.wait(timeout=5), "batch never started"
    release.set()
    assert ran_on and ran_on[0] != key_thread, f"batch ran on the keyboard thread ({ran_on})"


def test_badge_is_refreshed_from_the_ledger_after_the_batch():
    """`RetryCandidatesDetected` documents itself as published "at startup and
    after every retry batch". Caught at Gate 4: without the refresh the footer
    keeps advertising the pre-run count, so a batch that fixed everything still
    reads as N failed."""
    remaining = [_cand("a.mp3"), _cand("b.mp3"), _cand("c.mp3")]
    bus = EventBus()
    tui = TUI(
        bus=bus,
        retry_candidates_provider=lambda: list(remaining),
        retry_runner=lambda sel: remaining.clear(),
    )
    tui._retry_progress = None
    bus.publish(RetryFinished(succeeded=0, re_rendered=0, failed=0, not_attempted=0))
    tui._failed_count = 3

    tui._run_retry_batch([_cand()])  # inline; the thread hop is covered separately

    assert tui._failed_count == 0, "badge still advertises the pre-run count"


def test_badge_refresh_failure_does_not_crash_the_batch_thread():
    """An unreadable ledger after the run must be logged, not raised — the batch
    already did its work and the thread has nowhere to report a crash."""
    bus = EventBus()

    def boom():
        raise RuntimeError("ledger vanished")

    tui = TUI(bus=bus, retry_candidates_provider=boom, retry_runner=lambda sel: None)

    tui._run_retry_batch([_cand()])

    assert any("not refreshed" in m for _, m, _ in tui._log)


def test_a_crashing_batch_reports_a_redacted_message():
    """FR-12 covers "anything rendered", not just the abort reason. The runner's
    exception lands in the activity log and can carry AAI text — caught by the
    Gate 2 sweep over every path that moves error text to a rendered surface."""
    url = "https://cdn.assemblyai.com/upload/9f3c-secret-account-token"
    bus = EventBus()
    seen: list = []
    bus.subscribe(seen.append)

    def boom(selected):
        raise RuntimeError(f"upload failed for {url}")

    tui = TUI(bus=bus, retry_candidates_provider=lambda: [], retry_runner=boom)

    tui._run_retry_batch([_cand()])

    messages = [e.message for e in seen if isinstance(e, Error)]
    assert messages, "a crashing batch must surface an Error"
    assert not any("cdn.assemblyai.com/upload" in m for m in messages)
    assert any("<upload-url redacted>" in m for m in messages)
