"""Operator-initiated retry (transcription_retry_prd.md v3).

The regression that matters most is `test_operator_retry_never_escalates`: without
it, three presses of `r` against a dead AssemblyAI balance move every candidate to
`error_permanent`, which `should_skip` treats as terminal — the retry surface
would bury the files it exists to recover.
"""

from __future__ import annotations

import inspect
import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from hidock_direct import transcribe as transcribe_mod
from hidock_direct.events import EventBus
from hidock_direct.retry import (
    CLASS_OPERATOR_ACTIONABLE,
    CLASS_PERMANENT_DATA,
    CLASS_TRANSIENT,
    LedgerUnavailable,
    classify_failure,
    find_retry_candidates,
    redact,
    run_retry_batch,
    summarize,
)
from tests.conftest import call_kwargs_in, faithful

BALANCE_ERROR = (
    "AssemblyAI request failed: failed to transcribe url "
    "https://cdn.assemblyai.com/upload/44b5079e722d355038aa3852fa29cfda/751d6e7e: "
    "Your current account balance is negative. Please top up to continue using the API."
)
NO_SPEECH_ERROR = (
    "AssemblyAI returned status=error: language_detection cannot be performed on "
    "files with no spoken audio."
)


class _FakeState:
    def __init__(self, files: dict):
        self.data = {"files": files, "global": {}}


def _entry(status="error", *, error="boom", retry_count=1, marked=None):
    return {
        "status": status,
        "error": error,
        "retry_count": retry_count,
        "marked_in_flight_at": marked,
    }


def _archive(tmp_path: Path, *keys: str) -> Path:
    root = tmp_path / "archive"
    for key in keys:
        p = root / key
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"\x00" * 32)
    root.mkdir(parents=True, exist_ok=True)
    return root


# --- classification ---------------------------------------------------------


@pytest.mark.parametrize(
    "text,expected",
    [
        (BALANCE_ERROR, CLASS_OPERATOR_ACTIONABLE),
        ("AssemblyAI request failed: 401 Unauthorized", CLASS_OPERATOR_ACTIONABLE),
        (NO_SPEECH_ERROR, CLASS_PERMANENT_DATA),
        ("source file is too large (6000000000 bytes > 5368709120)", CLASS_PERMANENT_DATA),
        ("AssemblyAI request failed: [Errno 8] nodename nor servname provided", CLASS_TRANSIENT),
        ("aai timeout: waited 30m", CLASS_TRANSIENT),
        ("something nobody has ever seen", CLASS_TRANSIENT),
        ("", CLASS_TRANSIENT),
        (None, CLASS_TRANSIENT),
    ],
)
def test_classify_failure_uses_production_error_text(text, expected):
    assert classify_failure(text) == expected


def test_redact_strips_account_scoped_upload_urls():
    out = redact(BALANCE_ERROR)
    assert "cdn.assemblyai.com/upload/" not in out
    assert "<upload-url redacted>" in out
    assert "balance is negative" in out, "the actionable part must survive redaction"


# --- discovery --------------------------------------------------------------


def test_candidate_set_and_default_selection(tmp_path):
    root = _archive(tmp_path, "2026/08/a.mp3", "2026/08/perm.mp3", "2026/04/quiet.mp3")
    state = _FakeState(
        {
            "2026/08/a.mp3": _entry(error=BALANCE_ERROR),
            "2026/08/perm.mp3": _entry("error_permanent", error=BALANCE_ERROR, retry_count=3),
            "2026/04/quiet.mp3": _entry(error=NO_SPEECH_ERROR),
            "2026/08/gone.mp3": _entry(error=BALANCE_ERROR),
            "2026/08/ok.mp3": {"status": "done"},
            "2026/08/pend.mp3": {"status": "pending"},
        }
    )
    got = {c.state_key: c for c in find_retry_candidates(state, root)}

    assert set(got) == {
        "2026/08/a.mp3",
        "2026/08/perm.mp3",
        "2026/04/quiet.mp3",
        "2026/08/gone.mp3",
    }, "done and pending are not retryable"
    assert got["2026/08/a.mp3"].default_selected is True
    assert got["2026/08/perm.mp3"].default_selected is False
    assert got["2026/04/quiet.mp3"].default_selected is False
    assert got["2026/08/gone.mp3"].exists_on_disk is False
    assert got["2026/08/gone.mp3"].default_selected is False
    for key in ("2026/08/perm.mp3", "2026/04/quiet.mp3", "2026/08/gone.mp3"):
        assert got[key].exclusion_reason, f"{key} must say why it is excluded"


def test_stale_in_flight_included_fresh_excluded(tmp_path):
    root = _archive(tmp_path, "2026/08/stale.mp3", "2026/08/fresh.mp3")
    now = datetime.now().astimezone()
    state = _FakeState(
        {
            "2026/08/stale.mp3": _entry(
                "in_flight", marked=(now - timedelta(minutes=90)).isoformat()
            ),
            "2026/08/fresh.mp3": _entry(
                "in_flight", marked=(now - timedelta(minutes=5)).isoformat()
            ),
        }
    )
    keys = {c.state_key for c in find_retry_candidates(state, root, in_flight_ttl_minutes=60)}
    assert keys == {"2026/08/stale.mp3"}


def test_paid_transcript_detected(tmp_path):
    root = _archive(tmp_path, "2026/08/paid.mp3", "2026/08/unpaid.mp3", "2026/08/blank.mp3")
    (root / "2026/08/paid.aai.json").write_text(json.dumps({"id": "x"}))
    (root / "2026/08/blank.aai.json").write_text("")
    state = _FakeState(
        {
            "2026/08/paid.mp3": _entry(),
            "2026/08/unpaid.mp3": _entry(),
            "2026/08/blank.mp3": _entry(),
        }
    )
    got = {c.state_key: c.has_paid_transcript for c in find_retry_candidates(state, root)}
    assert got == {
        "2026/08/paid.mp3": True,
        "2026/08/unpaid.mp3": False,
        "2026/08/blank.mp3": False,
    }


def test_missing_archive_raises_rather_than_reporting_no_candidates(tmp_path):
    """A Drive mount that has not materialized must not read as an all-clear."""
    with pytest.raises(LedgerUnavailable):
        find_retry_candidates(_FakeState({}), tmp_path / "not-here")


# --- execution --------------------------------------------------------------


def _runner(tmp_path, entries_after, *, calls=None):
    """Drive a batch with an injected transcribe + ledger reader."""
    root = _archive(tmp_path, *entries_after)
    state = _FakeState({k: _entry(error=BALANCE_ERROR) for k in entries_after})
    cands = find_retry_candidates(state, root)
    seen = calls if calls is not None else []

    def fake_transcribe(path, archive, **kw):
        seen.append((path.name, kw))
        return None

    # Bound to the real seam's signature. A double that accepts more than
    # `transcribe_file` does is what let the kwarg mismatch ship green through
    # 278 tests — `**kw` here silently absorbed `max_retries` and
    # `reuse_existing_transcript`, which production rejects at the call.
    return root, cands, seen, faithful(transcribe_mod.transcribe_file, fake_transcribe)


def test_operator_retry_never_escalates(tmp_path):
    """FR-3. Every call must carry max_retries=None so repeated presses cannot
    move an entry to error_permanent."""
    root, cands, seen, fake = _runner(tmp_path, ["2026/08/a.mp3", "2026/08/b.mp3"])
    run_retry_batch(
        cands, root, bus=EventBus(), transcribe=fake,
        load_entry=lambda k: {"status": "done"},
    )
    assert seen, "nothing was attempted"
    for _, kwargs in seen:
        assert kwargs["max_retries"] is None


def test_paid_transcript_is_re_rendered_not_repurchased(tmp_path):
    """FR-6."""
    root, cands, seen, fake = _runner(tmp_path, ["2026/08/paid.mp3"])
    (root / "2026/08/paid.aai.json").write_text(json.dumps({"id": "x"}))
    cands = find_retry_candidates(_FakeState({"2026/08/paid.mp3": _entry()}), root)

    outcome = run_retry_batch(
        cands, root, bus=EventBus(), transcribe=fake,
        load_entry=lambda k: {"status": "done"},
    )
    assert seen[0][1]["reuse_existing_transcript"] is True
    assert outcome.re_rendered == 1 and outcome.succeeded == 0


def test_dead_balance_stops_the_batch(tmp_path):
    """FR-5: one failing upload is enough to know the rest will fail identically."""
    keys = [f"2026/08/f{i}.mp3" for i in range(5)]
    root, cands, seen, fake = _runner(tmp_path, keys)

    outcome = run_retry_batch(
        cands, root, bus=EventBus(), transcribe=fake,
        load_entry=lambda k: {"status": "error", "error": BALANCE_ERROR},
    )
    assert len(seen) == 1, "must not work through the rest"
    assert outcome.failed == 1
    assert outcome.not_attempted == 4
    assert "top up" in (outcome.aborted_reason or "").lower()
    assert "cdn.assemblyai.com" not in (outcome.aborted_reason or "")


def test_classification_reads_the_ledger_not_the_return_value(tmp_path):
    """transcribe_file discards PipelineResult.error and publishes only
    `pipeline status='error'`; classifying that would match nothing and the
    batch would run on."""
    keys = [f"2026/08/g{i}.mp3" for i in range(4)]
    root, cands, seen, fake = _runner(tmp_path, keys)

    outcome = run_retry_batch(
        cands, root, bus=EventBus(), transcribe=fake,
        load_entry=lambda k: {"status": "error", "error": BALANCE_ERROR},
    )
    assert outcome.aborted_reason and "top up" in outcome.aborted_reason.lower()
    assert len(seen) == 1


def test_consecutive_transient_failures_stop_the_batch(tmp_path):
    """FR-9 circuit breaker: the abort must not depend on matching vendor prose."""
    keys = [f"2026/08/h{i}.mp3" for i in range(6)]
    root, cands, seen, fake = _runner(tmp_path, keys)

    outcome = run_retry_batch(
        cands, root, bus=EventBus(), transcribe=fake,
        load_entry=lambda k: {"status": "error", "error": "totally novel failure"},
    )
    assert len(seen) == 2
    assert outcome.failed == 2
    assert outcome.not_attempted == 4
    assert "in a row" in (outcome.aborted_reason or "")


def test_success_resets_the_consecutive_counter(tmp_path):
    keys = [f"2026/08/i{i}.mp3" for i in range(4)]
    root, cands, seen, fake = _runner(tmp_path, keys)
    results = iter([
        {"status": "error", "error": "blip"},
        {"status": "done"},
        {"status": "error", "error": "blip"},
        {"status": "done"},
    ])
    outcome = run_retry_batch(
        cands, root, bus=EventBus(), transcribe=fake, load_entry=lambda k: next(results),
    )
    assert len(seen) == 4, "an isolated failure between successes must not abort"
    assert outcome.succeeded == 2 and outcome.failed == 2
    assert outcome.aborted_reason is None


def test_missing_file_is_skipped_not_attempted(tmp_path):
    root = _archive(tmp_path, "2026/08/here.mp3")
    state = _FakeState(
        {"2026/08/here.mp3": _entry(), "2026/08/gone.mp3": _entry()}
    )
    cands = find_retry_candidates(state, root)
    seen: list = []

    outcome = run_retry_batch(
        cands, root, bus=EventBus(),
        transcribe=faithful(
            transcribe_mod.transcribe_file, lambda p, a, **kw: seen.append(p.name)
        ),
        load_entry=lambda k: {"status": "done"},
    )
    assert seen == ["here.mp3"]
    assert outcome.not_attempted == 1


# --- confirm summary --------------------------------------------------------


def test_summary_splits_billable_from_free_and_counts_exclusions(tmp_path):
    root = _archive(
        tmp_path, "2026/08/a.mp3", "2026/08/paid.mp3", "2026/04/quiet.mp3"
    )
    (root / "2026/08/paid.aai.json").write_text(json.dumps({"id": "x"}))
    state = _FakeState(
        {
            "2026/08/a.mp3": _entry(error=BALANCE_ERROR),
            "2026/08/paid.mp3": _entry(error=BALANCE_ERROR),
            "2026/04/quiet.mp3": _entry(error=NO_SPEECH_ERROR),
            "2026/08/gone.mp3": _entry(error=BALANCE_ERROR),
        }
    )
    s = summarize(find_retry_candidates(state, root))

    assert s["total"] == 4
    assert s["selected"] == 2
    assert s["to_transcribe"] == 1
    assert s["re_render"] == 1, "a paid transcript costs nothing to re-render"
    assert s["excluded"] == {"retrying will not help": 1, "missing from the archive": 1}


def test_unreadable_duration_is_reported_as_unknown_not_zero(tmp_path):
    """A 32-byte stub has no readable header. The confirm must not render that
    as $0.00 — an operator would read it as free."""
    root = _archive(tmp_path, "2026/08/a.mp3")
    s = summarize(find_retry_candidates(_FakeState({"2026/08/a.mp3": _entry()}), root))
    assert s["to_transcribe"] == 1
    assert s["duration_known"] is False


# -- the seam's contract ---------------------------------------------------
#
# 02584c8 wrote `run_retry_batch` against `process_file`'s contract (which had
# just gained max_retries + reuse_existing_transcript at diarize 7149f63) but
# never widened `transcribe_file`, the bridge between them. Every double here
# was `**kw`-permissive, so nothing in 278 tests bound the real signature and
# the entire retry execution path shipped dead. Found by the Gate 4 PoL on the
# first batch that reached a selectable candidate.


def test_transcribe_file_accepts_every_kwarg_the_retry_batch_passes():
    """The production default of the `transcribe` seam must accept what the
    batch actually passes. Kwargs are read from the call site, not restated, so
    a future argument is covered without anyone updating this test."""
    src = Path(__file__).resolve().parent.parent / "src" / "hidock_direct"
    passed = call_kwargs_in(src / "retry.py", "transcribe")

    signature = inspect.signature(transcribe_mod.transcribe_file)
    rejected = [name for name in passed if name not in signature.parameters]
    assert not rejected, (
        f"run_retry_batch passes {rejected} but transcribe_file accepts "
        f"{sorted(signature.parameters)} — every batch dies on file 1"
    )

    # Binding is the real check: presence in `parameters` would still allow a
    # positional-only parameter that cannot be passed by keyword.
    signature.bind(Path("a.mp3"), Path("/archive"), **{name: None for name in passed})


# -- the marker vocabulary must match what AssemblyAI actually says ---------
#
# Captured from the live API 2026-08-20 by running the real client with a bogus
# key: AAI reports a bad key at the upload step, with neither "401" nor
# "Unauthorized" in the text, so classify_failure returned `transient` and the
# batch never named the operator-actionable cause. The marker AND the fixture
# that ratified it were both authored from one guess about what the API emits.

INVALID_KEY_ERROR = (
    "AssemblyAI request failed: Failed to upload audio file: Invalid API key"
)


def test_invalid_api_key_is_operator_actionable():
    """Captured response, not an authored proxy. Retrying with the same key
    fails identically forever, which is what the class means."""
    assert classify_failure(INVALID_KEY_ERROR) == CLASS_OPERATOR_ACTIONABLE


def test_transient_upload_failure_stays_transient():
    """Guards the over-correction: not every upload failure is a key problem.
    A dropped connection mid-upload is worth retrying."""
    assert classify_failure(
        "AssemblyAI request failed: Failed to upload audio file: "
        "[Errno 54] Connection reset by peer"
    ) == CLASS_TRANSIENT


def test_bad_key_batch_aborts_naming_the_key(tmp_path):
    """The abort must say what to fix. Falling through to the generic
    '2 failures in a row' message is the symptom this bug is about."""
    root, cands, seen, fake = _runner(tmp_path, ["2026/08/a.mp3", "2026/08/b.mp3"])
    outcome = run_retry_batch(
        cands, root, bus=EventBus(), transcribe=fake,
        load_entry=lambda k: {"status": "error", "error": INVALID_KEY_ERROR},
    )
    assert outcome.aborted_reason, "an invalid key must stop the batch"
    assert "failures in a row" not in outcome.aborted_reason, (
        "aborted on the consecutive-failure fallback rather than on the key"
    )
    assert "ASSEMBLYAI_API_KEY" in outcome.aborted_reason
    assert len(seen) == 1, "must stop on the first file, not work through the set"


def test_every_operator_actionable_marker_has_a_specific_remediation():
    """Agreement test. The marker vocabulary is implemented twice — once to
    classify (`_OPERATOR_ACTIONABLE_MARKERS`) and once to choose the message
    (`_remediation`'s branches). A marker with no branch classifies as
    actionable and then renders the raw error, which is the generic fallback
    wearing the actionable label."""
    from hidock_direct.retry import _OPERATOR_ACTIONABLE_MARKERS, _remediation

    for marker in _OPERATOR_ACTIONABLE_MARKERS:
        err = f"AssemblyAI request failed: {marker} something"
        assert classify_failure(err) == CLASS_OPERATOR_ACTIONABLE
        assert _remediation(err) != redact(err), (
            f"marker {marker!r} classifies as operator-actionable but "
            f"_remediation has no branch for it — it falls through to the "
            f"generic raw-error message"
        )


def test_operator_actionable_markers_record_whether_they_were_observed():
    """Provenance, per entry. The block claimed 'Verified against the live API'
    over all five markers; two were authored strings nobody had seen the API
    emit, and one of those could not be produced at all. An honest annotation
    is what makes that visible by reading."""
    src = (Path(__file__).resolve().parent.parent / "src" / "hidock_direct" / "retry.py").read_text()
    block = src.split("_OPERATOR_ACTIONABLE_MARKERS = (")[1].split(")")[0]
    entries = [ln for ln in block.splitlines() if ln.strip().startswith('"')]
    assert entries, "marker block not found"
    for line in entries:
        assert "captured" in line or "unobserved" in line, (
            f"marker line lacks a provenance annotation: {line.strip()!r}. "
            "Every entry must say whether the text was captured from the live "
            "API or authored without observation."
        )
