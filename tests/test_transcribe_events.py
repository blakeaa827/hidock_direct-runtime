"""Regression tests for `transcribe_file` event visibility.

Root cause of the shipping bug: when `diarize_audio` was unimportable
(UF_HIDDEN `.pth` files), `transcribe_file` silently returned `None`.
No event reached the TUI, so the operator had no idea transcription
was disabled. These tests pin each outcome to an observable event so
the silent-skip class of bug cannot return.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, List

import pytest

from hidock_direct import transcribe
from tests.conftest import faithful
from hidock_direct.events import (
    Event,
    EventBus,
    TranscribeComplete,
    TranscribeFailed,
    TranscribeSkipped,
    TranscribeStarted,
)


@dataclass
class _FakeResult:
    status: str
    drive_file_id: Any = None


@pytest.fixture
def bus_and_events() -> tuple[EventBus, List[Event]]:
    bus = EventBus()
    sink: List[Event] = []
    bus.subscribe(sink.append)
    return bus, sink


def test_unavailable_publishes_skipped_event(monkeypatch, bus_and_events, tmp_path):
    """When diarize_audio cannot be imported, operator must see a skip event.

    Previously: silent `return None`, no bus traffic. Today: TranscribeSkipped.
    """
    bus, sink = bus_and_events
    monkeypatch.setattr(transcribe, "_check_available", lambda: False)

    audio = tmp_path / "2026-04-17_090039.mp3"
    audio.write_bytes(b"\xff\xfb\x00\x00")  # MP3 sync word

    result = transcribe.transcribe_file(
        audio_path=audio,
        archive_dir=tmp_path,
        bus=bus,
        device_filename="2026Apr17-090039-Rec00.hda",
    )

    assert result is None
    skipped = [e for e in sink if isinstance(e, TranscribeSkipped)]
    assert len(skipped) == 1, f"expected one TranscribeSkipped, got bus={sink}"
    assert skipped[0].device_filename == "2026Apr17-090039-Rec00.hda"
    assert "diarize_audio" in skipped[0].reason.lower()
    # FR-D2: colleague-accurate remediation — diarize_audio is vendored now, so
    # the old iCloud/.pth remediation is misleading. Message must point at the
    # real fix (re-run bootstrap), not chflags/.pth.
    reason = skipped[0].reason
    assert ".pth" not in reason
    assert "iCloud" not in reason
    assert "chflags" not in reason
    assert "bootstrap" in reason
    # And importantly: no Started/Complete were falsely emitted.
    assert not [e for e in sink if isinstance(e, (TranscribeStarted, TranscribeComplete, TranscribeFailed))]


def test_success_publishes_started_then_complete(monkeypatch, bus_and_events, tmp_path):
    bus, sink = bus_and_events
    monkeypatch.setattr(transcribe, "_check_available", lambda: True)
    monkeypatch.setattr(
        transcribe,
        "_run_pipeline",
        faithful(
            transcribe._run_pipeline,
            lambda *a, **kw: _FakeResult(status="done", drive_file_id="DRIVE_XYZ"),
        ),
    )

    audio = tmp_path / "2026-04-17_090039.mp3"
    audio.write_bytes(b"\xff\xfb\x00\x00")

    result = transcribe.transcribe_file(
        audio_path=audio,
        archive_dir=tmp_path,
        bus=bus,
        device_filename="2026Apr17-090039-Rec00.hda",
    )

    assert result == "DRIVE_XYZ"
    started = [e for e in sink if isinstance(e, TranscribeStarted)]
    complete = [e for e in sink if isinstance(e, TranscribeComplete)]
    assert len(started) == 1 and started[0].device_filename == "2026Apr17-090039-Rec00.hda"
    assert len(complete) == 1
    assert complete[0].drive_file_id == "DRIVE_XYZ"
    assert complete[0].device_filename == "2026Apr17-090039-Rec00.hda"
    # Order: Started must precede Complete.
    assert sink.index(started[0]) < sink.index(complete[0])


def test_non_done_status_publishes_failed(monkeypatch, bus_and_events, tmp_path):
    """Pipeline returned status != 'done' — operator sees failure, not silence."""
    bus, sink = bus_and_events
    monkeypatch.setattr(transcribe, "_check_available", lambda: True)
    monkeypatch.setattr(
        transcribe,
        "_run_pipeline",
        faithful(transcribe._run_pipeline, lambda *a, **kw: _FakeResult(status="error")),
    )

    audio = tmp_path / "2026-04-17_090039.mp3"
    audio.write_bytes(b"\xff\xfb\x00\x00")

    result = transcribe.transcribe_file(
        audio_path=audio,
        archive_dir=tmp_path,
        bus=bus,
        device_filename="2026Apr17-090039-Rec00.hda",
    )

    assert result is None
    failed = [e for e in sink if isinstance(e, TranscribeFailed)]
    assert len(failed) == 1
    assert failed[0].device_filename == "2026Apr17-090039-Rec00.hda"
    assert "error" in failed[0].reason.lower()
    # No false Complete.
    assert not [e for e in sink if isinstance(e, TranscribeComplete)]


def test_pipeline_exception_publishes_failed(monkeypatch, bus_and_events, tmp_path):
    """Pipeline raised — bug must not propagate, but must be loud on the bus."""
    bus, sink = bus_and_events
    monkeypatch.setattr(transcribe, "_check_available", lambda: True)

    def _boom(*_a, **_kw):
        raise RuntimeError("upstream AAI quota exhausted")

    monkeypatch.setattr(
        transcribe, "_run_pipeline", faithful(transcribe._run_pipeline, _boom)
    )

    audio = tmp_path / "2026-04-17_090039.mp3"
    audio.write_bytes(b"\xff\xfb\x00\x00")

    result = transcribe.transcribe_file(
        audio_path=audio,
        archive_dir=tmp_path,
        bus=bus,
        device_filename="2026Apr17-090039-Rec00.hda",
    )

    assert result is None  # must not propagate
    failed = [e for e in sink if isinstance(e, TranscribeFailed)]
    assert len(failed) == 1
    # Reason must carry the real failure so the operator can act.
    assert "quota exhausted" in failed[0].reason
    assert failed[0].device_filename == "2026Apr17-090039-Rec00.hda"


# -- retry parameters must reach the pipeline -------------------------------
#
# `run_retry_batch` passes max_retries + reuse_existing_transcript; diarize's
# `process_file` accepts both (7149f63); `transcribe_file` sat between them and
# accepted neither, so every batch died at argument binding and the retry
# execution path shipped dead. Accepting the kwargs is not enough — they must
# arrive at `process_file`, because they are the no-escalation invariant
# (retry.py:12-15) and the no-double-billing guard (docs/SETUP.md) respectively.
#
# These assert at the `process_file` boundary rather than at `_run_pipeline`,
# because the sentinel translation ("caller didn't pass it" -> omit) happens
# below `_run_pipeline`. A spy one level up would pass against a version that
# forwards hidock's sentinel object straight through, which diarize compares by
# identity and would misread as a real value.


@pytest.fixture
def pipeline_spy(monkeypatch, tmp_path):
    """Capture what reaches diarize's `process_file`, with the stack stubbed.

    INBOX_DIRS is pinned to tmp_path so diarize's state_dir (`inbox_dirs[0]/
    .state`) resolves inside the test — nothing touches the real archive.
    """
    import diarize_audio.assemblyai_client as aai_mod
    import diarize_audio.pipeline as pipeline_mod

    seen: dict = {}

    def _spy(**kwargs):
        seen.update(kwargs)
        seen["_called"] = True
        return _FakeResult(status="done")

    monkeypatch.setattr(aai_mod, "AAIClient", lambda *a, **k: None)
    monkeypatch.setattr(pipeline_mod, "process_file", _spy)
    monkeypatch.setattr(transcribe, "_check_available", lambda: True)
    monkeypatch.setenv("ASSEMBLYAI_API_KEY", "test-key")
    monkeypatch.setenv("INBOX_DIRS", str(tmp_path))
    monkeypatch.setenv("DRIVE_ENABLED", "false")
    return seen


def _audio(tmp_path: Path) -> Path:
    p = tmp_path / "2026-08-20_083259.mp3"
    p.write_bytes(b"\xff\xfb\x00\x00")
    return p


def test_operator_retry_forwards_max_retries_none_to_the_pipeline(
    pipeline_spy, bus_and_events, tmp_path
):
    """None means "never escalate to error_permanent" and is distinct from
    "not supplied" (which means cfg.max_retries). A retry that dropped it would
    bury the files the surface exists to recover after three presses."""
    bus, _ = bus_and_events

    transcribe.transcribe_file(
        _audio(tmp_path), tmp_path, bus=bus, device_filename="a.hda", max_retries=None
    )

    assert pipeline_spy.get("_called"), "process_file was never reached"
    assert "max_retries" in pipeline_spy, "max_retries did not reach process_file"
    assert pipeline_spy["max_retries"] is None


def test_paid_transcript_forwards_reuse_existing_transcript(
    pipeline_spy, bus_and_events, tmp_path
):
    """The no-double-billing guard. Dropping this re-purchases audio whose
    transcript was already paid for — the claim docs/SETUP.md makes to users."""
    bus, _ = bus_and_events

    transcribe.transcribe_file(
        _audio(tmp_path),
        tmp_path,
        bus=bus,
        device_filename="a.hda",
        reuse_existing_transcript=True,
    )

    assert pipeline_spy.get("_called"), "process_file was never reached"
    assert pipeline_spy.get("reuse_existing_transcript") is True


def test_offload_path_leaves_the_retry_budget_to_the_config(
    pipeline_spy, bus_and_events, tmp_path
):
    """The normal offload must NOT pin max_retries. Forwarding a bare None — or
    hidock's own sentinel, which diarize compares by identity — silently stops
    the unattended path from ever escalating to error_permanent."""
    from diarize_audio.pipeline import _UNSET

    bus, _ = bus_and_events

    transcribe.transcribe_file(
        _audio(tmp_path), tmp_path, bus=bus, device_filename="a.hda"
    )

    assert pipeline_spy.get("_called"), "process_file was never reached"
    assert pipeline_spy.get("max_retries", _UNSET) is _UNSET, (
        f"the offload path must inherit cfg.max_retries, got "
        f"{pipeline_spy.get('max_retries')!r}"
    )
