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
        lambda audio_path, archive_dir: _FakeResult(status="done", drive_file_id="DRIVE_XYZ"),
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
        lambda audio_path, archive_dir: _FakeResult(status="error"),
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

    def _boom(audio_path, archive_dir):
        raise RuntimeError("upstream AAI quota exhausted")

    monkeypatch.setattr(transcribe, "_run_pipeline", _boom)

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
