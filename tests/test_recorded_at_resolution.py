"""Regression tests: transcripts must carry the RECORDING START time.

Root cause of the shipped bug: the recording-start time is reported by the
device (`DeviceFile.device_mtime`, parsed by the vendored Jensen layer from the
device filename) and used to mint the archive basename — then dropped at the
transcription hand-off. `diarize_audio.process_file` had no way to receive it,
so it re-derived the time from the archived file's mtime. Because offload
streams device bytes into a fresh file and never restamps it, that mtime is
recording-END + transfer: wrong by the full duration of the recording, and by
days when the device sits unplugged before offload.

Every transcript the pipeline had ever produced (252/252 in the operator's
archive) carried a wrong `recorded_at`; worst observed drift was 7.43 days.

See `bug_report_recorded_at_uses_offload_mtime.md`.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from hidock_direct import transcribe
from hidock_direct.events import EventBus, TranscribeComplete
from hidock_direct.offload import (
    Offloader,
    _archive_basename,
    resolve_recorded_at,
)
from hidock_direct.state import DeviceKey

from tests.fixtures.mock_device import MockDevice, MockFile, make_wav_bytes


DEVICE_KEY = DeviceKey(model="hidock-h1", serial="SN-TEST-1234")

# The production case from the bug report: recording starts 08:32:04, runs
# 22m00s, and the offload finishes writing the archive file at 08:54:45.
RECORDING_START = datetime(2026, 8, 5, 8, 32, 4)
OFFLOAD_WRITE = datetime(2026, 8, 5, 8, 54, 45)

FAKE_TRANSCRIPT = {
    "id": "test-transcript-id",
    "audio_duration": 1320,
    "language_code": "en_us",
    "utterances": [{"speaker": "A", "start": 0, "text": "Regression utterance."}],
    "auto_highlights_result": None,
}


class _FakeAAIClient:
    """Stands in for the network client so the REAL render path still runs.

    Asserting on the rendered `.md` rather than on a mock call is deliberate:
    a plumbing refactor that stops threading the value through cannot pass a
    test that reads the actual document.
    """

    def __init__(self, *_args, **_kwargs):
        pass

    def transcribe_file(self, _path):
        return FAKE_TRANSCRIPT


@pytest.fixture
def transcribing_env(monkeypatch, archive_dir: Path):
    """Env + client stub so `_run_pipeline` reaches the real renderer."""
    import diarize_audio.assemblyai_client as aai_mod

    monkeypatch.setattr(aai_mod, "AAIClient", _FakeAAIClient)
    monkeypatch.setenv("ASSEMBLYAI_API_KEY", "test-key")
    monkeypatch.setenv("INBOX_DIRS", str(archive_dir))
    monkeypatch.setenv("DRIVE_ENABLED", "false")
    return archive_dir


def _touch(path: Path, when: datetime) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\xff\xfb" + b"\x00" * 64)
    os.utime(path, (when.timestamp(), when.timestamp()))
    return path


# --- resolver ---------------------------------------------------------------


def test_resolve_recorded_at_prefers_device_report(tmp_path):
    """Leg (a): the device told us when the recording started — use it."""
    archived = _touch(tmp_path / "2026-08-05_083204.mp3", OFFLOAD_WRITE)

    assert resolve_recorded_at(archived, RECORDING_START).replace(tzinfo=None) == RECORDING_START


def test_resolve_recorded_at_falls_back_to_archive_basename(tmp_path):
    """Leg (b): no device attached (re-run / recovery) — the basename holds it.

    This is the path the 2026-07-08 stranded-transcription recovery took:
    `process_file` invoked directly against an already-archived file.
    """
    archived = _touch(tmp_path / "2026-08-05_083204.mp3", OFFLOAD_WRITE)

    assert resolve_recorded_at(archived).replace(tzinfo=None) == RECORDING_START


def test_resolve_recorded_at_falls_back_to_mtime_when_basename_unparseable(tmp_path):
    """Leg (c): a file this tool didn't name — mtime is all we have."""
    archived = _touch(tmp_path / "vendor-export-notes.mp3", OFFLOAD_WRITE)

    assert resolve_recorded_at(archived).replace(tzinfo=None) == OFFLOAD_WRITE


def test_resolve_recorded_at_returns_timezone_aware(tmp_path):
    """The device reports naive local wall-clock; the renderer emits isoformat().

    A naive value silently drops the UTC offset from the frontmatter, changing
    its shape for downstream parsers. Every leg must return aware.
    """
    named = _touch(tmp_path / "2026-08-05_083204.mp3", OFFLOAD_WRITE)
    unnamed = _touch(tmp_path / "vendor-export.mp3", OFFLOAD_WRITE)

    assert resolve_recorded_at(named, RECORDING_START).tzinfo is not None
    assert resolve_recorded_at(named).tzinfo is not None
    assert resolve_recorded_at(unnamed).tzinfo is not None


@pytest.mark.parametrize(
    "device_time",
    [
        datetime(2026, 8, 5, 8, 32, 4),
        datetime(2026, 12, 31, 23, 59, 59),
        datetime(2026, 1, 1, 0, 0, 0),
    ],
)
def test_archive_basename_round_trips_through_resolver(tmp_path, device_time):
    """Mint and parse must not drift apart.

    `_archive_basename` writes the recording time into the filename; the
    resolver reads it back. They live in the same module so this invariant is
    checkable — lock it, because the basename is the only durable on-disk
    record of the recording start once the device is gone.
    """
    basename = _archive_basename(device_time, None, ext=".mp3")
    archived = _touch(tmp_path / basename, OFFLOAD_WRITE)

    assert resolve_recorded_at(archived).replace(tzinfo=None) == device_time


# --- end-to-end through the real renderer -----------------------------------


def _offload_one(archive_dir, mock_device, state_store, bus, device_mtime, *, name="REC_0001.wav"):
    mock_device.add_file(
        MockFile(name=name, content=make_wav_bytes(duration_seconds=1.0), device_mtime=device_mtime)
    )
    mock_device.connect()
    offloader = Offloader(
        adapter=mock_device,
        store=state_store,
        bus=bus,
        archive_dir=archive_dir,
        tmp_dir=archive_dir / ".tmp",
        delete_after_offload=False,
        transcribe_on_offload=True,
        sleep=lambda *_a, **_k: None,
    )
    files = offloader.scan_new_files(DEVICE_KEY)
    return offloader.offload(device_key=DEVICE_KEY, file=files[0])


def test_offload_renders_recording_start_not_offload_time(
    transcribing_env, archive_dir: Path, mock_device: MockDevice, state_store, event_sink
):
    """The seam that dropped the value: offload -> transcribe -> render."""
    bus, events = event_sink

    result = _offload_one(archive_dir, mock_device, state_store, bus, RECORDING_START)

    assert any(isinstance(e, TranscribeComplete) for e in events), (
        "transcription did not complete; the rendered assertions below would be vacuous"
    )
    md = (result.archive_path.parent / f"{result.archive_path.stem}.md").read_text()
    assert "recorded_at: 2026-08-05T08:32:04" in md
    assert "# 2026-08-05 08:32" in md
    # The archive file was written just now, so its mtime is ~today. Pin that
    # the rendered date is the RECORDING's, not the write's.
    written_at = datetime.fromtimestamp(result.archive_path.stat().st_mtime)
    assert f"# {written_at:%Y-%m-%d %H:%M}" not in md


def test_recorded_at_survives_midnight_boundary(
    transcribing_env, archive_dir: Path, mock_device: MockDevice, state_store, event_sink
):
    """Highest-consequence failure mode: the wrong CALENDAR DATE.

    A recording starting 23:47 that finishes offloading after midnight was
    filed under the following day. Two of the operator's 2026-07-31 recordings
    were rendered as 2026-08-05 for the same reason at a five-day scale.
    """
    bus, events = event_sink
    late_night = datetime(2026, 8, 4, 23, 47, 12)

    result = _offload_one(archive_dir, mock_device, state_store, bus, late_night)

    assert any(isinstance(e, TranscribeComplete) for e in events)
    md = (result.archive_path.parent / f"{result.archive_path.stem}.md").read_text()
    assert "recorded_at: 2026-08-04T23:47:12" in md
    assert "# 2026-08-04 23:47" in md
    assert "2026-08-05" not in md.split("---")[1], "frontmatter rolled to the next day"


def test_transcribe_file_resolves_from_basename_without_device(
    transcribing_env, archive_dir: Path
):
    """Recovery path: re-transcribing an archived file with no device attached.

    `transcribe_file` must resolve the recording start itself rather than
    inheriting the file's mtime, or every re-run re-corrupts the timestamp.
    """
    bus = EventBus()
    sink = []
    bus.subscribe(sink.append)
    archived = _touch(
        archive_dir / "2026" / "08" / "2026-08-05_083204.mp3",
        OFFLOAD_WRITE + timedelta(days=3),
    )

    transcribe.transcribe_file(
        archived, archive_dir, bus=bus, device_filename="2026Aug05-083204-Rec01.hda"
    )

    assert any(isinstance(e, TranscribeComplete) for e in sink)
    md = (archived.parent / f"{archived.stem}.md").read_text()
    assert "recorded_at: 2026-08-05T08:32:04" in md
    assert "# 2026-08-05 08:32" in md


# --- input-grammar surface forms (Gate 1 step 7) -----------------------------
#
# `_archive_basename` + `_unique_archive_path` between them mint exactly two
# stem shapes and two extensions. The resolver must read back every form this
# tool writes — a form the minting side produces but the parsing side rejects
# silently degrades to the mtime fallback, which is the bug.


@pytest.mark.parametrize(
    "basename",
    [
        "2026-08-05_083204.wav",     # H1: HTA converted to WAV
        "2026-08-05_083204.mp3",     # P1: MP3 stored with .hda extension
        "2026-08-05_083204-1.mp3",   # second-precision collision, first dedupe
        "2026-08-05_083204-2.wav",   # ...and the next
        "2026-08-05_083204-11.mp3",  # multi-digit suffix
    ],
)
def test_resolver_reads_back_every_minted_basename_form(tmp_path, basename):
    archived = _touch(tmp_path / basename, OFFLOAD_WRITE)

    assert resolve_recorded_at(archived).replace(tzinfo=None) == RECORDING_START


@pytest.mark.parametrize(
    "basename",
    [
        "Some Vendor Export.mp3",
        "2026-08-05.mp3",              # date only, no time
        "20260805_083204.mp3",         # no dashes — a shape we never mint
        "2026-08-05_083204-notes.mp3", # suffix that isn't a collision index
    ],
)
def test_resolver_rejects_names_this_tool_never_minted(tmp_path, basename):
    """Falling through to mtime is correct here — we have no better signal."""
    archived = _touch(tmp_path / basename, OFFLOAD_WRITE)

    assert resolve_recorded_at(archived).replace(tzinfo=None) == OFFLOAD_WRITE


def test_collision_suffixed_files_round_trip_from_the_minting_side(tmp_path):
    """Prove the collision form is actually minted, not just hypothesised."""
    from hidock_direct.offload import _unique_archive_path

    basename = _archive_basename(RECORDING_START, None, ext=".mp3")
    first = _unique_archive_path(tmp_path, basename, RECORDING_START)
    first.parent.mkdir(parents=True, exist_ok=True)
    first.write_bytes(b"\xff\xfb")
    second = _unique_archive_path(tmp_path, basename, RECORDING_START)
    second.write_bytes(b"\xff\xfb")

    assert second.name == "2026-08-05_083204-1.mp3", "minting shape changed"
    for path in (first, second):
        assert resolve_recorded_at(path).replace(tzinfo=None) == RECORDING_START


# --- whispers routing variant ------------------------------------------------


def test_whisper_offload_renders_recording_start(
    transcribing_env, archive_dir: Path, mock_device: MockDevice, state_store, event_sink
):
    """Whispers land under `<archive>/whispers/`, not the archive root.

    A distinct production-mainline variant: the archive-relative path starts
    with `whispers/` rather than `YYYY/`, so anything reading the year and month
    off the path (`_year_month_from`) falls through to `recorded_at` instead.
    """
    from hidock_direct.classify import RecordingKind

    bus, events = event_sink
    mock_device.add_file(
        MockFile(
            name="REC_W01.wav",
            content=make_wav_bytes(duration_seconds=1.0),
            device_mtime=RECORDING_START,
        )
    )
    mock_device.connect()
    offloader = Offloader(
        adapter=mock_device,
        store=state_store,
        bus=bus,
        archive_dir=archive_dir,
        tmp_dir=archive_dir / ".tmp",
        delete_after_offload=False,
        transcribe_on_offload=True,
        sleep=lambda *_a, **_k: None,
    )
    files = offloader.scan_new_files(DEVICE_KEY)
    result = offloader.offload_one(
        device_key=DEVICE_KEY, file=files[0], kind=RecordingKind.WHISPER
    )

    assert "whispers" in result.archive_path.parts
    assert any(isinstance(e, TranscribeComplete) for e in events)
    md = (result.archive_path.parent / f"{result.archive_path.stem}.md").read_text()
    assert "recorded_at: 2026-08-05T08:32:04" in md
    assert "# 2026-08-05 08:32" in md
