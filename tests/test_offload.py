"""PRD §5.1 offload tests. The Jensen layer is replaced by MockDevice."""

from __future__ import annotations

import threading
import wave
from datetime import datetime
from pathlib import Path

import pytest

from hidock_direct.device import TransferAborted
from hidock_direct.events import (
    DownloadComplete,
    DownloadProgress,
    DownloadStarted,
    ScanComplete,
    ScanStarted,
)
from hidock_direct.state import DeviceKey

from tests.fixtures.mock_device import MockDevice, MockFile, make_wav_bytes


DEVICE_KEY = DeviceKey(model="hidock-h1", serial="SN-TEST-1234")


def test_offload_happy_path(offloader, mock_device: MockDevice, archive_dir: Path, event_sink):
    bus, events = event_sink
    mock_device.add_file(MockFile(name="REC_0001.wav", content=make_wav_bytes(duration_seconds=1.0), device_mtime=datetime(2026, 4, 12, 14, 33, 0)))
    mock_device.add_file(MockFile(name="REC_0002.wav", content=make_wav_bytes(duration_seconds=2.0), device_mtime=datetime(2026, 4, 12, 15, 20, 11)))
    mock_device.add_file(MockFile(name="REC_0003.wav", content=make_wav_bytes(duration_seconds=0.5), device_mtime=datetime(2026, 4, 13, 9, 15, 0)))

    new_files = offloader.scan_new_files(DEVICE_KEY)
    assert [f.name for f in new_files] == ["REC_0001.wav", "REC_0002.wav", "REC_0003.wav"]

    results = [offloader.offload(device_key=DEVICE_KEY, file=f) for f in new_files]
    for r in results:
        assert r.archive_path.exists()
    assert (archive_dir / "2026" / "04" / "2026-04-12_143300.wav").exists()
    assert (archive_dir / "2026" / "04" / "2026-04-12_152011.wav").exists()
    assert (archive_dir / "2026" / "04" / "2026-04-13_091500.wav").exists()

    started = [e for e in events if isinstance(e, DownloadStarted)]
    completed = [e for e in events if isinstance(e, DownloadComplete)]
    assert len(started) == len(completed) == 3


def test_offload_already_processed(offloader, mock_device: MockDevice, state_store):
    f = MockFile(name="REC_X.wav", content=make_wav_bytes(duration_seconds=1.0))
    mock_device.add_file(f)
    new_files = offloader.scan_new_files(DEVICE_KEY)
    assert new_files, "sanity — first pass should offer it"
    offloader.offload(device_key=DEVICE_KEY, file=new_files[0])
    assert state_store.is_processed(DEVICE_KEY, "REC_X.wav")
    again = offloader.scan_new_files(DEVICE_KEY)
    assert again == [], "already-processed file should be skipped on re-scan"


def test_offload_atomic_kill_mid_transfer(offloader, mock_device: MockDevice, archive_dir: Path, state_store):
    content = make_wav_bytes(duration_seconds=2.0)
    # Small chunk size so the abort threshold actually trips mid-stream
    # (default 64K chunks can swallow the whole 2-second WAV in one pass).
    mock_device._chunk_size = 4096  # type: ignore[attr-defined]
    mock_device.add_file(MockFile(name="REC_KILL.wav", content=content))
    mock_device.raise_mid_transfer_after_bytes = len(content) // 3
    new_files = offloader.scan_new_files(DEVICE_KEY)
    assert new_files
    with pytest.raises(TransferAborted):
        offloader.offload(device_key=DEVICE_KEY, file=new_files[0])
    # No .wav in archive.
    wavs = list(archive_dir.rglob("*.wav"))
    assert wavs == []
    # State untouched.
    assert not state_store.is_processed(DEVICE_KEY, "REC_KILL.wav")
    # Staging dir clean.
    partials = list((archive_dir / ".tmp").glob("*.partial"))
    assert partials == []


class FakeHTAConverter:
    """Stand-in for jensen.HTAConverter that emits a canned WAV."""

    def __init__(self, wav_bytes: bytes):
        self._wav = wav_bytes

    def convert_hta_to_wav(self, hta_path: str, output_path=None) -> str:
        out = str(Path(hta_path).with_suffix(".wav"))
        Path(out).write_bytes(self._wav)
        return out


def test_offload_hda_conversion(archive_dir: Path, mock_device: MockDevice, state_store, event_sink):
    from hidock_direct.offload import Offloader

    bus, _ = event_sink
    wav_bytes = make_wav_bytes(duration_seconds=1.0)
    mock_device.connect()
    # Upload an .hda source whose content is NOT a valid wav; the fake converter will emit a real wav.
    mock_device.add_file(MockFile(name="REC_X.hda", content=b"\xff" * 4096, device_mtime=datetime(2026, 4, 12, 11, 0, 0)))
    offloader = Offloader(
        adapter=mock_device,
        store=state_store,
        bus=bus,
        archive_dir=archive_dir,
        tmp_dir=archive_dir / ".tmp",
        delete_after_offload=False,
        hta_converter=FakeHTAConverter(wav_bytes),
        sleep=lambda *_a, **_k: None,
    )
    new_files = offloader.scan_new_files(DEVICE_KEY)
    assert [f.name for f in new_files] == ["REC_X.hda"]
    result = offloader.offload(device_key=DEVICE_KEY, file=new_files[0])
    assert result.archive_path.exists()
    assert result.archive_path.suffix == ".wav"
    # archive contains the CONVERTED wav, not the .hda bytes.
    with wave.open(str(result.archive_path), "rb") as w:
        assert w.getnchannels() == 1
        assert w.getframerate() == 16000
    assert result.converted_from_hda is True


def test_filename_from_device_metadata(offloader, mock_device: MockDevice, archive_dir: Path):
    mock_device.add_file(MockFile(name="REC_META.wav", content=make_wav_bytes(), device_mtime=datetime(2026, 4, 12, 14, 33, 0)))
    files = offloader.scan_new_files(DEVICE_KEY)
    result = offloader.offload(device_key=DEVICE_KEY, file=files[0])
    assert result.archive_path == archive_dir / "2026" / "04" / "2026-04-12_143300.wav"


def test_filename_fallback_to_mtime(mock_device: MockDevice, archive_dir: Path, state_store, event_sink, monkeypatch):
    from hidock_direct.offload import Offloader

    bus, _ = event_sink
    mock_device.connect()
    mock_device.add_file(MockFile(name="REC_NOMETA.wav", content=make_wav_bytes(), device_mtime=None))
    fixed = datetime(2030, 1, 2, 3, 4, 5)
    offloader = Offloader(
        adapter=mock_device,
        store=state_store,
        bus=bus,
        archive_dir=archive_dir,
        tmp_dir=archive_dir / ".tmp",
        delete_after_offload=False,
        sleep=lambda *_a, **_k: None,
        clock=lambda: fixed,
    )
    files = offloader.scan_new_files(DEVICE_KEY)
    result = offloader.offload(device_key=DEVICE_KEY, file=files[0])
    assert result.archive_path == archive_dir / "2030" / "01" / "2030-01-02_030405.wav"


def test_chunked_progress_callbacks(offloader, mock_device: MockDevice, event_sink):
    bus, events = event_sink
    # ~192 KB at 16kHz/16-bit/1ch × 6s; chunk size 65536 → 3 chunks.
    mock_device.add_file(MockFile(name="REC_PROG.wav", content=make_wav_bytes(duration_seconds=6.0)))
    files = offloader.scan_new_files(DEVICE_KEY)
    offloader.offload(device_key=DEVICE_KEY, file=files[0])
    progress = [e for e in events if isinstance(e, DownloadProgress)]
    assert len(progress) >= 3
    # Last progress event hits the total.
    last = progress[-1]
    assert last.bytes_done == last.bytes_total


def test_skips_oversized_file(offloader, mock_device: MockDevice):
    from hidock_direct.offload import MAX_DEVICE_FILE_SIZE

    # Device claims the file is bigger than the ceiling — scan drops it.
    # `size_override` simulates a pathological device report without
    # actually allocating 2+ GB of memory.
    too_big = MockFile(
        name="REC_HUGE.wav",
        content=b"\x00",
        size_override=MAX_DEVICE_FILE_SIZE + 1,
    )
    mock_device.add_file(too_big)
    files = offloader.scan_new_files(DEVICE_KEY)
    assert files == []


def test_scan_publishes_scan_events(offloader, mock_device: MockDevice, event_sink):
    bus, events = event_sink
    mock_device.add_file(MockFile(name="REC_S.wav", content=make_wav_bytes()))
    offloader.scan_new_files(DEVICE_KEY)
    assert any(isinstance(e, ScanStarted) for e in events)
    assert any(isinstance(e, ScanComplete) and e.new_file_count == 1 for e in events)


def test_offload_delete_after_offload(archive_dir: Path, mock_device: MockDevice, state_store, event_sink):
    from hidock_direct.offload import Offloader

    bus, _ = event_sink
    mock_device.connect()
    mock_device.add_file(
        MockFile(
            name="REC_DEL.wav",
            content=make_wav_bytes(duration_seconds=1.0),
            device_mtime=datetime(2026, 4, 12, 9, 0, 0),
        )
    )
    off = Offloader(
        adapter=mock_device,
        store=state_store,
        bus=bus,
        archive_dir=archive_dir,
        tmp_dir=archive_dir / ".tmp",
        delete_after_offload=True,
        sleep=lambda *_a, **_k: None,
    )
    files = off.scan_new_files(DEVICE_KEY)
    off.offload(device_key=DEVICE_KEY, file=files[0])
    # Archive written AND file removed from device.
    assert (archive_dir / "2026" / "04" / "2026-04-12_090000.wav").exists()
    snap = state_store.snapshot()
    entry = snap["devices"][DEVICE_KEY.namespaced]["processed_recordings"]["REC_DEL.wav"]
    assert entry["device_deleted"] is True


def test_offload_hda_conversion_failure(archive_dir: Path, mock_device: MockDevice, state_store, event_sink):
    from hidock_direct.offload import OffloadError, Offloader

    class FailingHTA:
        def convert_hta_to_wav(self, *_a, **_k):
            return None  # converter signals failure by returning None

    bus, _ = event_sink
    mock_device.connect()
    mock_device.add_file(MockFile(name="REC_BAD.hda", content=b"\xff" * 4096))
    off = Offloader(
        adapter=mock_device,
        store=state_store,
        bus=bus,
        archive_dir=archive_dir,
        tmp_dir=archive_dir / ".tmp",
        delete_after_offload=False,
        hta_converter=FailingHTA(),
        sleep=lambda *_a, **_k: None,
    )
    files = off.scan_new_files(DEVICE_KEY)
    with pytest.raises(OffloadError):
        off.offload(device_key=DEVICE_KEY, file=files[0])
    assert not any(archive_dir.rglob("*.wav"))
    assert not state_store.is_processed(DEVICE_KEY, "REC_BAD.hda")


def test_scan_pending_files_returns_typed_bundle(offloader, mock_device: MockDevice, event_sink):
    """PRD §5.3: 1 meeting, 2 whispers, 1 unknown — bucketed correctly."""
    bus, events = event_sink
    mock_device.add_file(MockFile(name="20260414-161650-Rec31.hda", content=make_wav_bytes(duration_seconds=0.5), device_mtime=datetime(2026, 4, 14, 16, 16, 50)))
    mock_device.add_file(MockFile(name="20260414-161700-Wip1.hda", content=make_wav_bytes(duration_seconds=0.2), device_mtime=datetime(2026, 4, 14, 16, 17, 0)))
    mock_device.add_file(MockFile(name="20260414-161800-Wip2.hda", content=make_wav_bytes(duration_seconds=0.2), device_mtime=datetime(2026, 4, 14, 16, 18, 0)))
    mock_device.add_file(MockFile(name="20260414-161900-Foo7.hda", content=make_wav_bytes(duration_seconds=0.2), device_mtime=datetime(2026, 4, 14, 16, 19, 0)))

    result = offloader.scan_pending_files(DEVICE_KEY)
    assert [f.name for f in result.meetings] == ["20260414-161650-Rec31.hda"]
    assert sorted(f.name for f in result.whispers) == ["20260414-161700-Wip1.hda", "20260414-161800-Wip2.hda"]
    assert [f.name for f in result.unknowns] == ["20260414-161900-Foo7.hda"]


def test_scan_publishes_whispers_and_unknowns_events(offloader, mock_device: MockDevice, event_sink):
    """PRD §5.3: WhispersDetected + UnknownsDetected published on scan."""
    from hidock_direct.events import ScanComplete, UnknownsDetected, WhispersDetected

    bus, events = event_sink
    mock_device.add_file(MockFile(name="20260414-161650-Rec31.hda", content=make_wav_bytes(), device_mtime=datetime(2026, 4, 14, 16, 16, 50)))
    mock_device.add_file(MockFile(name="20260414-161700-Wip1.hda", content=make_wav_bytes(), device_mtime=datetime(2026, 4, 14, 16, 17, 0)))
    mock_device.add_file(MockFile(name="20260414-161700-Wip2.hda", content=make_wav_bytes(), device_mtime=datetime(2026, 4, 14, 16, 17, 0)))
    mock_device.add_file(MockFile(name="weirdfile.hda", content=make_wav_bytes(), device_mtime=datetime(2026, 4, 14, 16, 18, 0)))

    offloader.scan_pending_files(DEVICE_KEY)
    whispers = [e for e in events if isinstance(e, WhispersDetected)]
    unknowns = [e for e in events if isinstance(e, UnknownsDetected)]
    scans = [e for e in events if isinstance(e, ScanComplete)]
    assert len(whispers) == 1 and whispers[0].count == 2
    assert len(unknowns) == 1 and unknowns[0].count == 1
    assert unknowns[0].filenames == ["weirdfile.hda"]
    # ScanComplete.new_file_count == meeting count (PRD §2.5).
    assert scans[-1].new_file_count == 1


def test_scan_no_whispers_events_when_only_meetings(offloader, mock_device: MockDevice, event_sink):
    from hidock_direct.events import UnknownsDetected, WhispersDetected

    bus, events = event_sink
    mock_device.add_file(MockFile(name="20260414-161650-Rec31.hda", content=make_wav_bytes(), device_mtime=datetime(2026, 4, 14, 16, 16, 50)))
    offloader.scan_pending_files(DEVICE_KEY)
    assert not any(isinstance(e, WhispersDetected) for e in events)
    assert not any(isinstance(e, UnknownsDetected) for e in events)


def test_offload_one_whisper_routes_to_whispers_dir(offloader, mock_device: MockDevice, archive_dir: Path):
    from hidock_direct.classify import RecordingKind

    mock_device.add_file(MockFile(name="20250827-012419-Wip67.hda", content=b"\xff\xfb" + b"\x00" * 2048, device_mtime=datetime(2025, 8, 27, 1, 24, 19)))
    # Use scan_pending_files so the whisper shows up even without size-stable.
    result = offloader.scan_pending_files(DEVICE_KEY)
    assert len(result.whispers) == 1
    out = offloader.offload_one(device_key=DEVICE_KEY, file=result.whispers[0], kind=RecordingKind.WHISPER)
    assert "whispers" in out.archive_path.parts
    rel = out.archive_path.relative_to(archive_dir).as_posix()
    assert rel.startswith("whispers/")
    assert out.archive_path.exists()


def test_offload_one_meeting_routes_to_meetings_root(offloader, mock_device: MockDevice, archive_dir: Path):
    from hidock_direct.classify import RecordingKind

    mock_device.add_file(MockFile(name="20260414-161650-Rec31.hda", content=b"\xff\xfb" + b"\x00" * 2048, device_mtime=datetime(2026, 4, 14, 16, 16, 50)))
    result = offloader.scan_pending_files(DEVICE_KEY)
    out = offloader.offload_one(device_key=DEVICE_KEY, file=result.meetings[0], kind=RecordingKind.MEETING)
    rel = out.archive_path.relative_to(archive_dir).as_posix()
    assert not rel.startswith("whispers/")


def test_offload_one_records_kind_in_ledger(offloader, mock_device: MockDevice, state_store):
    from hidock_direct.classify import RecordingKind

    mock_device.add_file(MockFile(name="20260414-161650-Rec31.hda", content=b"\xff\xfb" + b"\x00" * 2048, device_mtime=datetime(2026, 4, 14, 16, 16, 50)))
    mock_device.add_file(MockFile(name="20250827-012419-Wip67.hda", content=b"\xff\xfb" + b"\x00" * 2048, device_mtime=datetime(2025, 8, 27, 1, 24, 19)))
    result = offloader.scan_pending_files(DEVICE_KEY)
    offloader.offload_one(device_key=DEVICE_KEY, file=result.meetings[0], kind=RecordingKind.MEETING)
    offloader.offload_one(device_key=DEVICE_KEY, file=result.whispers[0], kind=RecordingKind.WHISPER)
    assert state_store.kind_for(DEVICE_KEY, "20260414-161650-Rec31.hda") is RecordingKind.MEETING
    assert state_store.kind_for(DEVICE_KEY, "20250827-012419-Wip67.hda") is RecordingKind.WHISPER


def test_delete_after_offload_filters_to_meetings(archive_dir: Path, mock_device: MockDevice, state_store, event_sink):
    """PRD §2.3: delete-after-offload only applies to MEETING files."""
    from hidock_direct.classify import RecordingKind
    from hidock_direct.offload import Offloader

    bus, _ = event_sink
    mock_device.connect()
    mock_device.add_file(MockFile(name="20260414-161650-Rec31.hda", content=b"\xff\xfb" + b"\x00" * 2048, device_mtime=datetime(2026, 4, 14, 16, 16, 50)))
    mock_device.add_file(MockFile(name="20260414-161700-Wip1.hda", content=b"\xff\xfb" + b"\x00" * 2048, device_mtime=datetime(2026, 4, 14, 16, 17, 0)))
    off = Offloader(
        adapter=mock_device,
        store=state_store,
        bus=bus,
        archive_dir=archive_dir,
        tmp_dir=archive_dir / ".tmp",
        delete_after_offload=True,
        sleep=lambda *_a, **_k: None,
    )
    result = off.scan_pending_files(DEVICE_KEY)
    off.offload_one(device_key=DEVICE_KEY, file=result.meetings[0], kind=RecordingKind.MEETING)
    off.offload_one(device_key=DEVICE_KEY, file=result.whispers[0], kind=RecordingKind.WHISPER)
    remaining = {f.name for f in mock_device.list_raw()}
    # Meeting is device-deleted; whisper stays on device.
    assert "20260414-161650-Rec31.hda" not in remaining
    assert "20260414-161700-Wip1.hda" in remaining
    snap = state_store.snapshot()
    recs = snap["devices"][DEVICE_KEY.namespaced]["processed_recordings"]
    assert recs["20260414-161650-Rec31.hda"]["device_deleted"] is True
    assert recs["20260414-161700-Wip1.hda"]["device_deleted"] is False


def test_skipped_whisper_no_ledger_entry(offloader, mock_device: MockDevice, state_store):
    """PRD §2.2: whispers the operator doesn't select get no ledger entry."""
    mock_device.add_file(MockFile(name="20260414-161700-Wip1.hda", content=make_wav_bytes(), device_mtime=datetime(2026, 4, 14, 16, 17, 0)))
    result = offloader.scan_pending_files(DEVICE_KEY)
    assert result.whispers  # scan finds it
    # ...but the operator never offloads it. No ledger entry.
    assert not state_store.is_processed(DEVICE_KEY, "20260414-161700-Wip1.hda")


def test_ensure_whispers_dir_error_publishes_actionable_message(archive_dir: Path, mock_device: MockDevice, state_store, event_sink, monkeypatch):
    """PRD §2.7: mkdir failure for whispers dir surfaces cause + remediation."""
    from hidock_direct.classify import RecordingKind
    from hidock_direct.events import Error, Severity
    from hidock_direct.offload import OffloadError, Offloader

    bus, events = event_sink
    mock_device.connect()
    mock_device.add_file(MockFile(name="20250827-012419-Wip67.hda", content=b"\xff\xfb" + b"\x00" * 2048, device_mtime=datetime(2025, 8, 27, 1, 24, 19)))
    off = Offloader(
        adapter=mock_device,
        store=state_store,
        bus=bus,
        archive_dir=archive_dir,
        tmp_dir=archive_dir / ".tmp",
        delete_after_offload=False,
        sleep=lambda *_a, **_k: None,
    )
    # Force mkdir to fail only for the whispers subdir (simulates the archive
    # root being writable but the sibling create failing).
    original_mkdir = Path.mkdir
    def selective_boom(self, *a, **k):
        if self.name == "whispers":
            raise PermissionError("read-only file system")
        return original_mkdir(self, *a, **k)
    monkeypatch.setattr(Path, "mkdir", selective_boom)
    result = off.scan_pending_files(DEVICE_KEY)
    with pytest.raises(OffloadError):
        off.offload_one(device_key=DEVICE_KEY, file=result.whispers[0], kind=RecordingKind.WHISPER)
    err = next((e for e in events if isinstance(e, Error) and e.context == "archive_setup"), None)
    assert err is not None
    assert err.severity is Severity.ERROR
    assert "whispers archive" in err.message
    assert "writable" in err.message  # remediation language


def test_offload_collision_gets_suffix(offloader, mock_device: MockDevice, archive_dir: Path):
    # Two recordings with the same second-precision timestamp → second gets `-1`.
    shared = datetime(2026, 4, 12, 12, 0, 0)
    mock_device.add_file(MockFile(name="REC_A.wav", content=make_wav_bytes(duration_seconds=0.5), device_mtime=shared))
    mock_device.add_file(MockFile(name="REC_B.wav", content=make_wav_bytes(duration_seconds=0.5), device_mtime=shared))
    files = offloader.scan_new_files(DEVICE_KEY)
    out = [offloader.offload(device_key=DEVICE_KEY, file=f).archive_path for f in files]
    assert out[0] == archive_dir / "2026" / "04" / "2026-04-12_120000.wav"
    assert out[1] == archive_dir / "2026" / "04" / "2026-04-12_120000-1.wav"
