"""Tests for `scripts/transcribe_file.py` — the Audio Hijack import hook.

The real pipeline is replaced by a fake that publishes the terminal event the
bridge would, so nothing here talks to AssemblyAI.
"""

from __future__ import annotations

import importlib.util
import os
from datetime import datetime
from pathlib import Path

import pytest

from hidock_direct.events import TranscribeComplete, TranscribeFailed
from hidock_direct.locks import FileLock

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "transcribe_file.py"
WHEN = datetime(2026, 10, 13, 9, 30, 5)


@pytest.fixture
def mod():
    spec = importlib.util.spec_from_file_location("transcribe_file_script", SCRIPT)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


@pytest.fixture
def archive(tmp_path, monkeypatch):
    arc = tmp_path / "archive"
    env = tmp_path / ".env"
    env.write_text(f"HIDOCK_ARCHIVE_DIR={arc}\n")
    monkeypatch.setenv("HIDOCK_DIRECT_ENV_FILE", str(env))
    monkeypatch.delenv("HIDOCK_ARCHIVE_DIR", raising=False)
    return arc


def _recording(tmp_path, name="Recording.mp3", data=b"ID3fake-audio"):
    p = tmp_path / "hijack" / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)
    ts = WHEN.timestamp()
    os.utime(p, (ts, ts))
    return p


class FakeTranscribe:
    def __init__(self, ok=True):
        self.ok = ok
        self.calls = []

    def __call__(self, path, archive_dir, *, bus, device_filename, recorded_at, reuse_existing_transcript):
        self.calls.append((path, reuse_existing_transcript))
        bus.publish(TranscribeComplete(device_filename=device_filename, drive_file_id=None) if self.ok
                    else TranscribeFailed(device_filename=device_filename, reason="pipeline status='error'"))


def _run(mod, files, fake, status=lambda p, a: None, extra=()):
    return mod.main([*map(str, files), "--no-notify", "--wait", "0", *extra], transcribe=fake, status=status)


@pytest.fixture(autouse=True)
def fixed_start(mod, monkeypatch):
    monkeypatch.setattr(mod, "recording_start", lambda p: WHEN)


def test_imports_under_recording_start_name_and_transcribes(mod, archive, tmp_path):
    src = _recording(tmp_path)
    data = src.read_bytes()
    fake = FakeTranscribe()
    assert _run(mod, [src], fake) == 0
    dest = archive / "2026" / "10" / "2026-10-13_093005.mp3"
    assert dest.read_bytes() == data
    assert not src.exists(), "source must be moved, not copied (operator is short on disk)"
    assert fake.calls == [(dest, False)]


def test_keep_source_copies(mod, archive, tmp_path):
    src = _recording(tmp_path)
    assert _run(mod, [src], FakeTranscribe(), extra=["--keep-source"]) == 0
    assert src.exists()
    assert (archive / "2026" / "10" / "2026-10-13_093005.mp3").read_bytes() == src.read_bytes()


def test_short_copy_keeps_original(mod, archive, tmp_path, monkeypatch):
    src = _recording(tmp_path)
    monkeypatch.setattr(mod.shutil, "copy2", lambda a, b: Path(b).write_bytes(b"x"))
    fake = FakeTranscribe()
    assert _run(mod, [src], fake) == 1
    assert src.exists() and fake.calls == []
    assert list((archive / "2026" / "10").iterdir()) == []


def test_same_bytes_twice_reuses_path_and_skips_when_ledger_done(mod, archive, tmp_path):
    src = _recording(tmp_path)
    fake = FakeTranscribe()
    _run(mod, [src], fake, extra=["--keep-source"])
    assert _run(mod, [src], fake, status=lambda p, a: "done") == 0
    assert not src.exists(), "a duplicate of archived bytes is deleted, not left behind"
    assert len(fake.calls) == 1, "a done ledger entry must never be billed again"
    assert sorted(p.name for p in (archive / "2026" / "10").iterdir()) == ["2026-10-13_093005.mp3"]


def test_saved_aai_json_forces_free_rerender(mod, archive, tmp_path):
    src = _recording(tmp_path)
    fake = FakeTranscribe(ok=False)
    _run(mod, [src], fake, extra=["--keep-source"])
    (archive / "2026" / "10" / "2026-10-13_093005.aai.json").write_text("{}")
    fake.ok = True
    assert _run(mod, [src], fake, status=lambda p, a: "error") == 0
    assert fake.calls[-1][1] is True


def test_different_bytes_same_second_get_suffix(mod, archive, tmp_path):
    a = _recording(tmp_path, "a.mp3", b"one")
    b = _recording(tmp_path, "b.mp3", b"two")
    assert _run(mod, [a, b], FakeTranscribe()) == 0
    assert sorted(p.name for p in (archive / "2026" / "10").iterdir()) == [
        "2026-10-13_093005-1.mp3", "2026-10-13_093005.mp3"]


def test_wav_refused_and_nothing_archived(mod, archive, tmp_path):
    src = _recording(tmp_path, "Recording.wav")
    fake = FakeTranscribe()
    assert _run(mod, [src], fake) == 1
    assert fake.calls == []
    assert not (archive / "2026").exists()


def test_pipeline_failure_exits_nonzero(mod, archive, tmp_path):
    assert _run(mod, [_recording(tmp_path)], FakeTranscribe(ok=False)) == 1


def test_running_app_lock_blocks_and_nothing_happens(mod, archive, tmp_path):
    from hidock_direct.config import load_config

    held = FileLock(load_config().lock_path)
    held.acquire()
    try:
        fake = FakeTranscribe()
        assert _run(mod, [_recording(tmp_path)], fake) == 1
        assert fake.calls == []
        assert not (archive / "2026").exists()
    finally:
        held.release()


def test_dry_run_writes_nothing(mod, archive, tmp_path):
    fake = FakeTranscribe()
    assert _run(mod, [_recording(tmp_path)], fake, extra=["--dry-run"]) == 0
    assert fake.calls == [] and not (archive / "2026").exists()


def test_recording_start_prefers_birthtime(tmp_path):
    spec = importlib.util.spec_from_file_location("tf2", SCRIPT)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    p = tmp_path / "x.mp3"
    p.write_bytes(b"x")
    later = datetime(2030, 1, 1).timestamp()
    os.utime(p, (later, later))
    if hasattr(p.stat(), "st_birthtime"):
        assert m.recording_start(p) < datetime(2030, 1, 1)


def test_non_audio_refused(mod, archive, tmp_path):
    fake = FakeTranscribe()
    assert _run(mod, [_recording(tmp_path, "notes.txt")], fake) == 1
    assert fake.calls == [] and not (archive / "2026").exists()
