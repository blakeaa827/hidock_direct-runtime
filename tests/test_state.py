"""PRD §5.1 state tests."""

from __future__ import annotations

import json
import wave
from pathlib import Path

import pytest

from hidock_direct.state import DeviceKey, StateStore, default_schema, wav_duration_minutes


def test_state_initial_load(archive_dir: Path) -> None:
    store = StateStore(archive_dir / ".state" / "offload_state.json")
    data = store.load()
    assert data == default_schema()
    assert data["schema_version"] == 1
    assert data["global"]["cumulative_audio_minutes"] == 0.0


def test_state_atomic_write(archive_dir: Path) -> None:
    path = archive_dir / ".state" / "offload_state.json"
    store = StateStore(path)
    key = DeviceKey(model="hidock-h1", serial="SN1")
    store.register_device(key)
    store.record_processed(
        key,
        device_filename="REC_0001.wav",
        size_bytes=100,
        device_mtime="2026-04-12T14:33:00Z",
        sha256="deadbeef",
        archive_path="2026/04/2026-04-12_143300.wav",
        device_deleted=False,
        duration_minutes=1.5,
    )
    # second write triggers .bak from the first write.
    store.record_processed(
        key,
        device_filename="REC_0002.wav",
        size_bytes=200,
        device_mtime=None,
        sha256="cafe",
        archive_path="2026/04/2026-04-12_150000.wav",
        device_deleted=True,
        duration_minutes=0.5,
    )
    assert path.exists()
    bak = path.with_suffix(path.suffix + ".bak")
    assert bak.exists(), "previous revision should be retained as .bak"
    # .tmp should be cleaned up (no hidock_direct tmp files left lingering).
    tmps = [p for p in path.parent.iterdir() if ".tmp" in p.suffixes or p.name.endswith(".tmp")]
    assert not tmps, f"unexpected .tmp files: {tmps}"
    # cumulative minutes accumulates.
    data = json.loads(path.read_text())
    assert data["global"]["cumulative_audio_minutes"] == pytest.approx(2.0)


def test_state_corrupt_recovery(archive_dir: Path) -> None:
    path = archive_dir / ".state" / "offload_state.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    bak = path.with_suffix(path.suffix + ".bak")
    good = default_schema()
    good["devices"]["hidock-h1-SN1"] = {
        "device_model": "hidock-h1",
        "serial": "SN1",
        "first_seen": "2026-04-12T00:00:00+00:00",
        "processed_recordings": {},
    }
    bak.write_text(json.dumps(good))
    path.write_text("{not valid json")
    store = StateStore(path)
    data = store.load()
    assert "hidock-h1-SN1" in data["devices"], "corrupt primary should fall back to .bak"


def test_state_device_namespacing(archive_dir: Path) -> None:
    store = StateStore(archive_dir / ".state" / "offload_state.json")
    k1 = DeviceKey(model="hidock-h1", serial="SN1")
    k2 = DeviceKey(model="hidock-h1e", serial="SN2")
    store.record_processed(
        k1,
        device_filename="REC_A.wav",
        size_bytes=10,
        device_mtime=None,
        sha256="a",
        archive_path="a",
        device_deleted=False,
        duration_minutes=0.1,
    )
    store.record_processed(
        k2,
        device_filename="REC_A.wav",
        size_bytes=20,
        device_mtime=None,
        sha256="b",
        archive_path="b",
        device_deleted=False,
        duration_minutes=0.2,
    )
    assert store.is_processed(k1, "REC_A.wav")
    assert store.is_processed(k2, "REC_A.wav")
    snap = store.snapshot()
    assert k1.namespaced in snap["devices"]
    assert k2.namespaced in snap["devices"]
    assert snap["devices"][k1.namespaced]["processed_recordings"]["REC_A.wav"]["sha256"] == "a"
    assert snap["devices"][k2.namespaced]["processed_recordings"]["REC_A.wav"]["sha256"] == "b"


def test_cumulative_minutes(tmp_path: Path) -> None:
    wav_path = tmp_path / "sample.wav"
    with wave.open(str(wav_path), "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(16000)
        w.writeframes(b"\x00\x00" * 16000 * 60)  # 60 seconds = 1 minute
    minutes = wav_duration_minutes(wav_path)
    assert minutes == pytest.approx(1.0, rel=1e-3)


def test_cumulative_minutes_bad_file_returns_zero(tmp_path: Path) -> None:
    bad = tmp_path / "not-a-wav.bin"
    bad.write_bytes(b"\x00" * 256)
    assert wav_duration_minutes(bad) == 0.0


def test_register_device_updates_on_second_call(archive_dir: Path) -> None:
    path = archive_dir / ".state" / "offload_state.json"
    store = StateStore(path)
    key = DeviceKey(model="hidock-h1", serial="SN42")
    store.register_device(key, first_seen="2026-01-01T00:00:00+00:00")
    # re-register with a mutated-but-same-identity key; first_seen should persist.
    store.register_device(key)
    snap = store.snapshot()
    entry = snap["devices"][key.namespaced]
    assert entry["first_seen"] == "2026-01-01T00:00:00+00:00"
    assert entry["device_model"] == "hidock-h1"
    assert entry["serial"] == "SN42"


def test_mark_touched_updates_last_run(archive_dir: Path) -> None:
    store = StateStore(archive_dir / ".state" / "offload_state.json")
    store.load()
    before = store.snapshot()["global"]["last_run"]
    assert before is None
    store.mark_touched()
    after = store.snapshot()["global"]["last_run"]
    assert isinstance(after, str) and len(after) > 0


def test_processed_names_reports_all(archive_dir: Path) -> None:
    store = StateStore(archive_dir / ".state" / "offload_state.json")
    key = DeviceKey(model="hidock-h1", serial="SN-N")
    for i in range(3):
        store.record_processed(
            key,
            device_filename=f"REC_{i}.wav",
            size_bytes=10,
            device_mtime=None,
            sha256=f"{i:x}",
            archive_path=f"p{i}",
            device_deleted=False,
            duration_minutes=0.0,
        )
    names = set(store.processed_names(key))
    assert names == {"REC_0.wav", "REC_1.wav", "REC_2.wav"}
