"""Shared fixtures: temporary archive, event-sink bus, offloader wiring."""

from __future__ import annotations

import shutil
import subprocess
import wave
from pathlib import Path
from typing import List

import pytest

from hidock_direct.events import Event, EventBus
from hidock_direct.offload import Offloader
from hidock_direct.state import StateStore

from tests.fixtures.mock_device import MockDevice


@pytest.fixture
def synthetic_wav(tmp_path: Path) -> Path:
    """A valid 1-second mono 16kHz WAV at tmp_path/synthetic.wav."""
    p = tmp_path / "synthetic.wav"
    with wave.open(str(p), "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(16000)
        w.writeframes(b"\x00\x00" * 16000)  # exactly 1 second of silence
    return p


@pytest.fixture
def synthetic_mp3(tmp_path: Path) -> Path:
    """A valid ~2-second silent MP3 at tmp_path/synthetic.mp3.

    Generated via ffmpeg if available; mutagen alone cannot author MP3 frames
    that pass its own frame-sync validation. Skips cleanly on machines without
    ffmpeg — operator-side dev path on macOS has it via Homebrew, but CI
    without ffmpeg legitimately can't exercise the MP3-positive path. Gate 4
    Live PoL against a real archive MP3 covers the same surface.
    """
    if shutil.which("ffmpeg") is None:
        pytest.skip("ffmpeg not installed; MP3 fixture cannot be built")
    p = tmp_path / "synthetic.mp3"
    result = subprocess.run(
        [
            "ffmpeg", "-y", "-loglevel", "error",
            "-f", "lavfi", "-i", "anullsrc=channel_layout=mono:sample_rate=16000",
            "-t", "2",
            "-b:a", "32k",
            "-codec:a", "libmp3lame",
            str(p),
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.skip(f"ffmpeg failed to build MP3 fixture: {result.stderr}")
    return p


@pytest.fixture
def archive_dir(tmp_path: Path) -> Path:
    root = tmp_path / "HiDock_archive"
    (root / ".state").mkdir(parents=True, exist_ok=True)
    (root / ".tmp").mkdir(parents=True, exist_ok=True)
    return root


@pytest.fixture
def state_store(archive_dir: Path) -> StateStore:
    return StateStore(archive_dir / ".state" / "offload_state.json")


@pytest.fixture
def event_sink():
    events: List[Event] = []
    bus = EventBus()
    bus.subscribe(events.append)
    return bus, events


@pytest.fixture
def mock_device() -> MockDevice:
    return MockDevice()


@pytest.fixture
def offloader(archive_dir: Path, mock_device: MockDevice, state_store: StateStore, event_sink):
    bus, _ = event_sink
    mock_device.connect()
    return Offloader(
        adapter=mock_device,
        store=state_store,
        bus=bus,
        archive_dir=archive_dir,
        tmp_dir=archive_dir / ".tmp",
        delete_after_offload=False,
        sleep=lambda *_a, **_k: None,  # no-op so size-stable check doesn't actually sleep
    )
