"""Shared fixtures: temporary archive, event-sink bus, offloader wiring."""

from __future__ import annotations

import ast
import inspect
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


# -- injection-seam contract helpers ---------------------------------------
#
# `run_retry_batch` calls its `transcribe` seam with kwargs the production
# default (`transcribe.transcribe_file`) did not accept, and every double in the
# suite was `def f(path, archive, **kw)` — strictly more permissive than the real
# collaborator, so it accepted calls production refused and the whole retry
# execution path shipped dead. See
# planning/bug_report_retry_batch_calls_transcribe_file_with_unsupported_kwargs.md.
#
# `faithful` makes a double refuse exactly what the real function refuses;
# `call_kwargs_in` derives the caller's kwarg set from the call site itself, so
# the contract test can't drift from the code it is checking.


def faithful(real, impl):
    """Wrap a test double so it refuses any call the real function would refuse.

    The double stays free to fake behaviour; it loses only the freedom to accept
    an argument list production would reject.
    """
    signature = inspect.signature(real)

    def _double(*args, **kwargs):
        signature.bind(*args, **kwargs)
        return impl(*args, **kwargs)

    _double.__name__ = getattr(impl, "__name__", "faithful_double")
    return _double


def call_sites_of(module_path: Path, callee: str) -> List[tuple]:
    """Every `<callee>(...)` call in a module as (positional_count, kwarg_names).

    Derived from the source rather than restated, so a new argument at the call
    site is covered without anyone remembering to update the test.
    """
    tree = ast.parse(module_path.read_text())
    sites = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == callee:
            sites.append((len(node.args), [kw.arg for kw in node.keywords if kw.arg]))
    if not sites:
        raise AssertionError(f"no `{callee}(...)` call found in {module_path.name}")
    return sites


def call_kwargs_in(module_path: Path, callee: str) -> List[str]:
    """Keyword names passed at every `<callee>(...)` call in a module."""
    return [name for _, names in call_sites_of(module_path, callee) for name in names]


def injection_seams(module_path: Path) -> List[tuple]:
    """Seams of the shape `name = name or <default>` as (name, default_node).

    The left name must equal the first alternative, which is what distinguishes
    an injectable collaborator from ordinary value coalescing
    (`when = device_mtime or fallback_mtime` is not a seam).
    """
    tree = ast.parse(module_path.read_text())
    seams = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target, value = node.targets[0], node.value
        if not isinstance(target, ast.Name):
            continue
        if not (isinstance(value, ast.BoolOp) and isinstance(value.op, ast.Or)):
            continue
        first = value.values[0]
        if isinstance(first, ast.Name) and first.id == target.id:
            seams.append((target.id, value.values[1]))
    return seams
