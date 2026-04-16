"""Shared fixtures: temporary archive, event-sink bus, offloader wiring."""

from __future__ import annotations

from pathlib import Path
from typing import List

import pytest

from hidock_direct.events import Event, EventBus
from hidock_direct.offload import Offloader
from hidock_direct.state import StateStore

from tests.fixtures.mock_device import MockDevice


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
