"""PRD §2.7 / §5.1: single-instance lock."""

from __future__ import annotations

from pathlib import Path

import pytest

from hidock_direct.locks import FileLock, LockHeld


def test_lock_acquire_release(tmp_path: Path):
    lock = FileLock(tmp_path / "x.lock")
    lock.acquire()
    assert lock.path.exists()
    lock.release()


def test_lock_single_instance(tmp_path: Path):
    p = tmp_path / "x.lock"
    a = FileLock(p)
    b = FileLock(p)
    a.acquire()
    with pytest.raises(LockHeld):
        b.acquire()
    a.release()
    # once released, a second process can grab it.
    b.acquire()
    b.release()


def test_lock_context_manager(tmp_path: Path):
    p = tmp_path / "x.lock"
    with FileLock(p) as _first:
        with pytest.raises(LockHeld):
            FileLock(p).acquire()
    # After the with-block, a new lock can acquire.
    with FileLock(p):
        pass
