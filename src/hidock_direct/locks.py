"""Exclusive single-instance flock.

Used to prevent two `hidock_direct` processes from racing on the same archive
directory. The lock lives at `<archive>/.state/hidock_direct.lock` and is held
for the lifetime of the process.
"""

from __future__ import annotations

import errno
import fcntl
import os
from pathlib import Path
from typing import Optional


class LockHeld(RuntimeError):
    """Another process holds the lock."""


class FileLock:
    def __init__(self, path: os.PathLike[str] | str):
        self._path = Path(path)
        self._fd: Optional[int] = None

    @property
    def path(self) -> Path:
        return self._path

    def acquire(self) -> None:
        if self._fd is not None:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self._path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(fd)
            if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
                raise LockHeld(f"Another instance is running (lock held: {self._path})") from exc
            raise
        os.ftruncate(fd, 0)
        os.write(fd, f"{os.getpid()}\n".encode())
        os.fsync(fd)
        self._fd = fd

    def release(self) -> None:
        if self._fd is None:
            return
        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            os.close(self._fd)
            self._fd = None

    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(self, *_exc) -> None:
        self.release()
