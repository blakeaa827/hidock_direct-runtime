"""Single-instance lock via fcntl.flock."""

from __future__ import annotations

import errno
import fcntl
from pathlib import Path
from typing import IO


class AlreadyLocked(RuntimeError):
    """Raised when another process holds the lock."""


class FileLock:
    def __init__(self, path: Path):
        self.path = Path(path)
        self._fh: IO | None = None

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fh = open(self.path, "w")
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            fh.close()
            if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
                raise AlreadyLocked(
                    f"another diarize_audio instance holds {self.path}"
                ) from exc
            raise
        self._fh = fh
        return self

    def __exit__(self, *exc) -> None:
        if self._fh is None:
            return
        try:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        finally:
            self._fh.close()
            self._fh = None
