"""Walk inbox directories for `.wav` candidates."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Iterable, Iterator

from .state import State

log = logging.getLogger(__name__)


def walk_wavs(inbox_dirs: Iterable[Path]) -> Iterator[Path]:
    """Yield every `.wav` under each inbox, resolving symlink escape.

    Files whose realpath escapes the inbox root are skipped silently
    (defense-in-depth against adversarial symlinks in shared inboxes).
    """
    for inbox in inbox_dirs:
        inbox = Path(inbox).expanduser().resolve()
        if not inbox.exists():
            continue
        for dirpath, _dirnames, filenames in os.walk(inbox, followlinks=False):
            for name in filenames:
                if not name.endswith(".wav"):
                    continue
                p = Path(dirpath) / name
                try:
                    real = p.resolve(strict=True)
                except (FileNotFoundError, RuntimeError):
                    continue
                try:
                    real.relative_to(inbox)
                except ValueError:
                    log.debug("skipping symlink escape: %s", p)
                    continue
                yield p


def find_candidates(
    inbox_dirs: Iterable[Path],
    state: State,
    in_flight_ttl_minutes: int,
    *,
    include_keys: bool = False,
):
    """Yield every `.wav` that is eligible for transcription.

    Skips files that already have a sibling `.aai.json`, or whose state is
    `done`, `error_permanent`, or a fresh `in_flight`.
    """
    inbox_roots = [Path(p).expanduser().resolve() for p in inbox_dirs]
    for p in walk_wavs(inbox_roots):
        if (p.parent / f"{p.stem}.aai.json").exists():
            continue
        # derive state key: relative path from whichever inbox contains this file
        key = _state_key(p, inbox_roots)
        if state.should_skip(key, in_flight_ttl_minutes=in_flight_ttl_minutes):
            continue
        yield (p, key) if include_keys else p


def _state_key(path: Path, inbox_roots: list[Path]) -> str:
    resolved = path.resolve()
    for root in inbox_roots:
        try:
            rel = resolved.relative_to(root)
            return str(rel)
        except ValueError:
            continue
    return resolved.name
