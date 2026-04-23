"""Filename-based classification of HiDock recordings.

HiDock firmware emits recording filenames as `YYYYMMDD-HHMMSS-<Prefix><N>.hda`
where `<Prefix>` is `Rec` for meeting recordings and `Wip` for whispers (short
voice notes). Anything else — lowercase variants, user-renamed files, other
extensions — is `UNKNOWN` and requires explicit operator routing.

Pure module: no I/O, no dependencies on other runtime modules. Comparison is
case-sensitive by design — firmware drift surfaces in the operator-review
queue rather than silently broadening the match.
"""

from __future__ import annotations

import re
from enum import Enum


class RecordingKind(str, Enum):
    MEETING = "meeting"
    WHISPER = "whisper"
    UNKNOWN = "unknown"


_MEETING_RE = re.compile(r"^\d{8}-\d{6}-Rec\d+\.hda$")
_WHISPER_RE = re.compile(r"^\d{8}-\d{6}-Wip\d+\.hda$")


def classify_recording(filename: str) -> RecordingKind:
    """Return the `RecordingKind` for a bare device filename.

    Returns `UNKNOWN` for anything that is not an exact, case-sensitive match
    for the canonical meeting or whisper pattern. Non-string, path-containing,
    or whitespace-padded inputs are `UNKNOWN` — never an exception.
    """
    if not isinstance(filename, str):
        return RecordingKind.UNKNOWN
    if _MEETING_RE.match(filename):
        return RecordingKind.MEETING
    if _WHISPER_RE.match(filename):
        return RecordingKind.WHISPER
    return RecordingKind.UNKNOWN
