"""Filename-based classification of HiDock recordings.

HiDock firmware emits recording filenames with a `-<Prefix><N>.hda` suffix
where `<Prefix>` is `Rec` for meeting recordings and `Wip` for whispers (short
voice notes). Anything else — lowercase variants, user-renamed files, other
extensions — is `UNKNOWN` and requires explicit operator routing.

The timestamp prefix format varies across firmware versions (observed:
`YYYYMMDD-HHMMSS-` on the H1 per the April PRD, and `YYYYMonDD-HHMMSS-`
on the P1 verified live via `scripts/probe_listing.py`). Classification
intentionally matches only the suffix — the timestamp shape is incidental
metadata, not a kind signal. If we coupled classification to a specific
timestamp format we'd misclassify every file on a device whose firmware
emits a different format, which is exactly what happened the first time.

Pure module: no I/O, no dependencies on other runtime modules. Comparison is
case-sensitive by design — firmware drift in the `Rec`/`Wip` token surfaces
in the operator-review queue rather than silently broadening the match.
"""

from __future__ import annotations

import re
from enum import Enum


class RecordingKind(str, Enum):
    MEETING = "meeting"
    WHISPER = "whisper"
    UNKNOWN = "unknown"


# Suffix-only match. Requires at least one character before the `-Rec`/`-Wip`
# indicator so bare `Rec1.hda` (no timestamp prefix at all) falls to UNKNOWN,
# and the separator dash disambiguates `fooRec1.hda` from `foo-Rec1.hda`.
_MEETING_RE = re.compile(r"^.+-Rec\d+\.hda$")
_WHISPER_RE = re.compile(r"^.+-Wip\d+\.hda$")


def classify_recording(filename: str) -> RecordingKind:
    """Return the `RecordingKind` for a bare device filename.

    Returns `UNKNOWN` for anything that is not a case-sensitive match on the
    `-Rec\\d+\\.hda` / `-Wip\\d+\\.hda` suffix. Non-string, path-containing,
    or whitespace-padded inputs are `UNKNOWN` — never an exception.
    """
    if not isinstance(filename, str):
        return RecordingKind.UNKNOWN
    # Reject anything containing a path separator as a defense-in-depth
    # measure -- classification operates on bare device filenames.
    if "/" in filename or "\\" in filename:
        return RecordingKind.UNKNOWN
    # Reject leading/trailing whitespace (the suffix-only regex would
    # otherwise accept " foo-Rec1.hda" because `.+` swallows the space).
    if filename != filename.strip():
        return RecordingKind.UNKNOWN
    if _MEETING_RE.match(filename):
        return RecordingKind.MEETING
    if _WHISPER_RE.match(filename):
        return RecordingKind.WHISPER
    return RecordingKind.UNKNOWN
