"""PRD §5.1 classify tests — suffix-only match matrix.

The classifier intentionally validates only the `-Rec\\d+\\.hda` /
`-Wip\\d+\\.hda` suffix. Timestamp-prefix format is incidental metadata
and varies across firmware (`YYYYMMDD-` on H1, `YYYYMonDD-` on P1 per
live probe on 2026-04-23). Classification must not depend on it.
"""

from __future__ import annotations

import pytest

from hidock_direct.classify import RecordingKind, classify_recording


@pytest.mark.parametrize(
    "filename, expected",
    [
        # H1 firmware format (all-digit timestamp)
        ("20260414-161650-Rec31.hda", RecordingKind.MEETING),
        ("20250827-012419-Wip67.hda", RecordingKind.WHISPER),
        ("20260101-000000-Rec1.hda", RecordingKind.MEETING),
        ("20260101-000000-Wip0.hda", RecordingKind.WHISPER),
        ("20260414-161650-Rec999.hda", RecordingKind.MEETING),
        # P1 firmware format (3-letter month) — verified live 2026-04-23.
        ("2026Mar31-110241-Rec54.hda", RecordingKind.MEETING),
        ("2026Mar31-133112-Rec56.hda", RecordingKind.MEETING),
        ("2025Aug27-012419-Wip67.hda", RecordingKind.WHISPER),
        # Arbitrary-but-reasonable prefix — classifier is suffix-only.
        ("anything-goes-here-Rec1.hda", RecordingKind.MEETING),
        ("my-renamed-file-Wip5.hda", RecordingKind.WHISPER),
        # Case-sensitive rejections — firmware drift surfaces in review queue.
        ("20260414-161650-rec31.hda", RecordingKind.UNKNOWN),
        ("20260414-161650-REC31.hda", RecordingKind.UNKNOWN),
        ("20260414-161650-Rec31.HDA", RecordingKind.UNKNOWN),
        # Whitespace rejections (anchored regex)
        ("20260414-161650-Rec31.hda ", RecordingKind.UNKNOWN),
        (" 20260414-161650-Rec31.hda", RecordingKind.UNKNOWN),
        # Structural rejections — suffix must be `-Rec<digits>.hda` exactly.
        ("20260414-161650-RecA.hda", RecordingKind.UNKNOWN),
        ("20260414-161650-Rec.hda", RecordingKind.UNKNOWN),
        ("20260414-161650-Foo31.hda", RecordingKind.UNKNOWN),
        ("meeting-with-blake.hda", RecordingKind.UNKNOWN),
        ("20260414-161650-Rec31.wav", RecordingKind.UNKNOWN),
        ("20260414-161650-Rec31", RecordingKind.UNKNOWN),
        ("", RecordingKind.UNKNOWN),
        ("   ", RecordingKind.UNKNOWN),
        ("path/20260414-161650-Rec31.hda", RecordingKind.UNKNOWN),
        ("Rec1.hda", RecordingKind.UNKNOWN),  # no prefix → UNKNOWN
        ("Wip1.hda", RecordingKind.UNKNOWN),  # no prefix → UNKNOWN
        ("fooRec1.hda", RecordingKind.UNKNOWN),  # missing separator → UNKNOWN
    ],
)
def test_classify_recording(filename: str, expected: RecordingKind) -> None:
    assert classify_recording(filename) is expected


def test_recording_kind_is_str_enum() -> None:
    assert RecordingKind.MEETING.value == "meeting"
    assert RecordingKind.WHISPER.value == "whisper"
    assert RecordingKind.UNKNOWN.value == "unknown"
