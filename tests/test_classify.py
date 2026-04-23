"""PRD §5.1 classify tests — full response-variant matrix."""

from __future__ import annotations

import pytest

from hidock_direct.classify import RecordingKind, classify_recording


@pytest.mark.parametrize(
    "filename, expected",
    [
        # Canonical meeting / whisper filenames
        ("20260414-161650-Rec31.hda", RecordingKind.MEETING),
        ("20250827-012419-Wip67.hda", RecordingKind.WHISPER),
        ("20260101-000000-Rec1.hda", RecordingKind.MEETING),
        ("20260101-000000-Wip0.hda", RecordingKind.WHISPER),
        ("20260414-161650-Rec999.hda", RecordingKind.MEETING),
        # Case-sensitive rejections
        ("20260414-161650-rec31.hda", RecordingKind.UNKNOWN),
        ("20260414-161650-REC31.hda", RecordingKind.UNKNOWN),
        ("20260414-161650-Rec31.HDA", RecordingKind.UNKNOWN),
        # Whitespace rejections (anchored regex)
        ("20260414-161650-Rec31.hda ", RecordingKind.UNKNOWN),
        (" 20260414-161650-Rec31.hda", RecordingKind.UNKNOWN),
        # Structural rejections
        ("20260414-161650-RecA.hda", RecordingKind.UNKNOWN),
        ("20260414-161650-Rec.hda", RecordingKind.UNKNOWN),
        ("20260414-161650-Foo31.hda", RecordingKind.UNKNOWN),
        ("meeting-with-blake.hda", RecordingKind.UNKNOWN),
        ("20260414-161650-Rec31.wav", RecordingKind.UNKNOWN),
        ("20260414-161650-Rec31", RecordingKind.UNKNOWN),
        ("", RecordingKind.UNKNOWN),
        ("   ", RecordingKind.UNKNOWN),
        ("path/20260414-161650-Rec31.hda", RecordingKind.UNKNOWN),
        ("2026414-161650-Rec31.hda", RecordingKind.UNKNOWN),
    ],
)
def test_classify_recording(filename: str, expected: RecordingKind) -> None:
    assert classify_recording(filename) is expected


def test_recording_kind_is_str_enum() -> None:
    assert RecordingKind.MEETING.value == "meeting"
    assert RecordingKind.WHISPER.value == "whisper"
    assert RecordingKind.UNKNOWN.value == "unknown"
