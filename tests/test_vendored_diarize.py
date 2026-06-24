"""The vendored diarize_audio package (FR-A1) must import from the installed
app and be co-located with hidock_direct — proving single-clone vendoring,
not a separate sibling install."""

from __future__ import annotations

from pathlib import Path


def test_vendored_diarize_importable():
    import diarize_audio  # noqa: F401
    from diarize_audio.pipeline import process_file  # noqa: F401
    from diarize_audio.config import Config  # noqa: F401


def test_vendored_diarize_colocated_with_hidock():
    import diarize_audio
    import hidock_direct

    d = Path(diarize_audio.__file__).resolve().parent
    h = Path(hidock_direct.__file__).resolve().parent
    # Both packages live side-by-side under the same src/ (editable) or
    # site-packages (wheel) tree — diarize is vendored into this repo.
    assert d.parent == h.parent


def test_vendored_commit_marker_present():
    import diarize_audio

    marker = Path(diarize_audio.__file__).resolve().parent / "VENDORED_COMMIT"
    assert marker.is_file(), "VENDORED_COMMIT pin missing from vendored diarize_audio"
    assert marker.read_text().strip(), "VENDORED_COMMIT is empty"
