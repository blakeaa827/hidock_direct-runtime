"""Tests for the one-shot `recorded_at` backfill.

Every transcript produced before the recording-start fix carries the offload
write time instead of the recording start. The true value survives losslessly
in each archive basename, so the corpus is repairable in place — no re-submission
to AssemblyAI, no re-render of the utterance body.

See `bug_report_recorded_at_uses_offload_mtime.md`.
"""

from __future__ import annotations

import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "backfill_recorded_at.py"

DRIFTED_MD = """\
---
recorded_at: 2026-08-05T08:54:45-05:00
duration_seconds: 1320
audio_duration_minutes: 22.00
source_filename: 2026-08-05_083204.mp3
assemblyai_id: abc123
language_code: en_us
speaker_count: 2
auto_highlights: []
---

# 2026-08-05 08:54

**Speaker 1** (00:00): The body must survive byte-identical.

**Speaker 2** (00:14): Including this line, and the 08:54 inside it.
"""


def _run(root: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SCRIPT), str(root), *args],
        capture_output=True,
        text=True,
    )


def _write(root: Path, name: str, text: str) -> Path:
    p = root / "2026" / "08" / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)
    return p


def test_rewrites_recorded_at_and_heading_from_basename(tmp_path):
    md = _write(tmp_path, "2026-08-05_083204.md", DRIFTED_MD)

    result = _run(tmp_path, "--apply")

    assert result.returncode == 0, result.stderr
    text = md.read_text()
    assert "recorded_at: 2026-08-05T08:32:04" in text
    assert "# 2026-08-05 08:32" in text
    assert "recorded_at: 2026-08-05T08:54:45-05:00" not in text
    assert "# 2026-08-05 08:54" not in text


def test_preserves_utterance_body_byte_for_byte(tmp_path):
    md = _write(tmp_path, "2026-08-05_083204.md", DRIFTED_MD)
    body_before = DRIFTED_MD.split("---\n", 2)[2].split("\n", 2)[2]

    _run(tmp_path, "--apply")

    body_after = md.read_text().split("---\n", 2)[2].split("\n", 2)[2]
    assert body_after == body_before
    # The heading rewrite must not chase the same timestamp through prose.
    assert "Including this line, and the 08:54 inside it." in md.read_text()


def test_is_idempotent(tmp_path):
    md = _write(tmp_path, "2026-08-05_083204.md", DRIFTED_MD)

    _run(tmp_path, "--apply")
    once = md.read_text()
    second = _run(tmp_path, "--apply")
    twice = md.read_text()

    assert twice == once
    assert "0 file(s) rewritten" in second.stdout


def test_dry_run_is_the_default_and_writes_nothing(tmp_path):
    md = _write(tmp_path, "2026-08-05_083204.md", DRIFTED_MD)

    result = _run(tmp_path)

    assert result.returncode == 0, result.stderr
    assert md.read_text() == DRIFTED_MD, "dry run must not modify the archive"
    assert "would rewrite" in result.stdout.lower()


def test_skips_files_this_tool_did_not_name(tmp_path):
    """The archive also holds vendor HiNotes exports — leave them alone."""
    vendor = _write(tmp_path, "Some Vendor Export.md", DRIFTED_MD)

    _run(tmp_path, "--apply")

    assert vendor.read_text() == DRIFTED_MD


def test_skips_files_without_recorded_at_frontmatter(tmp_path):
    plain = _write(tmp_path, "2026-08-05_083204.md", "# Notes\n\nNo frontmatter here.\n")

    result = _run(tmp_path, "--apply")

    assert result.returncode == 0, result.stderr
    assert plain.read_text() == "# Notes\n\nNo frontmatter here.\n"


def test_reports_already_correct_files_as_unchanged(tmp_path):
    correct = DRIFTED_MD.replace(
        "recorded_at: 2026-08-05T08:54:45-05:00", "recorded_at: 2026-08-05T08:32:04-05:00"
    ).replace("# 2026-08-05 08:54", "# 2026-08-05 08:32")
    md = _write(tmp_path, "2026-08-05_083204.md", correct)

    result = _run(tmp_path, "--apply")

    assert md.read_text() == correct
    assert "0 file(s) rewritten" in result.stdout


def test_requires_an_explicit_root(tmp_path):
    """No default archive path — the script cannot be run against production by accident."""
    result = subprocess.run(
        [sys.executable, str(SCRIPT)], capture_output=True, text=True
    )

    assert result.returncode != 0
    assert "root" in (result.stderr + result.stdout).lower()


@pytest.mark.parametrize(
    "stem,expected_iso,expected_heading",
    [
        ("2026-08-04_234712", "2026-08-04T23:47:12", "# 2026-08-04 23:47"),
        ("2026-01-01_000000", "2026-01-01T00:00:00", "# 2026-01-01 00:00"),
        ("2026-12-31_235959", "2026-12-31T23:59:59", "# 2026-12-31 23:59"),
    ],
)
def test_handles_boundary_timestamps(tmp_path, stem, expected_iso, expected_heading):
    md = _write(tmp_path, f"{stem}.md", DRIFTED_MD)

    _run(tmp_path, "--apply")

    text = md.read_text()
    assert f"recorded_at: {expected_iso}" in text
    assert expected_heading in text


def test_emitted_recorded_at_keeps_a_utc_offset(tmp_path):
    """Downstream parsers read an offset-bearing ISO timestamp; don't change the shape."""
    import re

    md = _write(tmp_path, "2026-08-05_083204.md", DRIFTED_MD)

    _run(tmp_path, "--apply")

    assert re.search(
        r"^recorded_at: 2026-08-05T08:32:04[+-]\d{2}:\d{2}$", md.read_text(), re.M
    )


def test_reports_a_count_of_rewritten_files(tmp_path):
    for stem in ("2026-08-05_083204", "2026-08-05_110814", "2026-08-05_120519"):
        _write(tmp_path, f"{stem}.md", DRIFTED_MD)

    result = _run(tmp_path, "--apply")

    assert "3 file(s) rewritten" in result.stdout


def test_leaves_the_recording_date_alone_when_only_the_time_drifted(tmp_path):
    """A same-day drift is still a drift — the fix is not date-only."""
    md = _write(tmp_path, "2026-08-05_083204.md", DRIFTED_MD)

    _run(tmp_path, "--apply")

    parsed = datetime.fromisoformat(
        md.read_text().split("recorded_at: ")[1].split("\n")[0]
    )
    assert (parsed.hour, parsed.minute, parsed.second) == (8, 32, 4)


# --- frontmatter value surface forms (Gate 1 step 7) -------------------------
#
# Successive renderer versions wrote `recorded_at` three different ways. The
# backfill must read all three, or it silently mis-classifies older transcripts
# as unfixable (or, worse, rewrites correct ones only to change their spelling).

SPACE_SEPARATED = DRIFTED_MD.replace(
    "recorded_at: 2026-08-05T08:54:45-05:00", "recorded_at: 2026-08-05 08:54:45-05:00"
)
QUOTED = DRIFTED_MD.replace(
    "recorded_at: 2026-08-05T08:54:45-05:00", "recorded_at: '2026-08-05 08:54:45-05:00'"
)


@pytest.mark.parametrize(
    "source", [DRIFTED_MD, SPACE_SEPARATED, QUOTED], ids=["iso-T", "space-sep", "quoted"]
)
def test_corrects_every_recorded_at_surface_form(tmp_path, source):
    md = _write(tmp_path, "2026-08-05_083204.md", source)

    result = _run(tmp_path, "--apply")

    assert "1 file(s) rewritten" in result.stdout
    assert "recorded_at: 2026-08-05T08:32:04" in md.read_text()


@pytest.mark.parametrize(
    "template", [DRIFTED_MD, SPACE_SEPARATED, QUOTED], ids=["iso-T", "space-sep", "quoted"]
)
def test_leaves_already_correct_files_untouched_whatever_their_format(tmp_path, template):
    """Right instant, older spelling — not this script's business to reformat.

    4 of the operator's 670 archived transcripts are in exactly this state.
    Rewriting them would inflate the change set with files that were never wrong.
    """
    correct = template.replace("08:54:45", "08:32:04").replace(
        "# 2026-08-05 08:54", "# 2026-08-05 08:32"
    )
    md = _write(tmp_path, "2026-08-05_083204.md", correct)

    result = _run(tmp_path, "--apply")

    assert md.read_text() == correct, "a correct file was rewritten"
    assert "0 file(s) rewritten" in result.stdout


def test_corrects_collision_suffixed_transcripts(tmp_path):
    """`_unique_archive_path` mints `-1`/`-2` names; they carry a recording time too."""
    md = _write(tmp_path, "2026-08-05_083204-1.md", DRIFTED_MD)

    _run(tmp_path, "--apply")

    assert "recorded_at: 2026-08-05T08:32:04" in md.read_text()
