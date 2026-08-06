#!/usr/bin/env python3
"""Repair `recorded_at` in transcripts written before the recording-start fix.

Transcripts produced before that fix carry the time the recording was *offloaded*
— recording-end plus USB transfer — rather than the time it started. The error is
at minimum the full length of the recording and grows with however long the
device sat unplugged.

The true recording start survives in each archive basename (`YYYY-MM-DD_HHMMSS`),
which is minted from the device's own report, so the corpus is repairable in
place: no re-submission to AssemblyAI, no re-render of the transcript body.

Only the frontmatter `recorded_at:` line and the `# YYYY-MM-DD HH:MM` heading are
rewritten. Files this tool did not name, and files with no `recorded_at:`
frontmatter, are left untouched.

Usage:
    scripts/backfill_recorded_at.py <archive-dir>            # dry run (default)
    scripts/backfill_recorded_at.py <archive-dir> --apply    # write changes

The archive directory is a required argument with no default, so the script
cannot be pointed at a real archive by accident.

Timezone note: the device reports local wall-clock with no zone, so the emitted
offset is this machine's, resolved for the recording's own date (DST-aware). Run
the backfill in the timezone the recordings were made in.
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path

BASENAME_FORMAT = "%Y-%m-%d_%H%M%S"
# Mirrors `hidock_direct.offload.ARCHIVE_STEM_PATTERN`: the timestamp, plus the
# optional `-1` / `-2` collision suffix appended when two recordings share a
# second-precision timestamp. Kept as a standalone copy so the script runs from
# a bare checkout without importing the package.
ARCHIVE_STEM_PATTERN = re.compile(r"(?P<timestamp>\d{4}-\d{2}-\d{2}_\d{6})(?:-\d+)?")
RECORDED_AT_LINE = re.compile(r"^recorded_at: .*$", re.M)
HEADING_LINE = re.compile(r"^# \d{4}-\d{2}-\d{2} \d{2}:\d{2}$", re.M)


def recording_start_from_name(md_path: Path) -> datetime | None:
    """Recording start encoded in the archive basename, or None if not ours."""
    match = ARCHIVE_STEM_PATTERN.fullmatch(md_path.stem)
    if match is None:
        return None
    try:
        return datetime.strptime(match.group("timestamp"), BASENAME_FORMAT).astimezone()
    except ValueError:
        return None


def current_recorded_at(text: str) -> datetime | None:
    """Parse the document's existing `recorded_at`, across every shape written.

    Three surface forms exist in real archives, from successive renderer
    versions: `2026-08-05T08:32:04-05:00` (current), `2026-08-05 08:32:04-05:00`
    (space separator), and the same single-quoted. All three parse to the same
    instant; only the spelling differs.
    """
    match = RECORDED_AT_LINE.search(text)
    if match is None:
        return None
    raw = match.group(0).removeprefix("recorded_at:").strip().strip("'\"")
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def repair(text: str, recorded_at: datetime) -> str | None:
    """Return the corrected document, or None when nothing needs changing.

    Compares the recorded INSTANT, not its spelling, so a document that already
    carries the right time in an older format is left completely alone. Fixing
    what is wrong and reformatting what is not are different jobs; this script
    only does the first.

    Both substitutions are anchored to line starts and bounded to the first
    match, so a timestamp appearing inside the transcript body is never touched.
    """
    existing = current_recorded_at(text)
    if existing is None:
        return None
    if abs((existing - recorded_at).total_seconds()) <= 1:
        return None

    updated = RECORDED_AT_LINE.sub(
        f"recorded_at: {recorded_at.isoformat(timespec='seconds')}", text, count=1
    )
    updated = HEADING_LINE.sub(f"# {recorded_at:%Y-%m-%d %H:%M}", updated, count=1)
    return updated if updated != text else None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("root", type=Path, help="archive directory to scan (required)")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write the corrections; without this the script only reports",
    )
    args = parser.parse_args(argv)

    if not args.root.is_dir():
        print(f"error: {args.root} is not a directory", file=sys.stderr)
        return 2

    changed = 0
    skipped_foreign = 0
    for md in sorted(args.root.rglob("*.md")):
        recorded_at = recording_start_from_name(md)
        if recorded_at is None:
            skipped_foreign += 1
            continue
        try:
            text = md.read_text()
        except (OSError, UnicodeDecodeError) as exc:
            print(f"skip {md}: {exc}", file=sys.stderr)
            continue
        repaired = repair(text, recorded_at)
        if repaired is None:
            continue
        old = RECORDED_AT_LINE.search(text).group(0).removeprefix("recorded_at:").strip()
        verb = "rewriting" if args.apply else "would rewrite"
        print(f"{verb} {md.relative_to(args.root)}: {old} -> {recorded_at.isoformat(timespec='seconds')}")
        if args.apply:
            md.write_text(repaired)
        changed += 1

    noun = "rewritten" if args.apply else "to rewrite"
    print(f"{changed} file(s) {noun}; {skipped_foreign} not produced by this tool (skipped)")
    if not args.apply and changed:
        print("dry run — re-run with --apply to write the corrections")
    return 0


if __name__ == "__main__":
    sys.exit(main())
