#!/usr/bin/env python3
"""Measure a live transcript's speaker attribution against a reference transcript.

Usage:
    python scripts/compare_live_to_reference.py <live.aai.json> <reference.md>

`<live.aai.json>` is the parity file `live_archive` writes beside a live
session's `.md`. Its utterances are the provider's own TURNS, unmerged, keyed
`near` / `far-<label>` / `far-unlabelled`.

`<reference.md>` is a speaker-attributed transcript from a source that knows who
spoke because every person had their own audio feed: a Teams export, one line
per utterance, shaped `**Name** (hh:mm:ss): text`.

The two word sequences are aligned (difflib over normalised words). Each matched
live word then carries both its live label and its true speaker. The script
prints:
  * coverage: the share of live words that aligned at all;
  * majority-correct: the share of far words on a label whose majority speaker
    is the person who said them;
  * per-label purity: label -> people, in %;
  * per-person spread: person -> labels;
  * mixed turns: far turns whose words belong to more than one person, which is
    the shape a too-long turn produces.

It reports and never gates: exit 0 whenever both files parse. It sends nothing
anywhere. live_speaker_revisions_sdk_upgrade_prd.md FR-3, with the baseline in
FR-3.2.
"""

from __future__ import annotations

import argparse
import collections
import difflib
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

_REFERENCE_LINE = re.compile(r"^\*\*(.+?)\*\* \((\d+):(\d+):(\d+)\): (.*)$", re.MULTILINE)
_NEAR = "near"


def _norm(word: str) -> str:
    return re.sub(r"[^a-z0-9]", "", word.lower())


def _words(text: str) -> List[str]:
    return [n for n in (_norm(w) for w in text.split()) if n]


@dataclass
class Report:
    live_words: int
    reference_words: int
    matched_words: int
    far_words: int
    far_majority_correct: int
    far_turns: int
    mixed_far_turns: int
    purity: Dict[str, collections.Counter] = field(default_factory=dict)
    spread: Dict[str, collections.Counter] = field(default_factory=dict)

    @property
    def coverage(self) -> float:
        return self.matched_words / self.live_words if self.live_words else 0.0


def parse_reference(text: str) -> List[Tuple[str, str]]:
    """`(speaker, text)` per reference utterance, in order."""
    return [(m.group(1), m.group(5)) for m in _REFERENCE_LINE.finditer(text)]


def compare(live: dict, reference: Sequence[Tuple[str, str]]) -> Report:
    utterances = live.get("utterances") or []
    live_words: List[str] = []
    live_label: List[str] = []
    live_turn: List[int] = []
    for index, utterance in enumerate(utterances):
        for word in _words(utterance.get("text", "")):
            live_words.append(word)
            live_label.append(utterance.get("speaker", "?"))
            live_turn.append(index)

    ref_words: List[str] = []
    ref_speaker: List[str] = []
    for speaker, text in reference:
        for word in _words(text):
            ref_words.append(word)
            ref_speaker.append(speaker)

    match: List[Optional[int]] = [None] * len(live_words)
    matcher = difflib.SequenceMatcher(None, live_words, ref_words, autojunk=False)
    for a, b, size in matcher.get_matching_blocks():
        for k in range(size):
            match[a + k] = b + k

    purity: Dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    per_turn: Dict[int, collections.Counter] = collections.defaultdict(collections.Counter)
    for i, j in enumerate(match):
        if j is None:
            continue
        purity[live_label[i]][ref_speaker[j]] += 1
        per_turn[live_turn[i]][ref_speaker[j]] += 1

    spread: Dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    for label, people in purity.items():
        for person, count in people.items():
            spread[person][label] += count

    far_words = 0
    far_majority_correct = 0
    for label, people in purity.items():
        if label == _NEAR:
            continue
        far_words += sum(people.values())
        far_majority_correct += people.most_common(1)[0][1]

    far_turns = 0
    mixed = 0
    for index, people in per_turn.items():
        if utterances[index].get("speaker") == _NEAR:
            continue
        far_turns += 1
        if len(people) > 1:
            mixed += 1

    return Report(
        live_words=len(live_words),
        reference_words=len(ref_words),
        matched_words=sum(1 for j in match if j is not None),
        far_words=far_words,
        far_majority_correct=far_majority_correct,
        far_turns=far_turns,
        mixed_far_turns=mixed,
        purity=dict(purity),
        spread=dict(spread),
    )


def _pct(n: int, total: int) -> str:
    return f"{n / total:.0%}" if total else "n/a"


def format_report(report: Report) -> str:
    lines = [
        f"coverage          {report.coverage:.0%} "
        f"({report.matched_words}/{report.live_words} live words aligned; "
        f"reference has {report.reference_words})",
        f"majority-correct  {_pct(report.far_majority_correct, report.far_words)} "
        f"({report.far_majority_correct}/{report.far_words} far words)",
        f"mixed far turns   {report.mixed_far_turns} of {report.far_turns}",
        "",
        "label -> people",
    ]
    for label, people in sorted(report.purity.items(), key=lambda kv: -sum(kv[1].values())):
        total = sum(people.values())
        detail = ", ".join(f"{p} {_pct(n, total)}" for p, n in people.most_common())
        lines.append(f"  {label:16} {total:6}  {detail}")
    lines += ["", "person -> labels"]
    for person, labels in sorted(report.spread.items(), key=lambda kv: -sum(kv[1].values())):
        total = sum(labels.values())
        detail = ", ".join(f"{label} {_pct(n, total)}" for label, n in labels.most_common())
        lines.append(f"  {person:20} {total:6}  {detail}")
    return "\n".join(lines)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("live_json", type=Path, help="the live session's .aai.json")
    parser.add_argument("reference_md", type=Path, help="reference transcript (Teams export)")
    args = parser.parse_args(argv)

    try:
        live = json.loads(args.live_json.read_text())
        reference = parse_reference(args.reference_md.read_text())
    except (OSError, ValueError) as exc:
        print(f"could not read the inputs: {exc}", file=sys.stderr)
        return 2
    if not reference:
        print(
            f"{args.reference_md} has no lines shaped `**Name** (hh:mm:ss): text`",
            file=sys.stderr,
        )
        return 2

    print(format_report(compare(live, reference)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
