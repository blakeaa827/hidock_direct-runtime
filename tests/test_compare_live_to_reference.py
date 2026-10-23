"""Tests for `scripts/compare_live_to_reference.py`, the Gate 4 measuring
instrument for live speaker attribution (live_speaker_revisions_sdk_upgrade_prd.md FR-3).

The fixtures are hand-authored from the spec, with every expected number worked
out by hand below. The script's PoL against the real 2026-10-23 session lives in
the 1j-b commit body, not here, because the real files are an operator's
conversation and do not belong in the repo.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "compare_live_to_reference.py"

# Three people on the far end, plus the operator. The live session:
#   turn 0  far-A  holds Ann's line AND Bob's reply: a MIXED turn
#   turn 1  near   the operator
#   turn 2  far-B  Cat
#   turn 3  far-C  Cat again, under a second label: Cat is SPLIT across B and C
REFERENCE = """\
**Ann Able** (00:00:01): good morning everyone shall we begin
**Bob Baker** (00:00:04): yes please
**Op Erator** (00:00:06): thanks all lets go round the room
**Cat Cole** (00:00:09): quick update the release shipped yesterday
**Cat Cole** (00:00:14): and the next one is on track for friday
"""
LIVE = {
    "id": "live-test",
    "utterances": [
        {"speaker": "far-A", "start": 0, "text": "Good morning everyone, shall we begin? Yes please."},
        {"speaker": "near", "start": 5000, "text": "Thanks all, let's go round the room."},
        {"speaker": "far-B", "start": 8000, "text": "Quick update: the release shipped yesterday."},
        {"speaker": "far-C", "start": 13000, "text": "And the next one is on track for Friday."},
    ],
}


@pytest.fixture
def mod(monkeypatch):
    spec = importlib.util.spec_from_file_location("compare_live_to_reference", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    # Registered first: `@dataclass` resolves the module's string annotations
    # through sys.modules, so an unregistered module fails at class creation.
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def files(tmp_path):
    live = tmp_path / "live.aai.json"
    live.write_text(json.dumps(LIVE))
    ref = tmp_path / "teams.md"
    ref.write_text(REFERENCE)
    return live, ref


def test_main_reports_and_exits_zero(mod, files, capsys):
    """Gate 1 step 20 smoke test: the real entry point, real file reads, the
    argument parser included.

    MUTATION: return 1 from main() after printing.
    """
    live, ref = files

    assert mod.main([str(live), str(ref)]) == 0
    out = capsys.readouterr().out
    assert "coverage          100%" in out
    assert "mixed far turns   1 of 3" in out


def test_a_turn_holding_two_people_is_counted_as_mixed(mod):
    """Turn 0 carries Ann's 6 words and Bob's 2; the other far turns are pure.

    MUTATION: count mixed turns over ALL turns, near included. The total becomes
    4 and this fails.
    MUTATION: test `len(people) > 0`. Every turn counts as mixed, giving 3.
    """
    report = mod.compare(LIVE, mod.parse_reference(REFERENCE))

    assert (report.mixed_far_turns, report.far_turns) == (1, 3)


def test_majority_correct_credits_each_label_only_its_majority_speaker(mod):
    """Far words: A = Ann 6 + Bob 2, B = Cat 6, C = Cat 9, so 23 in all. Majority
    credit is A 6 + B 6 + C 9 = 21. Bob's 2 words are the error.

    MUTATION: include `near` in the far totals. That gives 30 and 28.
    """
    report = mod.compare(LIVE, mod.parse_reference(REFERENCE))

    assert (report.far_majority_correct, report.far_words) == (21, 23)


def test_a_person_split_across_labels_shows_in_the_spread(mod):
    """Cat speaks under far-B AND far-C. This is the split the operator fixes by
    naming both rows the same person.

    MUTATION: build `spread` from the first label each person was seen on.
    """
    report = mod.compare(LIVE, mod.parse_reference(REFERENCE))

    assert dict(report.spread["Cat Cole"]) == {"far-B": 6, "far-C": 9}
    assert dict(report.purity["far-A"]) == {"Ann Able": 6, "Bob Baker": 2}


def test_a_reference_with_no_attributed_lines_is_refused(mod, tmp_path, capsys):
    """Without this, a wrongly exported reference reads as 0% everything. That
    looks like a catastrophically bad call, not a bad input.

    MUTATION: drop the empty-reference check. Then main returns 0.
    """
    live = tmp_path / "live.aai.json"
    live.write_text(json.dumps(LIVE))
    ref = tmp_path / "plain.txt"
    ref.write_text("just some text with no speaker attribution\n")

    assert mod.main([str(live), str(ref)]) == 2
    assert "**Name** (hh:mm:ss): text" in capsys.readouterr().err
