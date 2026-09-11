"""Render an AssemblyAI transcript dict into Markdown per PRD §2.4.

Consumes the raw JSON dict produced by the AAI SDK (`transcript.json_response`)
rather than the SDK `Transcript` object so render is independently testable
and immune to SDK version drift.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import datetime
from typing import Any


MAX_HIGHLIGHTS = 5


def render_markdown(
    transcript: dict[str, Any],
    *,
    source_filename: str,
    recorded_at: datetime,
    speaker_names: Mapping[str, str] | None = None,
) -> str:
    """Render a transcript dict to the PRD §2.4 markdown format.

    Arguments:
        transcript: raw AAI response dict (matches types.TranscriptResponse schema).
        source_filename: the original `.wav` filename (used verbatim in frontmatter).
        recorded_at: timezone-aware datetime (source file mtime).
        speaker_names: optional map from the RAW provider speaker key (the value
            in `utterances[].speaker`, e.g. `"A"`) to a display name. A key that
            has a name renders `**<name>**`; every other speaker keeps the
            existing `**Speaker {num}**` label. Numbering is computed over ALL
            speakers in first-appearance order exactly as it is without this
            argument, so naming one speaker never renumbers another -- a number
            means the same thing whether or not its neighbours are named.
            Names are untrusted operator input and are sanitised
            (`_sanitize_speaker_name`) before they reach the document.
            `None` (the default), an empty map, and a map whose keys match no
            speaker all produce byte-identical output to omitting the argument.

    Returns:
        Full markdown document with trailing newline.
    """
    duration_seconds = int(transcript.get("audio_duration") or 0)
    audio_duration_minutes = round(duration_seconds / 60.0, 2)
    utterances = transcript.get("utterances") or []
    speakers = {u.get("speaker") for u in utterances if u.get("speaker")}
    speaker_count = len(speakers)
    highlights = _top_highlights(transcript.get("auto_highlights_result"))

    heading_dt = recorded_at.strftime("%Y-%m-%d %H:%M")
    recorded_iso = recorded_at.isoformat(timespec="seconds")

    lines: list[str] = ["---"]
    lines.append(f"recorded_at: {recorded_iso}")
    lines.append(f"duration_seconds: {duration_seconds}")
    lines.append(f"audio_duration_minutes: {audio_duration_minutes:.2f}")
    lines.append(f"source_filename: {source_filename}")
    lines.append(f"assemblyai_id: {transcript.get('id') or ''}")
    lines.append(f"language_code: {_yaml_scalar(transcript.get('language_code'))}")
    lines.append(f"speaker_count: {speaker_count}")
    if highlights:
        lines.append("auto_highlights:")
        for h in highlights:
            lines.append(f"  - {_yaml_quoted(h)}")
    else:
        lines.append("auto_highlights: []")
    lines.append("---")
    lines.append("")
    lines.append(f"# {heading_dt}")
    lines.append("")
    labels = speaker_labels(transcript, speaker_names)
    for u in utterances:
        raw_speaker = u.get("speaker") or "?"
        label = labels[raw_speaker]
        start_ms = int(u.get("start") or 0)
        mm, ss = divmod(start_ms // 1000, 60)
        text = (u.get("text") or "").strip()
        lines.append(f"**{label}** ({mm:02d}:{ss:02d}): {text}")
        lines.append("")
    body = "\n".join(lines)
    # Ensure single trailing newline.
    return body.rstrip("\n") + "\n"


def speaker_labels(
    transcript: dict[str, Any],
    speaker_names: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """The `raw provider key -> bold label text` map this document renders with.

    Extracted so a caller that must find a label it already emitted — to
    substitute a name into a document without re-rendering it — asks the
    renderer what it wrote rather than reconstructing the rule. Two matching
    expressions in two files is the arrangement that goes stale silently, and
    the consequence here is a substitution anchored on a label the document
    does not contain.

    Numbering is first-appearance order over ALL speakers and is independent of
    `speaker_names`, so naming one speaker never renumbers another.

    Returns the label TEXT, without the surrounding `**`: the emphasis belongs
    to the line format, which is this module's business and not the caller's.
    """
    labels: dict[str, str] = {}
    for u in transcript.get("utterances") or []:
        raw_speaker = u.get("speaker") or "?"
        if raw_speaker not in labels:
            labels[raw_speaker] = _speaker_label(
                raw_speaker, len(labels) + 1, speaker_names
            )
    return labels


# Characters that would end the current line. A name carrying one of these could
# open a new document line and forge a frontmatter fence, a heading, or an extra
# speaker turn, so they are folded to a space rather than escaped.
_LINE_BREAKING = re.compile("[\r\n\v\f\x1c-\x1e\x85\u2028\u2029]")
# Remaining C0/C1 controls carry no display meaning; drop them outright.
_CONTROL = re.compile(r"[\x00-\x08\x0e-\x1f\x7f-\x9f]")
_WHITESPACE_RUN = re.compile(r"\s+")
# Inline Markdown structure that would otherwise escape the `**...**` label and
# reflow the rest of the line. Backslash is in the set and the translation is a
# single pass, so an escape introduced here is never itself re-escaped.
_MD_STRUCTURAL = str.maketrans({ch: "\\" + ch for ch in "\\*_`[]<>|~"})


def _sanitize_speaker_name(name: str) -> str:
    """Make an untrusted operator-supplied name safe to inline in the document.

    The name is written into a Markdown body that sits under YAML frontmatter,
    so the invariant is structural: whatever comes back must occupy exactly one
    line and must not introduce Markdown structure. Returns `""` when nothing
    printable survives, which the caller treats as "unnamed".
    """
    flattened = _LINE_BREAKING.sub(" ", name)
    flattened = _CONTROL.sub("", flattened)
    flattened = _WHITESPACE_RUN.sub(" ", flattened).strip()
    return flattened.translate(_MD_STRUCTURAL)


def _speaker_label(
    raw_speaker: str,
    num: int,
    speaker_names: Mapping[str, str] | None,
) -> str:
    """The bold label for one turn: the operator's name, else `Speaker {num}`.

    `num` is the unchanged first-appearance number, so falling back here yields
    exactly the label this renderer emitted before names existed.
    """
    if speaker_names:
        name = speaker_names.get(raw_speaker)
        if name is not None:
            safe = _sanitize_speaker_name(name if isinstance(name, str) else str(name))
            if safe:
                return safe
    return f"Speaker {num}"


def _top_highlights(result: Any) -> list[str]:
    if not isinstance(result, dict):
        return []
    items = result.get("results") or []
    # Sort by rank desc, preserving original order on ties (Python's sort is stable).
    ranked = sorted(
        enumerate(items),
        key=lambda pair: (-float(pair[1].get("rank", 0.0)), pair[0]),
    )
    out: list[str] = []
    for _idx, h in ranked[:MAX_HIGHLIGHTS]:
        text = h.get("text")
        if text:
            out.append(text)
    return out


def _yaml_scalar(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, str):
        return value
    return str(value)


def _yaml_quoted(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'
