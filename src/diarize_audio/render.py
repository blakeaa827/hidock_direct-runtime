"""Render an AssemblyAI transcript dict into Markdown per PRD §2.4.

Consumes the raw JSON dict produced by the AAI SDK (`transcript.json_response`)
rather than the SDK `Transcript` object so render is independently testable
and immune to SDK version drift.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any


MAX_HIGHLIGHTS = 5


def render_markdown(
    transcript: dict[str, Any],
    *,
    source_filename: str,
    recorded_at: datetime,
) -> str:
    """Render a transcript dict to the PRD §2.4 markdown format.

    Arguments:
        transcript: raw AAI response dict (matches types.TranscriptResponse schema).
        source_filename: the original `.wav` filename (used verbatim in frontmatter).
        recorded_at: timezone-aware datetime (source file mtime).

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
    speaker_map: dict[str, int] = {}
    for u in utterances:
        raw_speaker = u.get("speaker") or "?"
        if raw_speaker not in speaker_map:
            speaker_map[raw_speaker] = len(speaker_map) + 1
        num = speaker_map[raw_speaker]
        start_ms = int(u.get("start") or 0)
        mm, ss = divmod(start_ms // 1000, 60)
        text = (u.get("text") or "").strip()
        lines.append(f"**Speaker {num}** ({mm:02d}:{ss:02d}): {text}")
        lines.append("")
    body = "\n".join(lines)
    # Ensure single trailing newline.
    return body.rstrip("\n") + "\n"


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
