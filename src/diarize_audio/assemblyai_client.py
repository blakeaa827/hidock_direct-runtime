"""Thin wrapper over the AssemblyAI Python SDK.

Exposes a single `transcribe_file` that returns the raw `json_response` dict
(so downstream consumers are decoupled from the SDK's Transcript object).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import assemblyai as aai

log = logging.getLogger(__name__)


class TranscriptionError(RuntimeError):
    """Raised when AAI returns status=error or the job fails."""


# Map user-facing speech_model names to the AAI `speech_models` list API
# (the singular `speech_model` field was deprecated server-side in early 2026).
_SPEECH_MODELS_MAP = {
    "best": ["universal"],
    "nano": ["nano"],
}


class AAIClient:
    def __init__(
        self,
        *,
        api_key: str,
        speech_model: str,
        language_code: str | None,
        word_boost: list[str],
        timeout_minutes: int,
    ):
        aai.settings.api_key = api_key
        # auto_highlights disabled — AAI SDK 0.61.1 declares
        # AutohighlightResult.rank as required, but AAI's server omits it on
        # some highlights, killing the whole transcribe call via pydantic.
        # render.py only emits highlights as opaque YAML frontmatter, so we
        # lose nothing programmatic by disabling. See
        # bug_report_aai_highlight_rank_missing.md (2026-05-11).
        config_kwargs: dict[str, Any] = dict(
            speaker_labels=True,
            auto_highlights=False,
            punctuate=True,
            format_text=True,
            word_boost=list(word_boost),
        )
        if language_code:
            config_kwargs["language_code"] = language_code
            config_kwargs["language_detection"] = False
        else:
            config_kwargs["language_detection"] = True
        self._config = aai.TranscriptionConfig(**config_kwargs)
        # Switch off the deprecated `speech_model` field; use `speech_models`
        # list which the API now requires.
        self._config.raw.speech_model = None
        self._config.raw.speech_models = _SPEECH_MODELS_MAP.get(
            speech_model, _SPEECH_MODELS_MAP["best"]
        )
        self._timeout_seconds = timeout_minutes * 60
        self._transcriber = aai.Transcriber(config=self._config)

    def transcribe_file(self, wav_path: Path) -> dict[str, Any]:
        wav_path = Path(wav_path)
        log.info("aai_submit", extra={"file": wav_path.name})
        transcript = self._transcriber.transcribe(str(wav_path))
        status = _status_str(transcript.status)
        if status == "error":
            raise TranscriptionError(
                f"AssemblyAI returned status=error: {transcript.error or 'unknown error'}"
            )
        if status != "completed":
            raise TranscriptionError(
                f"AssemblyAI returned unexpected status={status!r}"
            )
        payload = transcript.json_response
        if not isinstance(payload, dict):
            raise TranscriptionError("AssemblyAI returned empty json_response")
        return payload


def _status_str(status: Any) -> str:
    if status is None:
        return ""
    if hasattr(status, "value"):
        return str(status.value)
    return str(status)
