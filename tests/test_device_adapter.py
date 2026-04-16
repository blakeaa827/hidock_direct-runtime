"""PRD §7 path-sanitization guarantees."""

from __future__ import annotations

import pytest

from hidock_direct.device import model_from_pid, sanitize_device_filename


def test_sanitize_accepts_safe_names():
    assert sanitize_device_filename("REC_0001.wav") == "REC_0001.wav"
    assert sanitize_device_filename("20260412-091500.hda") == "20260412-091500.hda"


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "../escape.wav",
        "rec;shell.wav",
        "rec/file.wav",
        "rec file.wav",
        "rec\x00null.wav",
        "rec$var.wav",
        "`cmd`.wav",
    ],
)
def test_sanitize_rejects_unsafe_names(bad: str):
    with pytest.raises(ValueError):
        sanitize_device_filename(bad)


def test_model_from_pid_known_and_unknown():
    assert model_from_pid(0xAF0C) == "hidock-h1"
    assert model_from_pid(0xB00D) == "hidock-h1e"
    assert model_from_pid(0x0000).startswith("hidock-unknown")
