"""Regression test for kernel-hostname DHCP-hijack venv-resolution bug.

Origin: 2026-05-11 — `~/.zshrc` hidock alias resolved `$(hostname -s)` to
`Wades-iPhone` (DHCP rDNS overwrote the kernel hostname) and produced
`zsh: no such file or directory: .../.venv-Wades-iPhone/bin/python`. The
kernel hostname is DHCP-hijackable on macOS; `scutil --get LocalHostName`
is owned by macOS's preferences subsystem and DHCP cannot overwrite it.

These tests pin both shell scripts (`bootstrap.sh`, `manual_verify.sh`) to
the durable anchor and explicitly refuse the DHCP-vulnerable one.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

RUNTIME_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = RUNTIME_ROOT / "scripts"

ANCHORED_SCRIPTS = [
    "bootstrap.sh",
    "manual_verify.sh",
]

VULNERABLE_PATTERN = re.compile(r"\$\(\s*hostname\s*(?:-s)?\s*\)")
DURABLE_PATTERN = re.compile(r"\$\(\s*scutil\s+--get\s+LocalHostName\s*\)")


@pytest.mark.parametrize("script_name", ANCHORED_SCRIPTS)
def test_script_does_not_anchor_on_kernel_hostname(script_name: str) -> None:
    script = SCRIPTS_DIR / script_name
    assert script.exists(), f"expected {script} to exist"
    body = script.read_text()
    matches = VULNERABLE_PATTERN.findall(body)
    assert not matches, (
        f"{script_name} anchors venv resolution on the kernel hostname "
        f"(`$(hostname -s)` or `$(hostname)`), which DHCP can overwrite "
        f"(observed 2026-05-11: kernel hostname → Wades-iPhone). "
        f"Use `$(scutil --get LocalHostName)` instead. Matches: {matches}"
    )


@pytest.mark.parametrize("script_name", ANCHORED_SCRIPTS)
def test_script_anchors_on_localhostname(script_name: str) -> None:
    script = SCRIPTS_DIR / script_name
    assert script.exists(), f"expected {script} to exist"
    body = script.read_text()
    assert DURABLE_PATTERN.search(body), (
        f"{script_name} must anchor venv resolution on "
        f"`$(scutil --get LocalHostName)`. `LocalHostName` is owned by "
        f"macOS preferences and is not DHCP-hijackable."
    )
