"""Regression tests for venv path conventions in shell scripts.

History:
  2026-05-11 — `~/.zshrc` hidock alias resolved `$(hostname -s)` to
  `Wades-iPhone` (DHCP rDNS overwrote the kernel hostname), producing
  `zsh: no such file or directory: .../.venv-Wades-iPhone/bin/python`.
  Fix was `scutil --get LocalHostName` (DHCP-resistant macOS preferences).

  2026-05-26 — Forge relocated from iCloud to Syncthing. Since venvs are
  machine-local and excluded from sync via `.syncthing-ignores`, hostname
  suffixes are no longer needed. Scripts now use a plain `.venv` path.
  No hostname expansion of any kind should appear.
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

KERNEL_HOSTNAME_PATTERN = re.compile(r"\$\(\s*hostname\s*(?:-s)?\s*\)")
SCUTIL_HOSTNAME_PATTERN = re.compile(r"\$\(\s*scutil\s+--get\s+LocalHostName\s*\)")
PLAIN_VENV_PATTERN = re.compile(r'^VENV=".venv"', re.MULTILINE)


@pytest.mark.parametrize("script_name", ANCHORED_SCRIPTS)
def test_script_does_not_anchor_on_kernel_hostname(script_name: str) -> None:
    script = SCRIPTS_DIR / script_name
    assert script.exists(), f"expected {script} to exist"
    body = script.read_text()
    matches = KERNEL_HOSTNAME_PATTERN.findall(body)
    assert not matches, (
        f"{script_name} uses kernel hostname (`$(hostname -s)` or `$(hostname)`), "
        f"which DHCP can overwrite (observed 2026-05-11: → Wades-iPhone). "
        f"Use a plain VENV='.venv' instead. Matches: {matches}"
    )


@pytest.mark.parametrize("script_name", ANCHORED_SCRIPTS)
def test_script_does_not_use_scutil_hostname(script_name: str) -> None:
    script = SCRIPTS_DIR / script_name
    assert script.exists(), f"expected {script} to exist"
    body = script.read_text()
    assert not SCUTIL_HOSTNAME_PATTERN.search(body), (
        f"{script_name} still uses `$(scutil --get LocalHostName)` for venv naming. "
        f"Since relocating to Syncthing (2026-05-26), venvs are machine-local and "
        f"sync-excluded — use plain VENV='.venv' with no hostname suffix."
    )


@pytest.mark.parametrize("script_name", ANCHORED_SCRIPTS)
def test_script_uses_plain_venv_path(script_name: str) -> None:
    script = SCRIPTS_DIR / script_name
    assert script.exists(), f"expected {script} to exist"
    body = script.read_text()
    assert PLAIN_VENV_PATTERN.search(body), (
        f"{script_name} must set VENV='.venv' (plain, no hostname suffix). "
        f"Venvs are machine-local and excluded from Syncthing sync."
    )
