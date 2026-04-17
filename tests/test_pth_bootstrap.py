"""Regression tests for the editable-`.pth` self-heal used to survive iCloud.

iCloud sets `UF_HIDDEN` on files inside synced venvs. Python's `site.py`
logs `Skipping hidden .pth file` and drops the path. The bootstrap module
re-injects those paths into `sys.path` regardless of the hidden flag, so
the runtime survives iCloud's periodic sweeps.
"""

from __future__ import annotations

import stat
import sys
from pathlib import Path

import pytest

from hidock_direct import _pth_bootstrap


def _write_pth(site_packages: Path, name: str, target: Path) -> Path:
    pth = site_packages / name
    pth.write_text(str(target) + "\n")
    return pth


def test_reads_editable_pth_and_injects_paths(tmp_path, monkeypatch):
    site_pkgs = tmp_path / "site-packages"
    site_pkgs.mkdir()
    target = tmp_path / "fake_pkg_src"
    target.mkdir()
    _write_pth(site_pkgs, "__editable__.fake_pkg-1.0.0.pth", target)

    # Clean slate: make sure target isn't in sys.path via some other route.
    monkeypatch.setattr(sys, "path", [p for p in sys.path if str(target) not in p])
    assert str(target) not in sys.path

    injected = _pth_bootstrap.bootstrap([site_pkgs])

    assert str(target) in sys.path
    assert str(target) in injected


def test_reads_pth_even_when_uf_hidden_is_set(tmp_path, monkeypatch):
    """The whole point: UF_HIDDEN must not stop us from reading and injecting.

    `site.py` bails on hidden files; we bail only on missing files.
    """
    if sys.platform != "darwin":
        pytest.skip("UF_HIDDEN is macOS-only")

    import os

    site_pkgs = tmp_path / "site-packages"
    site_pkgs.mkdir()
    target = tmp_path / "fake_pkg_src"
    target.mkdir()
    pth = _write_pth(site_pkgs, "__editable__.fake_pkg-1.0.0.pth", target)
    # Flip UF_HIDDEN the way iCloud does.
    os.chflags(pth, stat.UF_HIDDEN)

    monkeypatch.setattr(sys, "path", [p for p in sys.path if str(target) not in p])

    injected = _pth_bootstrap.bootstrap([site_pkgs])

    assert str(target) in sys.path
    assert str(target) in injected

    # Bootstrap should also proactively clear the flag so subsequent
    # interpreters (fresh processes) get the paths via normal site.py.
    new_flags = pth.stat().st_flags
    assert not (new_flags & stat.UF_HIDDEN), (
        f"UF_HIDDEN still set on {pth}; bootstrap must clear it"
    )


def test_ignores_non_editable_pth(tmp_path, monkeypatch):
    site_pkgs = tmp_path / "site-packages"
    site_pkgs.mkdir()
    target = tmp_path / "other_src"
    target.mkdir()
    _write_pth(site_pkgs, "a1_coverage.pth", target)

    monkeypatch.setattr(sys, "path", [p for p in sys.path if str(target) not in p])

    injected = _pth_bootstrap.bootstrap([site_pkgs])

    # Coverage/other pths are site.py's job; we only rescue __editable__*.
    assert str(target) not in injected


def test_is_idempotent(tmp_path, monkeypatch):
    site_pkgs = tmp_path / "site-packages"
    site_pkgs.mkdir()
    target = tmp_path / "pkg"
    target.mkdir()
    _write_pth(site_pkgs, "__editable__.pkg-1.0.0.pth", target)

    monkeypatch.setattr(sys, "path", [p for p in sys.path if str(target) not in p])

    _pth_bootstrap.bootstrap([site_pkgs])
    _pth_bootstrap.bootstrap([site_pkgs])

    # Should appear exactly once in sys.path, not twice.
    assert sys.path.count(str(target)) == 1
