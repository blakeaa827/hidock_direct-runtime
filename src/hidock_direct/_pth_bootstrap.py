"""Self-heal editable installs that iCloud has flagged `UF_HIDDEN`.

iCloud aggressively applies `UF_HIDDEN` to files inside venvs sitting in
iCloud-synced directories. Python 3.8+ `site.py` skips hidden `.pth`
files on macOS (it literally logs "Skipping hidden .pth file"), which
makes every editable install silently un-importable. Once `site.py` has
already run for this process, clearing the flag doesn't help — the
paths are already missing from `sys.path`.

This module runs at `__main__.py` startup, BEFORE any editable-installed
dependency is imported. It:
  1. Clears `UF_HIDDEN` on `__editable__*.pth` in each site-packages dir
     (so future interpreter launches succeed via the normal path), and
  2. Injects the referenced source directories into `sys.path` for the
     current process.

The hidden flag does not prevent `open()` from reading the file — only
`site.py`'s macOS-specific filter acts on it. Reading the `.pth`
ourselves works regardless.
"""

from __future__ import annotations

import os
import site
import stat
import sys
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

_EDITABLE_PREFIX = "__editable__"
_PTH_SUFFIX = ".pth"


def _default_site_packages() -> List[Path]:
    dirs: List[Path] = []
    for raw in site.getsitepackages():
        p = Path(raw)
        if p.is_dir():
            dirs.append(p)
    user = site.getusersitepackages()
    if user:
        up = Path(user)
        if up.is_dir():
            dirs.append(up)
    return dirs


def _clear_uf_hidden(path: Path) -> None:
    """Best-effort: drop the UF_HIDDEN flag if supported on this platform."""
    if not hasattr(os, "chflags"):
        return
    try:
        current = path.stat().st_flags  # type: ignore[attr-defined]
    except (AttributeError, OSError):
        return
    if current & stat.UF_HIDDEN:
        try:
            os.chflags(path, current & ~stat.UF_HIDDEN)
        except OSError:
            pass


def _paths_from_pth(pth: Path) -> List[str]:
    """Parse an `__editable__*.pth` and return the referenced absolute paths.

    `site.py` supports `import`-style lines for complex editable layouts;
    we only rescue the common case: one or more bare directory paths.
    That's what `pip install -e` emits for flat src-layout packages,
    which is what this runtime and its sibling editable deps use.
    """
    out: List[str] = []
    try:
        content = pth.read_text()
    except OSError:
        return out
    for raw in content.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith(("import ", "import\t")):
            continue  # leave import-hook style to site.py
        # Relative paths in .pth are relative to the .pth's own directory.
        p = Path(line)
        if not p.is_absolute():
            p = (pth.parent / p).resolve()
        out.append(str(p))
    return out


def bootstrap(site_dirs: Optional[Sequence[Path]] = None) -> List[str]:
    """Re-inject editable-install paths into `sys.path`.

    Returns the list of paths newly injected. Idempotent — calling
    twice does not duplicate sys.path entries.
    """
    if site_dirs is None:
        site_dirs = _default_site_packages()

    injected: List[str] = []
    seen_in_sys_path = set(sys.path)

    for site_dir in site_dirs:
        if not site_dir.is_dir():
            continue
        try:
            entries: Iterable[Path] = sorted(site_dir.iterdir())
        except OSError:
            continue
        for entry in entries:
            name = entry.name
            if not name.startswith(_EDITABLE_PREFIX) or not name.endswith(_PTH_SUFFIX):
                continue
            _clear_uf_hidden(entry)
            for target in _paths_from_pth(entry):
                if target in seen_in_sys_path:
                    continue
                sys.path.append(target)
                seen_in_sys_path.add(target)
                injected.append(target)
    return injected
