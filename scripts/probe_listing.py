#!/usr/bin/env python3
"""Probe what the HiDock exposes via `list_files` at a point in time.

Run this three times to settle the in-progress-visibility question:

  1. Before starting a recording  -- baseline file count + names.
  2. WHILE a recording is active  -- does the in-progress file appear?
  3. After stopping the recording -- does it appear now?

Usage:
    scripts/probe_listing.py

The script connects, calls `list_files`, prints a timestamped summary
(count, total bytes, names + sizes of the most recent 5 entries), and
disconnects. It also calls `get_file_count` first so we can see whether
the cheap count agrees with the full list.
"""

from __future__ import annotations

import sys
import time
from datetime import datetime
from pathlib import Path

# Reuse the runtime's iCloud .pth self-heal so editable imports work.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from hidock_direct import _pth_bootstrap  # noqa: E402

_pth_bootstrap.bootstrap()

from hidock_direct.device import JensenDeviceAdapter  # noqa: E402


def main() -> int:
    adapter = JensenDeviceAdapter()
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{ts}] connecting...")
    try:
        info = adapter.connect()
    except Exception as exc:  # noqa: BLE001
        print(f"[{ts}] connect failed: {exc}")
        return 1
    print(f"[{ts}] connected: {info.model} {info.serial}")

    # Cheap count first.
    jensen = adapter._jensen  # type: ignore[attr-defined]
    t0 = time.time()
    count_result = jensen.get_file_count()
    t_count = time.time() - t0
    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] get_file_count: {count_result} ({t_count:.2f}s)")

    # Full list.
    t0 = time.time()
    files = adapter.list_files()
    t_list = time.time() - t0
    total_bytes = sum(f.size for f in files)
    print(
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] list_files: "
        f"{len(files)} files, {total_bytes} bytes total ({t_list:.2f}s)"
    )
    # Show the 5 entries with the newest device_mtime (chronological,
    # robust against firmware-variant naming schemes).
    def _sort_key(f):
        return f.device_mtime or type(f.device_mtime)(1, 1, 1) if f.device_mtime else None
    with_mtime = [f for f in files if f.device_mtime is not None]
    without_mtime = [f for f in files if f.device_mtime is None]
    for f in sorted(with_mtime, key=lambda x: x.device_mtime)[-5:]:
        print(f"    {f.name}  size={f.size}  mtime={f.device_mtime.isoformat()}")
    if without_mtime:
        print(f"  ({len(without_mtime)} files without device_mtime):")
        for f in without_mtime[-5:]:
            print(f"    {f.name}  size={f.size}  mtime=None")

    adapter.disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(main())
