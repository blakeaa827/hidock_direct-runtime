#!/usr/bin/env python3
"""Read one recording off the HiDock and report what its bytes actually are.

Read-only: streams the file into memory, writes nothing to disk, touches no
ledger, deletes nothing. Quit hidock-direct first -- only one process can hold
the USB interface.

Usage:
    .venv/bin/python scripts/probe_header.py 2026May14-114525-Rec25.hda

Reports the size, the first 64 bytes, what `_detect_real_extension` makes of
them, and the offset of the first valid MPEG audio frame header anywhere in
the first 64 KB -- which separates "not MP3 at all" from "MP3 with junk in
front of the first frame".
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from hidock_direct import _pth_bootstrap  # noqa: E402

_pth_bootstrap.bootstrap()

from hidock_direct.device import JensenDeviceAdapter  # noqa: E402
from hidock_direct.offload import _detect_real_extension, _is_mpeg_audio_frame_header  # noqa: E402


def _first_frame_offset(data: bytes, limit: int = 65536) -> int | None:
    """First offset with a valid header whose NEXT header is also valid
    `frame_len` bytes later would be stricter; a single valid header is
    enough to distinguish the cases this probe exists for."""
    for i in range(min(len(data) - 3, limit)):
        if _is_mpeg_audio_frame_header(data[i:i + 4]):
            return i
    return None


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    name = sys.argv[1]
    adapter = JensenDeviceAdapter()
    info = adapter.connect()
    try:
        print(f"connected: {info.model} {info.serial}")
        match = [f for f in adapter.list_files() if f.name == name]
        if not match:
            print(f"{name}: not on the device")
            return 1
        f = match[0]
        buf = bytearray()
        adapter.download_file(f.name, f.size, on_chunk=buf.extend)
    finally:
        adapter.disconnect()

    data = bytes(buf)
    print(f"size      : {len(data)} bytes (device reported {f.size})")
    for row in range(0, min(64, len(data)), 16):
        chunk = data[row:row + 16]
        print(f"  {row:04x}  {chunk.hex(' '):<47}  {''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)}")
    with tempfile.NamedTemporaryFile(suffix=".bin") as t:   # deleted on close
        t.write(data[:65536]); t.flush()
        print(f"sniff     : {_detect_real_extension(Path(t.name))}")
    off = _first_frame_offset(data)
    print(f"1st frame : {'none in first 64 KB' if off is None else f'offset {off} -> ' + data[off:off+4].hex()}")
    print(f"all zero  : {not any(data)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
