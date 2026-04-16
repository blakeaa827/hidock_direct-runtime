"""Vendored HiDock Jensen USB protocol from sgeraldes/hidock-next.

See VENDORED_COMMIT for the pinned upstream SHA and README.md for what was
modified. Downstream code should import from this package's public names
only; do not import from the submodule internals.

The vendored `config_and_logger` module prints INFO/ERROR lines to stdout at
import time (it probes a config file and a log-file path that don't exist
in our layout). We redirect stdout/stderr to a bit-bucket across the first
import to keep the TUI clean. Subsequent logger output from the vendored
code still lands on stdout — when Jensen is talking to real hardware we
want to see that — we're only silencing the boot-time noise.
"""

import os as _os
import sys as _sys
import contextlib as _contextlib

_devnull = open(_os.devnull, "w", encoding="utf-8")
with _contextlib.redirect_stdout(_devnull), _contextlib.redirect_stderr(_devnull):
    from .hidock_device import HiDockJensen
    from .hta_converter import HTAConverter
    from .constants import (
        ALL_VENDOR_IDS,
        ALTERNATE_VENDOR_ID,
        DEFAULT_PRODUCT_ID,
        DEFAULT_VENDOR_ID,
        HIDOCK_PRODUCT_IDS,
        PRODUCT_ID_MODEL_MAP,
    )
_devnull.close()
del _devnull, _os, _sys, _contextlib

__all__ = [
    "HiDockJensen",
    "HTAConverter",
    "ALL_VENDOR_IDS",
    "ALTERNATE_VENDOR_ID",
    "DEFAULT_PRODUCT_ID",
    "DEFAULT_VENDOR_ID",
    "HIDOCK_PRODUCT_IDS",
    "PRODUCT_ID_MODEL_MAP",
]
