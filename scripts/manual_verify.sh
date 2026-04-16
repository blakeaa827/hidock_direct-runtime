#!/usr/bin/env bash
# manual_verify.sh — Gate 4c hardware verification procedure for hidock-direct.
#
# Unit tests exercise every code path against a mock device (see tests/). This
# script is the only way to exercise the real USB stack: pyusb + libusb + the
# vendored Jensen protocol against a physical HiDock. No CI path runs it.
#
# Run interactively with a real HiDock plugged in. Each step pauses for the
# operator to confirm visually.

set -euo pipefail

RUNTIME_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$RUNTIME_ROOT"

HOST="$(hostname -s)"
VENV=".venv-${HOST}"

if [ ! -d "$VENV" ]; then
  echo "No venv at $VENV. Run scripts/bootstrap.sh first." >&2
  exit 1
fi

ARCHIVE_DIR="${HIDOCK_ARCHIVE_DIR:-$HOME/HiDock/archive}"

confirm() {
  read -r -p "  [?] $1  [enter to continue, ^C to abort] " _
}

cat <<'EOF'
==============================================================
 HiDock Direct — manual hardware verification (Gate 4c)
==============================================================

Prereqs:
  • libusb installed (brew install libusb)
  • HiDock device NOT yet plugged in
  • No other hidock-direct instance running

EOF

echo "1. Archive directory: $ARCHIVE_DIR"
mkdir -p "$ARCHIVE_DIR"
echo "   ($ARCHIVE_DIR) ready."

echo ""
echo "2. Launch the TUI. Plug in the HiDock when prompted."
echo "   Expected: 'Waiting for device…' until plug, then 'Connected: hidock-h1 (SN…)'."
confirm "Ready to launch?"

HIDOCK_ARCHIVE_DIR="$ARCHIVE_DIR" "$VENV/bin/python" -m hidock_direct &
APP_PID=$!
trap 'kill $APP_PID 2>/dev/null || true' EXIT

echo ""
echo "3. Plug the HiDock into a USB-C port now."
confirm "After plug, does the TUI header flip to 'Connected' with the model/serial?"

echo ""
echo "4. On the HiDock, record a short (~10 second) test message, then stop."
confirm "Within ~10 seconds, does a new .wav appear in $ARCHIVE_DIR/YYYY/MM/?"

echo ""
echo "5. Inspect the newest .wav."
LATEST="$(find "$ARCHIVE_DIR" -type f -name '*.wav' -print0 2>/dev/null | xargs -0 ls -1t 2>/dev/null | head -1 || true)"
echo "   Newest: ${LATEST:-<none yet>}"
if [ -n "$LATEST" ]; then
  echo "   Size:   $(stat -f %z "$LATEST") bytes"
  echo "   SHA256: $(shasum -a 256 "$LATEST" | awk '{print $1}')"
  confirm "Open the file (QuickTime / afplay \"$LATEST\") and verify audio plays correctly."
fi

echo ""
echo "6. Inspect offload_state.json — the file should be listed under devices.<serial>.processed_recordings."
echo "   Path: $ARCHIVE_DIR/.state/offload_state.json"
confirm "Does the ledger include this recording?"

echo ""
echo "7. Unplug the HiDock."
confirm "Does the TUI flip back to 'Waiting for device…' within ~1 second?"

echo ""
echo "8. Replug."
confirm "Does the TUI reconnect, scan, and NOT re-download the already-archived file?"

echo ""
echo "9. Quit with Ctrl-C."
confirm "Does the TUI shut down cleanly (lock file released; no traceback)?"

kill "$APP_PID" 2>/dev/null || true

cat <<'EOF'

==============================================================
 Manual verification complete.
==============================================================

If any step failed, investigate before shipping. Record failures + root cause
in the project's session log (projects/hidock_direct/SESSION.md) and, for
reproducible issues, as a new ticket or directive learning.
EOF
