#!/usr/bin/env bash
# bootstrap.sh — create (or refresh) the local venv.
#
# The repo is Syncthing-synced. Venvs are machine-local and excluded from sync
# via .syncthing-ignores, so a plain .venv name is used on every machine.

set -euo pipefail

RUNTIME_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$RUNTIME_ROOT"

VENV=".venv"

if ! command -v brew >/dev/null 2>&1; then
  echo "WARN: Homebrew not found. libusb must be installed another way." >&2
else
  if ! brew list --versions libusb >/dev/null 2>&1; then
    echo "==> Installing libusb via Homebrew (required for pyusb)"
    brew install libusb
  fi
fi

if [ ! -d "$VENV" ]; then
  echo "==> Creating $VENV"
  python3 -m venv "$VENV"
else
  echo "==> Using existing $VENV"
fi

echo "==> Installing dependencies"
"$VENV/bin/pip" install --quiet --upgrade pip >/dev/null
"$VENV/bin/pip" install --quiet -e ".[test]"

DIARIZE_RUNTIME="$(dirname "$RUNTIME_ROOT")/diarize_audio-runtime"
if [ -d "$DIARIZE_RUNTIME" ]; then
  echo "==> Installing diarize_audio (sibling runtime)"
  "$VENV/bin/pip" install --quiet -e "$DIARIZE_RUNTIME"
else
  echo "WARN: diarize_audio-runtime not found at $DIARIZE_RUNTIME" >&2
  echo "      Transcription will be disabled until it is installed." >&2
fi

echo "==> Verifying imports"
"$VENV/bin/python3" -c 'import hidock_direct; print(f"ok: hidock_direct {hidock_direct.__version__}")'
"$VENV/bin/python3" -c 'import diarize_audio; print(f"ok: diarize_audio")' 2>/dev/null \
  || echo "WARN: diarize_audio not importable (transcription will be skipped)"

echo ""
echo "Bootstrap complete."
echo "  Venv:  $RUNTIME_ROOT/$VENV"
echo "  Tests: $VENV/bin/pytest tests/ -v"
echo "  Run:   $VENV/bin/python -m hidock_direct"
