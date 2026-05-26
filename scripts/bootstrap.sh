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

echo "==> Verifying import"
"$VENV/bin/python3" -c 'import hidock_direct; print(f"ok: hidock_direct {hidock_direct.__version__}")'

echo ""
echo "Bootstrap complete."
echo "  Venv:  $RUNTIME_ROOT/$VENV"
echo "  Tests: $VENV/bin/pytest tests/ -v"
echo "  Run:   $VENV/bin/python -m hidock_direct"
