#!/usr/bin/env bash
# bootstrap.sh — create (or refresh) this machine's venv inside the iCloud repo.
#
# The repo is iCloud-synced so source travels between machines, but each machine
# needs its own venv (different architectures, Python versions, binary wheels).
# We solve this by naming venvs .venv-<hostname>/.
#
# iCloud gotcha: files in a Mobile Documents container are automatically marked
# UF_HIDDEN, which makes Python's site.py silently skip any .pth file inside the
# venv's site-packages. Without this script's chflags fix, `pip install -e .`
# appears to succeed but the package is never importable. We unhide every .pth
# after install.

set -euo pipefail

RUNTIME_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$RUNTIME_ROOT"

HOST="$(hostname -s)"
VENV=".venv-${HOST}"

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

echo "==> Unhiding .pth files (iCloud UF_HIDDEN workaround)"
find "$VENV/lib" -name "*.pth" -exec chflags nohidden {} +

echo "==> Verifying import"
"$VENV/bin/python3" -c 'import hidock_direct; print(f"ok: hidock_direct {hidock_direct.__version__}")'

echo ""
echo "Bootstrap complete."
echo "  Venv:  $RUNTIME_ROOT/$VENV"
echo "  Tests: $VENV/bin/pytest tests/ -v"
echo "  Run:   $VENV/bin/python -m hidock_direct"
