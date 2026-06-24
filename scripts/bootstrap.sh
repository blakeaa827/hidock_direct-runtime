#!/usr/bin/env bash
# bootstrap.sh — create (or refresh) the local venv and install HiDock Direct.
#
# Single-clone: diarize_audio is VENDORED under src/diarize_audio/ and installs
# transitively with this package — no sibling repo needed. The repo is
# Syncthing-synced; venvs are machine-local (.gitignored), so a plain .venv name
# is used on every machine.

set -euo pipefail

RUNTIME_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$RUNTIME_ROOT"

VENV=".venv"

# --- Python 3.11+ interpreter ----------------------------------------------
PYTHON=""
for cand in python3.13 python3.12 python3.11 python3; do
  if command -v "$cand" >/dev/null 2>&1; then
    ver="$("$cand" -c 'import sys; print("%d.%d" % sys.version_info[:2])' 2>/dev/null || echo 0.0)"
    major="${ver%%.*}"; minor="${ver#*.}"
    if [ "$major" = "3" ] && [ "$minor" -ge 11 ] 2>/dev/null; then PYTHON="$cand"; break; fi
  fi
done
if [ -z "$PYTHON" ]; then
  echo "ERROR: HiDock Direct needs Python >=3.11, none found on PATH." >&2
  echo "       Install one (e.g. brew install python@3.12) and re-run." >&2
  exit 1
fi
echo "==> Using $("$PYTHON" --version) ($PYTHON)"

# --- libusb (required by pyusb for USB offload) ----------------------------
if ! command -v brew >/dev/null 2>&1; then
  echo "WARN: Homebrew not found. libusb must be installed another way." >&2
else
  if ! brew list --versions libusb >/dev/null 2>&1; then
    echo "==> Installing libusb via Homebrew (required for pyusb)"
    brew install libusb
  fi
fi

# --- venv ------------------------------------------------------------------
if [ ! -d "$VENV" ]; then
  echo "==> Creating $VENV"
  "$PYTHON" -m venv "$VENV"
else
  echo "==> Using existing $VENV"
fi

# Clear any stale editable diarize-audio install from the pre-vendoring layout
# so it can't shadow the now-vendored src/diarize_audio/ (no-op on a fresh clone).
"$VENV/bin/pip" uninstall -y diarize-audio >/dev/null 2>&1 || true

echo "==> Installing HiDock Direct (vendored diarize_audio installs with it)"
"$VENV/bin/pip" install --quiet --upgrade pip >/dev/null
"$VENV/bin/pip" install --quiet -e ".[test]"

echo "==> Verifying imports"
"$VENV/bin/python" -c 'import hidock_direct; print(f"ok: hidock_direct {hidock_direct.__version__}")'
# diarize_audio is vendored — it MUST import. A failure here is a real install
# problem, not an optional-transcription skip.
"$VENV/bin/python" -c 'import diarize_audio; print("ok: diarize_audio (vendored)")'

echo ""
echo "Bootstrap complete."
echo "  Venv:   $RUNTIME_ROOT/$VENV"
echo "  Config: cp .env.example .env   then edit .env (paste ASSEMBLYAI_API_KEY)"
echo "  Tests:  $VENV/bin/python -m pytest tests/ -q"
echo "  Run:    $VENV/bin/python -m hidock_direct"
