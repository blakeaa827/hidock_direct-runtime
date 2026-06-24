#!/usr/bin/env bash
# refresh_diarize.sh — re-vendor src/diarize_audio/ from the canonical
# diarize_audio-runtime repo at a specified commit.
#
# Usage:
#   scripts/refresh_diarize.sh <commit-sha> [path-to-diarize-repo]
#
# The canonical repo defaults to the sibling ../diarize_audio-runtime.
# Verbatim copy — diarize_audio uses relative intra-package imports, so NO
# import rewriting is needed (unlike refresh_jensen.sh). Updates VENDORED_COMMIT.
#
# IMPORTANT (per the distribution PRD's Shared-State sync protocol): the
# blake-commons decoupling lives in the CANONICAL repo. Always re-vendor from a
# canonical commit that already carries it — never hand-edit src/diarize_audio/.
# Review the diff and run the suite before committing.

set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: $0 <commit-sha> [path-to-diarize-repo]" >&2
  exit 2
fi

PIN="$1"
RUNTIME_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CANON="${2:-$(dirname "$RUNTIME_ROOT")/diarize_audio-runtime}"
DEST="$RUNTIME_ROOT/src/diarize_audio"

if [ ! -d "$CANON/.git" ]; then
  echo "Canonical diarize_audio repo not found at: $CANON" >&2
  echo "Pass its path as the 2nd argument, or clone it as a sibling." >&2
  exit 1
fi

if ! git -C "$CANON" cat-file -e "${PIN}^{commit}" 2>/dev/null; then
  echo "Commit $PIN not found in $CANON" >&2
  exit 1
fi

echo "==> Re-vendoring src/diarize_audio/ from $CANON at $PIN"
rm -rf "$DEST"
# Extract the package tree at the pinned commit, verbatim. git archive paths are
# relative to the canonical repo root (src/diarize_audio/...), so extracting at
# RUNTIME_ROOT lands them exactly at RUNTIME_ROOT/src/diarize_audio/...
git -C "$CANON" archive "$PIN" -- src/diarize_audio | tar -x -C "$RUNTIME_ROOT"

DATE="$(date -u +%Y-%m-%d)"
printf '%s %s\n' "$PIN" "$DATE" > "$DEST/VENDORED_COMMIT"

echo ""
echo "Refreshed src/diarize_audio/ at $PIN ($DATE). Now:"
echo "  1. git diff src/diarize_audio/   -- review upstream changes"
echo "  2. .venv/bin/pip install -e . && .venv/bin/python -m pytest tests/ -q"
echo "  3. git add src/diarize_audio/ && git commit"
