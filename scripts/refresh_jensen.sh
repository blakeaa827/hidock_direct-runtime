#!/usr/bin/env bash
# refresh_jensen.sh — re-vendor src/hidock_direct/jensen/ from sgeraldes/hidock-next
# at a specified upstream commit.
#
# Usage:
#   scripts/refresh_jensen.sh <commit-sha>
#
# Idempotent. Updates VENDORED_COMMIT. Review the diff before committing.

set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: $0 <commit-sha>" >&2
  exit 2
fi

PIN="$1"
RUNTIME_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JENSEN="$RUNTIME_ROOT/src/hidock_direct/jensen"

if ! command -v gh >/dev/null 2>&1; then
  echo "gh CLI required (brew install gh)" >&2
  exit 1
fi

echo "==> Fetching upstream files at $PIN"
for f in hidock_device.py jensen_protocol_extensions.py hta_converter.py config_and_logger.py constants.py; do
  echo "  $f"
  gh api "/repos/sgeraldes/hidock-next/contents/apps/desktop/src/$f?ref=$PIN" --jq '.content' | base64 -d > "$JENSEN/$f"
done

echo "==> Rewriting imports to be relative within the package"
python3 - <<PY
import re, pathlib
jensen = pathlib.Path("$JENSEN")
LOCAL = {"constants", "config_and_logger", "hidock_device", "jensen_protocol_extensions", "hta_converter"}
for path in jensen.glob("*.py"):
    text = path.read_text()
    def repl(m):
        mod = m.group(1)
        return f"from .{mod} import" if mod in LOCAL else m.group(0)
    text = re.sub(r"^from (\w+) import", repl, text, flags=re.MULTILINE)
    def repl_import(m):
        mod = m.group(1)
        return f"from . import {mod}" if mod in LOCAL else m.group(0)
    text = re.sub(r"^import (\w+)\$", repl_import, text, flags=re.MULTILINE)
    path.write_text(text)
PY

DATE="$(date -u +%Y-%m-%d)"
printf '%s %s\n' "$PIN" "$DATE" > "$JENSEN/VENDORED_COMMIT"

echo ""
echo "Refreshed. Now:"
echo "  1. git diff src/hidock_direct/jensen/  -- review upstream changes"
echo "  2. pytest tests/ -v                    -- confirm nothing broke"
echo "  3. git add & commit"
