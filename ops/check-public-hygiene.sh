#!/usr/bin/env bash

set -euo pipefail

ROOT="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "Checking ignored runtime paths..."
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  tracked_runtime="$(git ls-files | grep -E '^(archive/|\\.venv/)' || true)"
  if [[ -n "$tracked_runtime" ]]; then
    echo "Tracked runtime files detected:"
    printf '%s\n' "$tracked_runtime"
    exit 1
  fi
fi

echo "Checking for hardcoded local paths or personal identifiers..."
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  if git grep -nE '(/Users/|file:///Users/)' -- \
    . \
    ':(exclude)archive' \
    ':(exclude).venv' \
    ':(exclude)ops/check-public-hygiene.sh'; then
    echo "Found non-shareable local references."
    exit 1
  fi
else
  if find . \
    -type f \
    ! -path './archive/*' \
    ! -path './.venv/*' \
    ! -path '*/__pycache__/*' \
    ! -path './ops/check-public-hygiene.sh' \
    -print0 | xargs -0 grep -nE '(/Users/|file:///Users/)'; then
    echo "Found non-shareable local references."
    exit 1
  fi
fi

echo "Public hygiene checks passed."
