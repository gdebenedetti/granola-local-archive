#!/bin/zsh

set -euo pipefail

ROOT="${0:A:h:h}"
export PYTHONPATH="$ROOT/src${PYTHONPATH:+:$PYTHONPATH}"

exec "$ROOT/.venv/bin/python" -m granola_local_archive.cli --workspace "$ROOT" serve-mcp "$@"
