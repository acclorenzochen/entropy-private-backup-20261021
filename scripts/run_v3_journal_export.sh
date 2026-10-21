#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "$0")/.." && pwd)"
cd "$project_dir"

exec "$project_dir/.venv/bin/python" "$project_dir/tools/v3_journal_export.py" \
  --journal logs/v3/journal.sqlite3 \
  --output reports/entropy-edge-v1/journal-live.sql \
  --watch --interval-seconds 60 "$@"
