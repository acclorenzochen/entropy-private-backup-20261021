#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "$0")/.." && pwd)"
cd "$project_dir"

exec /opt/homebrew/bin/uv run --with-requirements requirements-live.txt \
  python main.py --symbol SNDK --hedge lighter \
  --config config.yaml --env-file .env --no-dashboard \
  --max-complete-cycles 20 --max-run-loss-usd 0.50
