#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "$0")/.." && pwd)"
cd "$project_dir"

symbol="${ENTROPY_SYMBOL:-SNDK}"
hedge="${ENTROPY_HEDGE:-lighter}"
config="${ENTROPY_CONFIG:-config.yaml}"
env_file="${ENTROPY_ENV_FILE:-.env}"

exec /opt/homebrew/bin/uv run --with-requirements requirements-live.txt \
  python main.py --symbol "$symbol" --hedge "$hedge" \
  --config "$config" --env-file "$env_file" --no-dashboard
