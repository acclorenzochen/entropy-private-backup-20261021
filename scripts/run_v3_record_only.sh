#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "$0")/.." && pwd)"
cd "$project_dir"

symbol="${ENTROPY_SYMBOL:-SNDK}"
hedge="${ENTROPY_HEDGE:-lighter}"
config="${ENTROPY_CONFIG:-config.yaml}"
env_file="${ENTROPY_ENV_FILE:-.env}"
v3_config="${ENTROPY_V3_CONFIG:-$project_dir/config.v3.edge.json}"

exec "$project_dir/.venv/bin/python" "$project_dir/main_v3.py" \
  --symbol "$symbol" --hedge "$hedge" \
  --config "$config" --env-file "$env_file" --v3-config "$v3_config" \
  --record-only --status-json logs/v3/record-status.edge.json \
  --observer-jsonl logs/v3/record-observer.edge.jsonl \
  --reference-checkpoint logs/v3/record-reference.edge.json "$@"
