#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "$0")/.." && pwd)"
cd "$project_dir"

symbol="${ENTROPY_SYMBOL:-SNDK}"
hedge="${ENTROPY_HEDGE:-lighter}"
config="${ENTROPY_CONFIG:-config.yaml}"
env_file="${ENTROPY_ENV_FILE:-.env}"
v3_config="${ENTROPY_V3_CONFIG:-$project_dir/config.v3.edge.json}"
authorization="${ENTROPY_V3_AUTHORIZATION:-logs/v3/batch.authorization.20260923-002.json}"

args=(
  --symbol "$symbol"
  --hedge "$hedge"
  --config "$config"
  --env-file "$env_file"
  --v3-config "$v3_config"
  --live
  --batch-authorization "$authorization"
)
if [[ -n "${ENTROPY_V3_MAX_NEW_CYCLES:-}" ]]; then
  args+=(--max-new-cycles "$ENTROPY_V3_MAX_NEW_CYCLES")
fi
args+=("$@")

exec "$project_dir/.venv/bin/python" "$project_dir/main_v3.py" "${args[@]}"
