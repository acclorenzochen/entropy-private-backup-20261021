#!/usr/bin/env bash
set -euo pipefail

host="${DASHBOARD_HOST:-127.0.0.1}"
port="${DASHBOARD_PORT:-8765}"

if command -v uvicorn >/dev/null 2>&1; then
  exec uvicorn entropy_arb.dashboard_api.app:app \
    --host "$host" \
    --port "$port"
fi

uv_bin="${UV_BIN:-/opt/homebrew/bin/uv}"
exec "$uv_bin" run --with-requirements requirements.txt uvicorn \
  entropy_arb.dashboard_api.app:app \
  --host "$host" \
  --port "$port"
