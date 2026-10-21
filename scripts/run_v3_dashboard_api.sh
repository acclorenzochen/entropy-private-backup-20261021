#!/usr/bin/env bash
set -euo pipefail

# Separate, local-only V3 reader.  Do not replace the legacy API until the
# source paths and external visitor access boundary have been checked.
host="${DASHBOARD_V3_HOST:-127.0.0.1}"
port="${DASHBOARD_V3_PORT:-8766}"

if command -v uvicorn >/dev/null 2>&1; then
  exec uvicorn entropy_arb.dashboard_api.v3_app:create_app \
    --factory --workers 1 --host "$host" --port "$port" --no-access-log
fi

if [[ -x .venv/bin/uvicorn ]]; then
  exec .venv/bin/uvicorn entropy_arb.dashboard_api.v3_app:create_app \
    --factory --workers 1 --host "$host" --port "$port" --no-access-log
fi

uv_bin="${UV_BIN:-/opt/homebrew/bin/uv}"
exec "$uv_bin" run --with-requirements requirements.txt uvicorn \
  entropy_arb.dashboard_api.v3_app:create_app \
  --factory --workers 1 --host "$host" --port "$port" --no-access-log
