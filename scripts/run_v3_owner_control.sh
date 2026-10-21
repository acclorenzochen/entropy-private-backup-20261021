#!/usr/bin/env bash
set -euo pipefail
# Opt-in independent adapter; never starts an engine, recorder, or exit CLI.
cd "$(dirname "$0")/.."
if [[ "${V3_OWNER_CONTROL_ENABLED:-}" != "1" ]]; then
  printf '%s\n' 'Set explicit V3_OWNER_CONTROL_ENABLED=1 and trusted owner bindings only after owner activation is approved.' >&2
  exit 2
fi
python_bin=".venv/bin/python"
[[ -x "$python_bin" ]] || python_bin="python3"
exec "$python_bin" -m uvicorn entropy_arb.dashboard_api.v3_control_app:create_app \
  --factory --workers 1 --host 127.0.0.1 --port "${V3_OWNER_CONTROL_PORT:-8786}" \
  --no-proxy-headers --no-access-log
