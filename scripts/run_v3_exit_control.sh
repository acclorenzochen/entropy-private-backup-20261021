#!/usr/bin/env bash
set -euo pipefail
# Deliberately separate from the GET-only dashboard and trading launchers.
cd "$(dirname "$0")/.."
python_bin=".venv/bin/python"
[[ -x "$python_bin" ]] || python_bin="python3"
exec "$python_bin" -m uvicorn entropy_arb.dashboard_api.v3_exit_app:create_app \
  --factory --workers 1 --host 127.0.0.1 --port 8786 \
  --no-proxy-headers --no-access-log
