#!/usr/bin/env bash
# The api under uvicorn. Spec: 003-infra.
set -euo pipefail
cd "$(dirname "$0")/../services/api"
exec uv run uvicorn app.main:app --host 127.0.0.1 --port "${CB_API_PORT:-8000}" "$@"
