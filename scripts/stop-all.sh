#!/usr/bin/env bash
# Stop everything run-all.sh started. Infra keeps running unless --infra is
# passed (data survives either way; `down -v` is the destructive form).
# Spec: 003-infra.
set -uo pipefail
cd "$(dirname "$0")/.."

for svc in api ingest edge; do
    if [ -f ".run/$svc.pid" ]; then
        pid=$(cat ".run/$svc.pid")
        kill "$pid" 2>/dev/null && echo "$svc supervisor stopped (pid $pid)"
        rm -f ".run/$svc.pid"
    fi
done

# The supervisors spawn the actual processes; make sure none outlive them.
pkill -f 'target/release/coldbore-edge' 2>/dev/null || true
pkill -f 'target/release/coldbore-ingest' 2>/dev/null || true
pkill -f 'uvicorn app.main:app' 2>/dev/null || true

if [ "${1:-}" = "--infra" ]; then
    docker compose -f infra/docker-compose.yml down
fi
exit 0
