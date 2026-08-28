#!/usr/bin/env bash
# One command to light the whole substrate: infra (waits on health checks),
# then the supervised edge, ingest, and api. Logs land in .run/<svc>.log,
# PIDs in .run/<svc>.pid; stop with scripts/stop-all.sh. Spec: 003-infra.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .run

docker compose -f infra/docker-compose.yml up -d --wait

for svc in edge ingest api; do
    if [ -f ".run/$svc.pid" ] && kill -0 "$(cat ".run/$svc.pid")" 2>/dev/null; then
        echo "$svc already running (pid $(cat ".run/$svc.pid")); skipping"
        continue
    fi
    "scripts/run-$svc.sh" >> ".run/$svc.log" 2>&1 &
    echo $! > ".run/$svc.pid"
    echo "$svc started (pid $!, log .run/$svc.log)"
done

echo "dashboard: http://localhost:${CB_API_PORT:-8000}"
