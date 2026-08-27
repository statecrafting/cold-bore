#!/usr/bin/env bash
# Supervised ingest run: restarts on non-zero exit so the `kill ingest`
# drill measures recovery (backlog drain, redelivery absorption), not
# babysitting. Spec: 003-infra.
set -u
cd "$(dirname "$0")/.."
while true; do
    cargo run --release -p coldbore-ingest "$@"
    code=$?
    if [ "$code" -eq 0 ]; then
        exit 0
    fi
    echo "coldbore-ingest exited $code; restarting in 1s" >&2
    sleep 1
done
