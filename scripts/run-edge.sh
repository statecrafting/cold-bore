#!/usr/bin/env bash
# Supervised edge run: restarts on non-zero exit so the `kill edge` drill
# measures recovery, not babysitting. Spec: 003-infra.
set -u
cd "$(dirname "$0")/.."
while true; do
    cargo run --release -p coldbore-edge "$@"
    code=$?
    if [ "$code" -eq 0 ]; then
        exit 0
    fi
    echo "coldbore-edge exited $code; restarting in 1s" >&2
    sleep 1
done
