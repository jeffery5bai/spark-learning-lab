#!/usr/bin/env bash

set -euo pipefail

readonly WORKER="ep15-spark-worker-1"

if ! podman container exists "$WORKER"; then
  echo "Missing $WORKER. Run Ep15 start_worker_1.sh first." >&2
  exit 1
fi

if [ "$(podman inspect --format '{{.State.Running}}' "$WORKER")" != "true" ]; then
  podman start "$WORKER"
fi

podman ps --filter "name=$WORKER"
