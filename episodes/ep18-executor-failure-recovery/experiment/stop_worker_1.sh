#!/usr/bin/env bash

set -euo pipefail

readonly WORKER="ep15-spark-worker-1"

if ! podman container exists "$WORKER"; then
  echo "Missing $WORKER. Run prepare_two_workers.sh first." >&2
  exit 1
fi

if [ "$(podman inspect --format '{{.State.Running}}' "$WORKER")" != "true" ]; then
  echo "$WORKER is already stopped." >&2
  exit 1
fi

podman stop --time 0 "$WORKER"
