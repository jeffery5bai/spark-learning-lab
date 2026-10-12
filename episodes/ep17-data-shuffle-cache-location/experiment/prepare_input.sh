#!/usr/bin/env bash

set -euo pipefail

readonly IMAGE="docker.io/apache/spark:3.4.4"
readonly SHARED_VOLUME="ep17-shared-data"
readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

if ! podman volume exists "$SHARED_VOLUME"; then
  echo "Missing $SHARED_VOLUME. Run prepare_shared_workers.sh first." >&2
  exit 1
fi

podman run --rm \
  --volume "$SHARED_VOLUME:/opt/ep17-shared" \
  --volume "$SCRIPT_DIR:/opt/ep17:ro" \
  "$IMAGE" \
  /opt/spark/bin/spark-submit \
  --master 'local[2]' \
  /opt/ep17/prepare_input.py
