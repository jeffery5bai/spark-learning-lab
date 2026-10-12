#!/usr/bin/env bash

set -euo pipefail

# Run while ep17-shuffle-cache-observer is in Phase 1 or Phase 2.  In this
# Standalone setup each Worker creates its Executor's local directory below
# /tmp/spark-.../executor-.../. This script only reads those files.
for worker in ep15-spark-worker-1 ep17-spark-worker-2; do
  echo "== $worker: Worker-managed Executor local directories =="
  podman exec "$worker" sh -c '
    if find /tmp -type f -name "shuffle_*" -print -quit | grep -q .; then
      echo "Shuffle files:"
      find /tmp -type f -name "shuffle_*" | sort
    else
      echo "No shuffle files found yet."
    fi
  '
done
