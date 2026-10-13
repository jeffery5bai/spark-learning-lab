#!/usr/bin/env bash

set -euo pipefail

readonly IMAGE="docker.io/apache/spark:3.4.4"
readonly NETWORK="ep15-spark-network"
readonly SUBMITTER="ep18-spark-submit"
readonly MASTER_URL="spark://spark-master:7077"
readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

for container in ep15-spark-master ep15-spark-worker-1 ep17-spark-worker-2; do
  if ! podman container exists "$container"; then
    echo "Missing $container. Run prepare_two_workers.sh first." >&2
    exit 1
  fi

  if [ "$(podman inspect --format '{{.State.Running}}' "$container")" != "true" ]; then
    echo "$container is not running. Run prepare_two_workers.sh first." >&2
    exit 1
  fi
done

if podman container exists "$SUBMITTER"; then
  podman rm --force "$SUBMITTER"
fi

podman run --detach \
  --name "$SUBMITTER" \
  --network "$NETWORK" \
  --publish 4040:4040 \
  --volume "$SCRIPT_DIR:/opt/ep18:ro" \
  "$IMAGE" \
  /opt/spark/bin/spark-submit \
  --master "$MASTER_URL" \
  --deploy-mode client \
  --conf spark.cores.max=4 \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=1024m \
  --conf spark.task.maxFailures=4 \
  --conf spark.ui.port=4040 \
  /opt/ep18/failure_observer.py

podman ps --filter "name=$SUBMITTER"
