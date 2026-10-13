#!/usr/bin/env bash

set -euo pipefail

readonly IMAGE="docker.io/apache/spark:3.4.4"
readonly NETWORK="ep15-spark-network"
readonly MASTER_URL="spark://spark-master:7077"
readonly WORKER_1="ep15-spark-worker-1"
readonly WORKER_2="ep17-spark-worker-2"

ensure_running() {
  local container="$1"

  if ! podman container exists "$container"; then
    return 1
  fi

  if [ "$(podman inspect --format '{{.State.Running}}' "$container")" != "true" ]; then
    podman start "$container"
  fi
}

if ! podman container exists ep15-spark-master; then
  echo "Missing Spark Master. Run Ep15 start_master.sh first." >&2
  exit 1
fi

if ! ensure_running "$WORKER_1"; then
  echo "Missing $WORKER_1. Run Ep15 start_worker_1.sh first." >&2
  exit 1
fi

if podman container exists "$WORKER_2"; then
  ensure_running "$WORKER_2"
else
  podman run --detach \
    --name "$WORKER_2" \
    --network "$NETWORK" \
    --network-alias spark-worker-2 \
    --publish 8082:8081 \
    "$IMAGE" \
    /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
    --cores 2 \
    --memory 3g \
    --webui-port 8081 \
    "$MASTER_URL"
fi

podman ps --filter "name=ep15-spark-master" --filter "name=$WORKER_1" --filter "name=$WORKER_2"
