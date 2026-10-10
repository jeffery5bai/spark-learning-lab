#!/usr/bin/env bash

set -euo pipefail

readonly IMAGE="docker.io/apache/spark:3.4.4"
readonly NETWORK="ep15-spark-network"
readonly WORKER="ep15-spark-worker-1"
readonly MASTER_URL="spark://spark-master:7077"

if ! podman network exists "$NETWORK"; then
  echo "Missing $NETWORK. Run start_master.sh first." >&2
  exit 1
fi

if podman container exists "$WORKER"; then
  podman start "$WORKER"
else
  podman run --detach \
    --name "$WORKER" \
    --network "$NETWORK" \
    --network-alias spark-worker-1 \
    --publish 8081:8081 \
    "$IMAGE" \
    /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
    --cores 2 \
    --memory 3g \
    --webui-port 8081 \
    "$MASTER_URL"
fi

podman ps --filter "name=$WORKER"
