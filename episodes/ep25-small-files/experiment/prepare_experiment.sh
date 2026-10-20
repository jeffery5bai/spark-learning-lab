#!/usr/bin/env bash

set -euo pipefail

readonly IMAGE="docker.io/apache/spark:3.4.4"
readonly NETWORK="ep15-spark-network"
readonly MASTER_URL="spark://spark-master:7077"
readonly SHARED_VOLUME="ep25-shared-data"
readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

if ! podman network exists "$NETWORK"; then
  echo "Missing $NETWORK. Run Ep15 start_master.sh first." >&2
  exit 1
fi

if ! podman volume exists "$SHARED_VOLUME"; then
  podman volume create "$SHARED_VOLUME"
fi

# This experiment needs exactly two Workers. Run only when no application is
# using the known Workers from Ep15, Ep17, Ep22, Ep23, Ep24, or Ep25.
for worker in \
  ep15-spark-worker-1 \
  ep17-spark-worker-2 \
  ep22-spark-worker-1 \
  ep22-spark-worker-2 \
  ep23-spark-worker-1 \
  ep23-spark-worker-2 \
  ep24-spark-worker-1 \
  ep24-spark-worker-2 \
  ep25-spark-worker-1 \
  ep25-spark-worker-2; do
  if podman container exists "$worker"; then
    podman rm --force "$worker"
  fi
done

podman run --detach \
  --name ep25-spark-worker-1 \
  --network "$NETWORK" \
  --network-alias spark-worker-1 \
  --publish 8081:8081 \
  --volume "$SHARED_VOLUME:/opt/ep25-shared" \
  "$IMAGE" \
  /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
  --cores 2 \
  --memory 3g \
  --webui-port 8081 \
  "$MASTER_URL"

podman run --detach \
  --name ep25-spark-worker-2 \
  --network "$NETWORK" \
  --network-alias spark-worker-2 \
  --publish 8082:8081 \
  --volume "$SHARED_VOLUME:/opt/ep25-shared" \
  "$IMAGE" \
  /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
  --cores 2 \
  --memory 3g \
  --webui-port 8081 \
  "$MASTER_URL"

podman run --rm \
  --volume "$SHARED_VOLUME:/opt/ep25-shared" \
  --volume "$SCRIPT_DIR:/opt/ep25:ro" \
  "$IMAGE" \
  /opt/spark/bin/spark-submit \
  --master 'local[2]' \
  /opt/ep25/prepare_input.py

podman ps --filter 'name=ep25-spark-worker'
