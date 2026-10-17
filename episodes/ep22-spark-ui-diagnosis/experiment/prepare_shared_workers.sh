#!/usr/bin/env bash

set -euo pipefail

readonly IMAGE="docker.io/apache/spark:3.4.4"
readonly NETWORK="ep15-spark-network"
readonly MASTER_URL="spark://spark-master:7077"
readonly SHARED_VOLUME="ep22-shared-data"

if ! podman network exists "$NETWORK"; then
  echo "Missing $NETWORK. Run Ep15 start_master.sh first." >&2
  exit 1
fi

if ! podman volume exists "$SHARED_VOLUME"; then
  podman volume create "$SHARED_VOLUME"
fi

# This experiment needs exactly two Workers. Run only when no application is
# using the known workers from Ep15, Ep17, or Ep22.
for worker in \
  ep15-spark-worker-1 \
  ep17-spark-worker-2 \
  ep22-spark-worker-1 \
  ep22-spark-worker-2; do
  if podman container exists "$worker"; then
    podman rm --force "$worker"
  fi
done

podman run --detach \
  --name ep22-spark-worker-1 \
  --network "$NETWORK" \
  --network-alias spark-worker-1 \
  --publish 8081:8081 \
  --volume "$SHARED_VOLUME:/opt/ep22-shared" \
  "$IMAGE" \
  /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
  --cores 2 \
  --memory 3g \
  --webui-port 8081 \
  "$MASTER_URL"

podman run --detach \
  --name ep22-spark-worker-2 \
  --network "$NETWORK" \
  --network-alias spark-worker-2 \
  --publish 8082:8081 \
  --volume "$SHARED_VOLUME:/opt/ep22-shared" \
  "$IMAGE" \
  /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
  --cores 2 \
  --memory 3g \
  --webui-port 8081 \
  "$MASTER_URL"

podman ps --filter 'name=ep22-spark-worker'
