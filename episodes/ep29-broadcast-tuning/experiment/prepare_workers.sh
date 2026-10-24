#!/usr/bin/env bash

set -euo pipefail

readonly IMAGE="docker.io/apache/spark:3.4.4"
readonly NETWORK="ep15-spark-network"
readonly MASTER_URL="spark://spark-master:7077"

if ! podman network exists "$NETWORK"; then
  echo "Missing $NETWORK. Run Ep15 start_master.sh first." >&2
  exit 1
fi

# Run only when no application is using these lesson Workers.
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
  ep25-spark-worker-2 \
  ep26-spark-worker-1 \
  ep26-spark-worker-2 \
  ep27-spark-worker-1 \
  ep27-spark-worker-2 \
  ep28-spark-worker-1 \
  ep28-spark-worker-2 \
  ep29-spark-worker-1 \
  ep29-spark-worker-2; do
  if podman container exists "$worker"; then
    podman rm --force "$worker"
  fi
done

podman run --detach \
  --name ep29-spark-worker-1 \
  --network "$NETWORK" \
  --network-alias spark-worker-1 \
  --publish 8081:8081 \
  "$IMAGE" \
  /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
  --cores 2 --memory 3g --webui-port 8081 "$MASTER_URL"

podman run --detach \
  --name ep29-spark-worker-2 \
  --network "$NETWORK" \
  --network-alias spark-worker-2 \
  --publish 8082:8081 \
  "$IMAGE" \
  /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
  --cores 2 --memory 3g --webui-port 8081 "$MASTER_URL"

podman ps --filter 'name=ep29-spark-worker'
