#!/usr/bin/env bash

set -euo pipefail

readonly IMAGE="docker.io/apache/spark:3.4.4"
readonly NETWORK="ep15-spark-network"
readonly SHARED_VOLUME="ep25-shared-data"
readonly SUBMITTER="ep25-spark-submit"
readonly MASTER_URL="spark://spark-master:7077"
readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

if ! podman container exists ep15-spark-master; then
  echo "Missing Spark Master. Run Ep15 start_master.sh first." >&2
  exit 1
fi

for worker in ep25-spark-worker-1 ep25-spark-worker-2; do
  if ! podman container exists "$worker"; then
    echo "Missing $worker. Run prepare_experiment.sh first." >&2
    exit 1
  fi
done

if ! podman volume exists "$SHARED_VOLUME"; then
  echo "Missing $SHARED_VOLUME. Run prepare_experiment.sh first." >&2
  exit 1
fi

if podman container exists "$SUBMITTER"; then
  podman rm --force "$SUBMITTER"
fi

podman run --detach \
  --name "$SUBMITTER" \
  --network "$NETWORK" \
  --publish 4040:4040 \
  --volume "$SHARED_VOLUME:/opt/ep25-shared" \
  --volume "$SCRIPT_DIR:/opt/ep25:ro" \
  "$IMAGE" \
  /opt/spark/bin/spark-submit \
  --master "$MASTER_URL" \
  --deploy-mode client \
  --conf spark.cores.max=4 \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=1024m \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.sql.files.maxPartitionBytes=1m \
  --conf spark.sql.files.openCostInBytes=4m \
  --conf spark.ui.port=4040 \
  /opt/ep25/small_file_observer.py

podman ps --filter "name=$SUBMITTER"
