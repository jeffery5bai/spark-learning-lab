#!/usr/bin/env bash

set -euo pipefail

readonly IMAGE="docker.io/apache/spark:3.4.4"
readonly NETWORK="ep15-spark-network"
readonly SUBMITTER="ep27-spark-submit"
readonly MASTER_URL="spark://spark-master:7077"
readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

if ! podman container exists ep15-spark-master; then
  echo "Missing Spark Master. Run Ep15 start_master.sh first." >&2
  exit 1
fi

for worker in ep27-spark-worker-1 ep27-spark-worker-2; do
  if ! podman container exists "$worker"; then
    echo "Missing $worker. Run prepare_workers.sh first." >&2
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
  --volume "$SCRIPT_DIR:/opt/ep27:ro" \
  "$IMAGE" \
  /opt/spark/bin/spark-submit \
  --master "$MASTER_URL" \
  --deploy-mode client \
  --conf spark.cores.max=4 \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=512m \
  --conf spark.sql.shuffle.partitions=32 \
  --conf spark.shuffle.compress=false \
  --conf spark.ui.port=4040 \
  /opt/ep27/aqe_partition_observer.py

podman ps --filter "name=$SUBMITTER"
