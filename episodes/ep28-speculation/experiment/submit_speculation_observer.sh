#!/usr/bin/env bash

set -euo pipefail

readonly IMAGE="docker.io/apache/spark:3.4.4"
readonly NETWORK="ep15-spark-network"
readonly SUBMITTER="ep28-spark-submit"
readonly MASTER_URL="spark://spark-master:7077"
readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

if ! podman container exists ep15-spark-master; then
  echo "Missing Spark Master. Run Ep15 start_master.sh first." >&2
  exit 1
fi

for worker in ep28-spark-worker-1 ep28-spark-worker-2; do
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
  --volume "$SCRIPT_DIR:/opt/ep28:ro" \
  "$IMAGE" \
  /opt/spark/bin/spark-submit \
  --master "$MASTER_URL" \
  --deploy-mode client \
  --conf spark.cores.max=4 \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=512m \
  --conf spark.speculation=true \
  --conf spark.speculation.interval=100ms \
  --conf spark.speculation.multiplier=1.0 \
  --conf spark.speculation.quantile=0.75 \
  --conf spark.speculation.minTaskRuntime=2s \
  --conf spark.ui.port=4040 \
  /opt/ep28/speculation_observer.py

podman ps --filter "name=$SUBMITTER"
