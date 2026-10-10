#!/usr/bin/env bash

set -euo pipefail

readonly IMAGE="docker.io/apache/spark:3.4.4"
readonly NETWORK="ep15-spark-network"
readonly MASTER_URL="spark://spark-master:7077"
readonly EXAMPLES_JAR="/opt/spark/examples/jars/spark-examples_2.12-3.4.4.jar"

if ! podman container exists ep15-spark-master; then
  echo "Missing Spark Master. Run start_master.sh first." >&2
  exit 1
fi

if ! podman container exists ep15-spark-worker-1; then
  echo "Missing Spark Worker. Run start_worker_1.sh first." >&2
  exit 1
fi

podman run --rm \
  --network "$NETWORK" \
  "$IMAGE" \
  /opt/spark/bin/spark-submit \
  --master "$MASTER_URL" \
  --deploy-mode cluster \
  --driver-cores 1 \
  --total-executor-cores 1 \
  --class org.apache.spark.examples.SparkPi \
  "$EXAMPLES_JAR" \
  20000
