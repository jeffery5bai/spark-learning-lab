#!/usr/bin/env bash

set -euo pipefail

readonly IMAGE="docker.io/apache/spark:3.4.4"
readonly NETWORK="ep15-spark-network"
readonly SHARED_VOLUME="ep30-shared-data"
readonly MASTER_URL="spark://spark-master:7077"
readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

if ! podman container exists ep15-spark-master || ! podman network exists "$NETWORK"; then
  echo "Missing Spark Master or network. Run Ep15 start_master.sh first." >&2
  exit 1
fi

# Refuse to replace lesson Workers while an application is registered with the Master.
podman exec ep15-spark-master python3 -c '
import json, urllib.request
with urllib.request.urlopen("http://localhost:8080/json/", timeout=10) as response:
    info = json.load(response)
if "activeapps" not in info:
    raise SystemExit("Cannot determine active applications; leaving Workers unchanged.")
if info["activeapps"]:
    raise SystemExit("Active Spark application found; stop it before preparing Ep30.")
'

if ! podman volume exists "$SHARED_VOLUME"; then
  podman volume create "$SHARED_VOLUME"
fi

podman run --rm --name ep30-input-writer \
  --volume "$SHARED_VOLUME:/opt/ep30-shared" \
  --volume "$SCRIPT_DIR:/opt/ep30:ro" \
  "$IMAGE" \
  /opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 512m \
  /opt/ep30/prepare_input.py

for worker in \
  ep15-spark-worker-1 \
  ep17-spark-worker-2 \
  ep22-spark-worker-1 ep22-spark-worker-2 \
  ep23-spark-worker-1 ep23-spark-worker-2 \
  ep24-spark-worker-1 ep24-spark-worker-2 \
  ep25-spark-worker-1 ep25-spark-worker-2 \
  ep26-spark-worker-1 ep26-spark-worker-2 \
  ep27-spark-worker-1 ep27-spark-worker-2 \
  ep28-spark-worker-1 ep28-spark-worker-2 \
  ep29-spark-worker-1 ep29-spark-worker-2 \
  ep30-spark-worker-1 ep30-spark-worker-2; do
  if podman container exists "$worker"; then
    podman rm --force "$worker"
  fi
done

podman run --detach \
  --name ep30-spark-worker-1 \
  --network "$NETWORK" --network-alias spark-worker-1 \
  --publish 8081:8081 \
  --volume "$SHARED_VOLUME:/opt/ep30-shared:ro" \
  "$IMAGE" \
  /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
  --cores 2 --memory 3g --webui-port 8081 "$MASTER_URL"

podman run --detach \
  --name ep30-spark-worker-2 \
  --network "$NETWORK" --network-alias spark-worker-2 \
  --publish 8082:8081 \
  --volume "$SHARED_VOLUME:/opt/ep30-shared:ro" \
  "$IMAGE" \
  /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
  --cores 2 --memory 3g --webui-port 8081 "$MASTER_URL"

podman ps --filter 'name=ep30-spark-worker'
