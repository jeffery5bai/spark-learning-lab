#!/usr/bin/env bash

set -euo pipefail

readonly IMAGE="docker.io/apache/spark:3.4.4"
readonly NETWORK="ep15-spark-network"
readonly MASTER="ep15-spark-master"

if ! podman network exists "$NETWORK"; then
  podman network create "$NETWORK"
fi

if podman container exists "$MASTER"; then
  podman start "$MASTER"
else
  podman run --detach \
    --name "$MASTER" \
    --network "$NETWORK" \
    --network-alias spark-master \
    --publish 7077:7077 \
    --publish 8080:8080 \
    "$IMAGE" \
    /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
    --host spark-master \
    --port 7077 \
    --webui-port 8080
fi

podman ps --filter "name=$MASTER"
