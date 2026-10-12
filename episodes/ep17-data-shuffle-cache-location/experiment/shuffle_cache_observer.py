"""Observe where Spark keeps shuffle files and cached blocks in Ep17.

The application deliberately pauses after each action.  Those pauses leave the
Driver, Executors, shuffle files, and cached blocks available for UI and local
disk observation.
"""

import socket
import time

from pyspark import TaskContext
from pyspark.sql import SparkSession


INPUT_PATH = "/opt/ep17-shared/input-events"
SHUFFLE_PARTITIONS = 8
OBSERVATION_SECONDS = 300


def log_shuffle_partition(rows):
    """Record where each post-shuffle partition is materialized and cached."""
    context = TaskContext.get()
    print(
        "Materializing cached shuffle partition="
        f"{context.partitionId()} on executor host={socket.gethostname()}",
        flush=True,
    )
    yield from rows


spark = SparkSession.builder.appName("ep17-shuffle-cache-observer").getOrCreate()
sc = spark.sparkContext

events = spark.read.parquet(INPUT_PATH)
shuffled_events = events.repartition(SHUFFLE_PARTITIONS, "key")
cached_events = shuffled_events.rdd.mapPartitions(log_shuffle_partition).toDF(events.schema).cache()

print(f"Driver hostname: {socket.gethostname()}")
print(f"Driver UI: {sc.uiWebUrl}")
print(f"Input files discovered: {len(events.inputFiles())}")
print(f"Requested shuffle partitions: {SHUFFLE_PARTITIONS}")
print(f"Cache storage level: {cached_events.storageLevel}")

first_count = cached_events.count()
print(f"First count materialized the cache: {first_count:,} rows")
print(
    "Phase 1: inspect the shuffle boundary, Worker local files, and Storage tab "
    f"for {OBSERVATION_SECONDS} seconds.",
    flush=True,
)
time.sleep(OBSERVATION_SECONDS)

second_count = cached_events.filter("key = 7").count()
print(f"Second count reused the cached DataFrame: {second_count:,} rows")
print(
    "Phase 2: inspect the second action in the Jobs/Stages tabs; it should read "
    f"the cache instead of repeating the upstream scan and shuffle. Holding for "
    f"{OBSERVATION_SECONDS} seconds.",
    flush=True,
)
time.sleep(OBSERVATION_SECONDS)

cached_events.unpersist()
spark.stop()
