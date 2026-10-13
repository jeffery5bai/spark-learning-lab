"""Keep task attempts running long enough to observe Executor loss and retry."""

import socket
import time

from pyspark import TaskContext
from pyspark.sql import SparkSession


PARTITIONS = 4
ROWS = 40_000
INITIAL_ATTEMPT_HOLD_SECONDS = 240
RETRY_ATTEMPT_HOLD_SECONDS = 15
POST_RECOVERY_SECONDS = 180


def hold_partition(rows):
    """Make every partition attempt observable before producing its rows."""
    context = TaskContext.get()
    hold_seconds = (
        INITIAL_ATTEMPT_HOLD_SECONDS
        if context.attemptNumber() == 0
        else RETRY_ATTEMPT_HOLD_SECONDS
    )
    print(
        "Task started: "
        f"partition={context.partitionId()} attempt={context.attemptNumber()} "
        f"executor_host={socket.gethostname()} hold_seconds={hold_seconds}",
        flush=True,
    )
    time.sleep(hold_seconds)
    yield from rows


spark = SparkSession.builder.appName("ep18-executor-failure-observer").getOrCreate()
sc = spark.sparkContext

work = spark.range(0, ROWS, numPartitions=PARTITIONS)
print(f"Driver hostname: {socket.gethostname()}")
print(f"Driver UI: {sc.uiWebUrl}")
print(f"Partitions to execute: {PARTITIONS}")
print(
    "Initial task attempts hold for "
    f"{INITIAL_ATTEMPT_HOLD_SECONDS} seconds; retries hold for "
    f"{RETRY_ATTEMPT_HOLD_SECONDS} seconds.",
)

row_count = work.rdd.mapPartitions(hold_partition).count()
print(f"Recovered application completed: {row_count:,} rows")
print(
    "Keeping the Driver alive for "
    f"{POST_RECOVERY_SECONDS} seconds so completed retry evidence is observable.",
    flush=True,
)
time.sleep(POST_RECOVERY_SECONDS)

spark.stop()
