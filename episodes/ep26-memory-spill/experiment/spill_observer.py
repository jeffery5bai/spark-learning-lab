"""Create a controlled sort-based shuffle whose spill metrics can be inspected."""

import struct
import time
from typing import Iterable, Iterator, Tuple

from pyspark import TaskContext
from pyspark.sql import SparkSession


RECORD_COUNT = 2_400_000
SOURCE_PARTITIONS = 8
SHUFFLE_PARTITIONS = 8
OBSERVATION_SECONDS = 420


def make_payload(value: int) -> bytes:
    """Return 128 deterministic, varied bytes so shuffle compression cannot hide volume."""
    mask = (1 << 64) - 1
    seed = value & mask
    words = []
    for multiplier, increment in (
        (6364136223846793005, 1442695040888963407),
        (2862933555777941757, 3037000493),
        (3202034522624059733, 1),
        (3935559000370003845, 2691343689449507681),
        (1442695040888963407, 6364136223846793005),
        (1103515245, 12345),
        (214013, 2531011),
        (1664525, 1013904223),
        (22695477, 1),
        (69069, 362437),
        (48271, 0),
        (16807, 0),
        (25214903917, 11),
        (2147483629, 1),
        (1181783497276652981, 0),
        (7046029254386353131, 1),
    ):
        seed = (seed * multiplier + increment) & mask
        words.append(seed)
    return struct.pack(">16Q", *words)


def summarize_partition(
    partition_id: int, records: Iterable[Tuple[int, int]]
) -> Iterator[Tuple[int, int, int, int]]:
    """Consume every sorted record and return one small result per partition."""
    count = 0
    first_key = -1
    last_key = -1

    for key, _ in records:
        if count == 0:
            first_key = key
        last_key = key
        count += 1

    context = TaskContext.get()
    task_attempt = context.taskAttemptId() if context is not None else -1
    print(
        f"partition={partition_id} task_attempt={task_attempt} "
        f"records={count} first_key={first_key} last_key={last_key}",
        flush=True,
    )
    yield partition_id, count, first_key, last_key


spark = SparkSession.builder.appName("ep26-memory-spill").getOrCreate()
sc = spark.sparkContext

records = sc.range(RECORD_COUNT, numSlices=SOURCE_PARTITIONS).map(
    lambda value: ((value * 37) % RECORD_COUNT, make_payload(value))
)
sorted_records = records.sortByKey(numPartitions=SHUFFLE_PARTITIONS)

print(f"Driver UI: {sc.uiWebUrl}", flush=True)
print(
    "Experiment constants: "
    f"records={RECORD_COUNT}, source_partitions={SOURCE_PARTITIONS}, "
    f"shuffle_partitions={SHUFFLE_PARTITIONS}",
    flush=True,
)
print(
    "Spill settings: "
    f"executor_memory={sc.getConf().get('spark.executor.memory')}, "
    "memory_fraction="
    f"{sc.getConf().get('spark.memory.fraction')}, "
    "bypass_merge_threshold="
    f"{sc.getConf().get('spark.shuffle.sort.bypassMergeThreshold')}",
    flush=True,
)

sc.setJobDescription("forced spill: sortByKey with controlled shuffle sorter")
summaries = sorted_records.mapPartitionsWithIndex(summarize_partition).collect()
sc.setJobDescription(None)

print(f"Completed sort summaries: {sorted(summaries)}", flush=True)
print(
    "The action has completed. Keep the application alive for "
    f"{OBSERVATION_SECONDS} seconds so spill, GC, shuffle, and task metrics remain "
    "available in the Driver UI.",
    flush=True,
)
time.sleep(OBSERVATION_SECONDS)

spark.stop()
