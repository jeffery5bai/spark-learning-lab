"""Compare balanced and skewed shuffle keys in one observable Spark application."""

import time
from typing import Iterable, Iterator, Tuple

from pyspark import TaskContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


ROW_COUNT = 600_000
SOURCE_PARTITIONS = 8
SHUFFLE_PARTITIONS = 8
CPU_ITERATIONS_PER_ROW = 96
OBSERVATION_SECONDS = 420


def fixed_cpu_work(rows: Iterable[Tuple[int, int]]) -> Iterator[Tuple[int, int, int]]:
    """Do identical work per received row so partition size is visible in duration."""
    context = TaskContext.get()
    partition_id = context.partitionId() if context is not None else -1
    row_count = 0
    checksum = 0

    for _, value in rows:
        state = value
        for _ in range(CPU_ITERATIONS_PER_ROW):
            state = (state * 17 + 23) % 1_000_003
        checksum = (checksum + state) % 1_000_003
        row_count += 1

    print(
        f"post-shuffle partition={partition_id} rows={row_count} checksum={checksum}",
        flush=True,
    )
    yield partition_id, row_count, checksum


def run_phase(events: DataFrame, key_column: str, phase_name: str) -> None:
    print(f"Starting {phase_name} phase with key column {key_column}.", flush=True)
    sc.setJobDescription(f"{phase_name}: redistribute by {key_column}")
    results = (
        events.select(F.col(key_column).alias("key"), F.col("value"))
        .repartition(SHUFFLE_PARTITIONS, "key")
        .rdd.mapPartitions(fixed_cpu_work)
        .collect()
    )
    partition_counts = sorted((partition_id, row_count) for partition_id, row_count, _ in results)
    sc.setJobDescription(None)
    print(f"Completed {phase_name} phase: {partition_counts}", flush=True)
    print(
        f"Keep the Driver UI open for {OBSERVATION_SECONDS} seconds. "
        f"Capture the completed {phase_name} Job and its post-shuffle Stage details now.",
        flush=True,
    )
    time.sleep(OBSERVATION_SECONDS)


spark = SparkSession.builder.appName("ep23-data-skew").getOrCreate()
sc = spark.sparkContext

events = (
    spark.range(ROW_COUNT, numPartitions=SOURCE_PARTITIONS)
    .select(
        F.col("id"),
        (F.col("id") % F.lit(64)).cast("int").alias("uniform_key"),
        F.when(F.col("id") % F.lit(10) < F.lit(9), F.lit(0))
        .otherwise((F.col("id") % F.lit(63) + F.lit(1)).cast("int"))
        .alias("skew_key"),
        (F.col("id") % F.lit(97)).cast("int").alias("value"),
    )
)

print(f"Driver UI: {sc.uiWebUrl}", flush=True)
print(
    "Experiment constants: "
    f"rows={ROW_COUNT}, source_partitions={SOURCE_PARTITIONS}, "
    f"shuffle_partitions={SHUFFLE_PARTITIONS}, hot_key_share≈90%",
    flush=True,
)

run_phase(events, "uniform_key", "baseline")
run_phase(events, "skew_key", "skew")

spark.stop()
