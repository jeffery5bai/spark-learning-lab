"""Compare static shuffle partitions with AQE post-shuffle coalescing."""

import time
from typing import Iterable, Iterator, Tuple

from pyspark import TaskContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


RECORD_COUNT = 1_200_000
SOURCE_PARTITIONS = 16
KEY_CARDINALITY = 120_000
INITIAL_SHUFFLE_PARTITIONS = 32
ADVISORY_PARTITION_SIZE = "16m"
OBSERVATION_SECONDS = 420


spark = SparkSession.builder.appName("ep27-aqe-shuffle-partitions").getOrCreate()
sc = spark.sparkContext


def build_aggregation() -> DataFrame:
    """Create uniformly distributed rows with a wide aggregate state."""
    events = (
        spark.range(RECORD_COUNT, numPartitions=SOURCE_PARTITIONS)
        .select(
            (F.col("id") % F.lit(KEY_CARDINALITY)).cast("int").alias("group_key"),
            (F.col("id") % F.lit(97)).cast("long").alias("amount"),
            F.concat(
                F.lpad(F.hex(F.col("id")), 16, "0"),
                F.lpad(F.hex(F.col("id") * F.lit(104729)), 16, "0"),
                F.lpad(F.hex(F.col("id") * F.lit(65537)), 16, "0"),
                F.lpad(F.hex(F.col("id") * F.lit(8191)), 16, "0"),
                F.lpad(F.hex(F.col("id") * F.lit(4099)), 16, "0"),
                F.lpad(F.hex(F.col("id") * F.lit(2053)), 16, "0"),
            ).alias("payload"),
        )
    )
    return events.groupBy("group_key").agg(
        F.sum("amount").alias("amount_sum"),
        F.max("payload").alias("payload_max"),
    )


def summarize_partition(
    partition_id: int, rows: Iterable[Tuple[int, int, str]]
) -> Iterator[Tuple[int, int, int]]:
    """Consume the final DataFrame without returning all groups to the Driver."""
    count = 0
    for _ in rows:
        count += 1

    context = TaskContext.get()
    task_attempt = context.taskAttemptId() if context is not None else -1
    print(
        f"result_partition={partition_id} task_attempt={task_attempt} rows={count}",
        flush=True,
    )
    yield partition_id, task_attempt, count


def set_phase_config(aqe_enabled: bool, coalesce_enabled: bool) -> None:
    spark.conf.set("spark.sql.shuffle.partitions", str(INITIAL_SHUFFLE_PARTITIONS))
    spark.conf.set("spark.sql.adaptive.enabled", str(aqe_enabled).lower())
    spark.conf.set(
        "spark.sql.adaptive.coalescePartitions.enabled", str(coalesce_enabled).lower()
    )
    spark.conf.set(
        "spark.sql.adaptive.coalescePartitions.initialPartitionNum",
        str(INITIAL_SHUFFLE_PARTITIONS),
    )
    spark.conf.set(
        "spark.sql.adaptive.advisoryPartitionSizeInBytes", ADVISORY_PARTITION_SIZE
    )
    spark.conf.set("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1m")


def run_phase(label: str, aqe_enabled: bool, coalesce_enabled: bool) -> None:
    set_phase_config(aqe_enabled=aqe_enabled, coalesce_enabled=coalesce_enabled)
    result = build_aggregation()

    print(f"\n=== {label} ===", flush=True)
    print(
        "Phase settings: "
        f"aqe_enabled={spark.conf.get('spark.sql.adaptive.enabled')}, "
        "coalesce_enabled="
        f"{spark.conf.get('spark.sql.adaptive.coalescePartitions.enabled')}, "
        "shuffle_partitions="
        f"{spark.conf.get('spark.sql.shuffle.partitions')}, "
        "initial_partition_num="
        f"{spark.conf.get('spark.sql.adaptive.coalescePartitions.initialPartitionNum')}, "
        "advisory_partition_size="
        f"{spark.conf.get('spark.sql.adaptive.advisoryPartitionSizeInBytes')}, "
        "parallelism_first="
        f"{spark.conf.get('spark.sql.adaptive.coalescePartitions.parallelismFirst')}",
        flush=True,
    )
    print("Physical plan before action:", flush=True)
    result.explain(mode="formatted")

    sc.setJobDescription(label)
    summaries = result.rdd.mapPartitionsWithIndex(summarize_partition).collect()
    sc.setJobDescription(None)

    output_rows = sum(summary[2] for summary in summaries)
    if output_rows != KEY_CARDINALITY:
        raise RuntimeError(
            f"Expected {KEY_CARDINALITY} aggregated rows, received {output_rows}."
        )

    print(
        f"Completed {label}: output_rows={output_rows}, "
        f"result_partitions={len(summaries)}",
        flush=True,
    )
    print("Physical plan after action:", flush=True)
    result.explain(mode="formatted")


print(f"Driver UI: {sc.uiWebUrl}", flush=True)
print(
    "Experiment constants: "
    f"records={RECORD_COUNT}, source_partitions={SOURCE_PARTITIONS}, "
    f"key_cardinality={KEY_CARDINALITY}, "
    f"initial_shuffle_partitions={INITIAL_SHUFFLE_PARTITIONS}, "
    f"advisory_partition_size={ADVISORY_PARTITION_SIZE}",
    flush=True,
)
print(
    "All phases use uniformly distributed keys and disable shuffle compression "
    "only to make post-shuffle byte sizes easier to compare.",
    flush=True,
)

try:
    run_phase(
        "phase 1: AQE off, static 32 shuffle partitions",
        aqe_enabled=False,
        coalesce_enabled=False,
    )
    run_phase(
        "phase 2: AQE on, post-shuffle coalescing off",
        aqe_enabled=True,
        coalesce_enabled=False,
    )
    run_phase(
        "phase 3: AQE on, coalesce post-shuffle partitions",
        aqe_enabled=True,
        coalesce_enabled=True,
    )

    print(
        "All phases completed. Keep the application alive for "
        f"{OBSERVATION_SECONDS} seconds so Jobs, SQL plans, and Stage metrics remain "
        "available in the Driver UI.",
        flush=True,
    )
    time.sleep(OBSERVATION_SECONDS)
finally:
    spark.stop()
