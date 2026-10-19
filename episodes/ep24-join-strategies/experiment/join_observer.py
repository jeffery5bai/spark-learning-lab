"""Compare broadcast and sort-merge joins using the same synthetic tables."""

import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


FACT_ROWS = 1_000_000
DIMENSION_ROWS = 2_000
SOURCE_PARTITIONS = 8
SHUFFLE_PARTITIONS = 8
OBSERVATION_SECONDS = 420


def show_physical_plan(label: str, dataframe: DataFrame) -> None:
    print(f"\n===== {label}: formatted physical plan =====", flush=True)
    dataframe.explain(mode="formatted")
    print(f"===== end {label}: formatted physical plan =====\n", flush=True)


def run_phase(dataframe: DataFrame, phase_name: str) -> None:
    sc.setJobDescription(phase_name)
    row_count = dataframe.count()
    sc.setJobDescription(None)
    print(f"Completed {phase_name}: joined row count={row_count}", flush=True)
    print(
        f"Keep the Driver UI open for {OBSERVATION_SECONDS} seconds. "
        f"Capture the completed {phase_name} Job, SQL plan, and Stage metrics now.",
        flush=True,
    )
    time.sleep(OBSERVATION_SECONDS)


spark = SparkSession.builder.appName("ep24-join-strategies").getOrCreate()
sc = spark.sparkContext

fact = (
    spark.range(FACT_ROWS, numPartitions=SOURCE_PARTITIONS)
    .select(
        (F.col("id") % F.lit(DIMENSION_ROWS)).cast("int").alias("product_id"),
        (F.col("id") % F.lit(97)).cast("int").alias("event_value"),
    )
)
dimension = (
    spark.range(DIMENSION_ROWS, numPartitions=SOURCE_PARTITIONS)
    .select(
        F.col("id").cast("int").alias("product_id"),
        F.when(F.col("id") % F.lit(4) == F.lit(0), F.lit("featured"))
        .otherwise(F.lit("standard"))
        .alias("segment"),
    )
)

print(f"Driver UI: {sc.uiWebUrl}", flush=True)
print(
    "Experiment constants: "
    f"fact_rows={FACT_ROWS}, dimension_rows={DIMENSION_ROWS}, "
    f"source_partitions={SOURCE_PARTITIONS}, shuffle_partitions={SHUFFLE_PARTITIONS}",
    flush=True,
)

broadcast_join = fact.join(F.broadcast(dimension), on="product_id", how="inner")
show_physical_plan("broadcast phase", broadcast_join)
run_phase(broadcast_join, "broadcast: BroadcastHashJoin with small dimension")

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
shuffle_join = fact.join(dimension, on="product_id", how="inner")
show_physical_plan("shuffle phase", shuffle_join)
run_phase(shuffle_join, "shuffle: SortMergeJoin with both sides repartitioned")

spark.stop()
