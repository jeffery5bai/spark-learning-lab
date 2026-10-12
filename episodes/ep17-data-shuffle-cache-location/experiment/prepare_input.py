"""Create a small, reproducible Parquet input for Ep17."""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


INPUT_PATH = "/opt/ep17-shared/input-events"

spark = SparkSession.builder.appName("ep17-prepare-input").getOrCreate()

events = (
    spark.range(0, 80_000, numPartitions=8)
    .select(
        F.col("id").alias("event_id"),
        (F.col("id") % 20).cast("int").alias("key"),
        F.concat(F.lit("payload-"), F.col("id")).alias("payload"),
    )
)

events.repartition(8).write.mode("overwrite").parquet(INPUT_PATH)

part_files = sorted(Path(INPUT_PATH).glob("part-*.parquet"))
print(f"Wrote {len(part_files)} Parquet files to {INPUT_PATH}")
print(f"Rows: {events.count():,}")

spark.stop()
