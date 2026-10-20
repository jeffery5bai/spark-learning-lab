"""Write identical Parquet datasets with eight and 160 output files."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SHARED_ROOT = "/opt/ep25-shared"
ROW_COUNT = 960_000
SOURCE_PARTITIONS = 8
LARGE_FILE_PARTITIONS = 8
SMALL_FILE_PARTITIONS = 160


spark = SparkSession.builder.appName("ep25-prepare-small-files-input").getOrCreate()

events = (
    spark.range(ROW_COUNT, numPartitions=SOURCE_PARTITIONS)
    .select(
        F.col("id"),
        (F.col("id") % F.lit(97)).cast("int").alias("value"),
        (F.col("id") % F.lit(31)).cast("int").alias("category"),
    )
)

events.repartition(LARGE_FILE_PARTITIONS).write.mode("overwrite").parquet(
    f"{SHARED_ROOT}/large-files"
)
events.repartition(SMALL_FILE_PARTITIONS).write.mode("overwrite").parquet(
    f"{SHARED_ROOT}/small-files"
)

print(
    f"Wrote {ROW_COUNT:,} identical rows as {LARGE_FILE_PARTITIONS} and "
    f"{SMALL_FILE_PARTITIONS} requested Parquet output partitions.",
    flush=True,
)

spark.stop()
