"""Build a small shared Parquet input for the Ep22 diagnosis exercise."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


INPUT_PATH = "/opt/ep22-shared/input-events"
ROW_COUNT = 800_000
OUTPUT_PARTITIONS = 8


spark = SparkSession.builder.appName("ep22-prepare-input").getOrCreate()

events = (
    spark.range(ROW_COUNT, numPartitions=OUTPUT_PARTITIONS)
    .select(
        F.col("id"),
        (F.col("id") % F.lit(100)).cast("int").alias("category"),
        (F.col("id") % F.lit(40)).cast("int").alias("value"),
    )
)

events.repartition(OUTPUT_PARTITIONS).write.mode("overwrite").parquet(INPUT_PATH)

print(
    f"Wrote {ROW_COUNT:,} rows as {OUTPUT_PARTITIONS} Parquet output partitions "
    f"to {INPUT_PATH}."
)

spark.stop()
