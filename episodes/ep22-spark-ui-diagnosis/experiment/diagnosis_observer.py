"""Create one scan-and-shuffle job whose completed Spark UI can be inspected."""

import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


INPUT_PATH = "/opt/ep22-shared/input-events"
SHUFFLE_PARTITIONS = 8
OBSERVATION_SECONDS = 420


spark = SparkSession.builder.appName("ep22-spark-ui-diagnosis").getOrCreate()
sc = spark.sparkContext

events = spark.read.parquet(INPUT_PATH)
summary = (
    events.filter(F.col("value") >= 10)
    .repartition(SHUFFLE_PARTITIONS, "category")
    .groupBy("category")
    .agg(
        F.count("*").alias("row_count"),
        F.sum("value").alias("value_sum"),
    )
)

print(f"Driver UI: {sc.uiWebUrl}", flush=True)
print(f"Input files discovered: {len(events.inputFiles())}", flush=True)
print(f"Requested shuffle partitions: {SHUFFLE_PARTITIONS}", flush=True)

result = summary.collect()
print(f"Completed aggregation for {len(result)} categories.", flush=True)
print(
    "The action has completed. Keep the application alive for "
    f"{OBSERVATION_SECONDS} seconds so the completed Job, Stages, task timeline, "
    "and Executor metrics remain available in the Driver UI.",
    flush=True,
)
time.sleep(OBSERVATION_SECONDS)

spark.stop()
