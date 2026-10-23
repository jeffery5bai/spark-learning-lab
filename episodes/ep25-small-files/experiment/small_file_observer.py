"""Compare scan tasks for identical Parquet data with few and many files."""

import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


SHARED_ROOT = "/opt/ep25-shared"
OBSERVATION_SECONDS = 420


def run_phase(path: str, phase_name: str) -> None:
    events: DataFrame = spark.read.parquet(path).select("value")
    discovered_files = len(events.inputFiles())
    planned_scan_partitions = events.rdd.getNumPartitions()

    print(
        f"{phase_name}: files discovered={discovered_files}, "
        f"planned scan partitions={planned_scan_partitions}",
        flush=True,
    )
    sc.setJobDescription(phase_name)
    value_sum = events.agg(F.sum("value").alias("value_sum")).collect()[0]["value_sum"]
    sc.setJobDescription(None)
    print(f"Completed {phase_name}: value_sum={value_sum}", flush=True)
    print(
        f"Keeping the application alive for {OBSERVATION_SECONDS} seconds.",
        flush=True,
    )
    time.sleep(OBSERVATION_SECONDS)


spark = SparkSession.builder.appName("ep25-small-files").getOrCreate()
sc = spark.sparkContext

print(f"Driver UI: {sc.uiWebUrl}", flush=True)
print(
    "Scan planning settings: "
    f"maxPartitionBytes={spark.conf.get('spark.sql.files.maxPartitionBytes')}, "
    f"openCostInBytes={spark.conf.get('spark.sql.files.openCostInBytes')}",
    flush=True,
)

run_phase(f"{SHARED_ROOT}/large-files", "large-files: scan 8 Parquet files")
run_phase(f"{SHARED_ROOT}/small-files", "small-files: scan 160 Parquet files")

spark.stop()
