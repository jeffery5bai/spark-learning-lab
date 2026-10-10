"""A deliberately long-running application for observing Spark client mode."""

import socket
import time

from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("ep15-client-driver").getOrCreate()

print(f"Driver hostname: {socket.gethostname()}")
print(f"Driver UI: {spark.sparkContext.uiWebUrl}")

result = (
    spark.range(0, 2_000_000, numPartitions=2)
    .repartition(2)
    .groupBy()
    .count()
    .collect()[0][0]
)
print(f"Completed a small action: {result:,} rows")
print("Keeping the application alive for 300 seconds so the Driver and Executor are observable.")
time.sleep(300)

spark.stop()
