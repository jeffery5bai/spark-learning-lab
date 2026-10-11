"""A deliberately observable Spark job for Ep16 task scheduling."""

import socket
import time

from pyspark import TaskContext
from pyspark.sql import SparkSession


def observe_partition(iterator):
    context = TaskContext.get()
    partition_id = context.partitionId()
    attempt = context.attemptNumber()
    host = socket.gethostname()

    print(
        f"Task for partition={partition_id}, attempt={attempt} "
        f"started on executor host={host}",
        flush=True,
    )
    time.sleep(15)
    yield from iterator


spark = SparkSession.builder.appName("ep16-task-observer").getOrCreate()
sc = spark.sparkContext

print(f"Driver hostname: {socket.gethostname()}")
print(f"Driver UI: {sc.uiWebUrl}")

result = (
    sc.parallelize(range(60), 6)
    .mapPartitions(observe_partition)
    .count()
)
print(f"Completed tasks and counted {result} records.")
print("Keeping the Driver alive for 120 seconds so completed UI evidence is observable.")
time.sleep(120)

spark.stop()
