import os
import socket
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

C = F.col


@F.udf(returnType=T.StringType())
def python_worker_identity(_: int) -> str:
    """在 Python UDF 執行的 worker process 中回傳位置資訊。"""
    return f"host={socket.gethostname()}, pid={os.getpid()}"


spark: SparkSession = (
    SparkSession.builder
    .appName("ep04-local-mode-platform-map")
    .master("local[2]")
    .getOrCreate()
)

try:
    sc = spark.sparkContext
    jvm_pid = sc._jvm.java.lang.ProcessHandle.current().pid()

    print("1. Driver 啟動位置")
    print(f"   Python driver host: {socket.gethostname()}")
    print(f"   Python driver PID: {os.getpid()}")
    print(f"   Python version: {sys.version.split()[0]}")
    print(f"   JVM host: {socket.gethostname()}")
    print(f"   JVM PID: {jvm_pid}")
    print(f"   Spark master: {sc.master}")

    print("\n2. 原生 DataFrame API：task 在 JVM 中執行")
    numbers = spark.range(1, 9, numPartitions=2)
    native_result = (
        numbers.withColumn("double", C("id") * F.lit(2))
        .withColumn("partition_id", F.spark_partition_id())
    )
    native_result.show()

    print("\n3. Python UDF：task 需要額外的 Python worker")
    udf_result = native_result.withColumn(
        "python_worker",
        python_worker_identity(C("id")),
    )
    udf_result.show(truncate=False)

    print("\n4. 結論")
    print("   原生 DataFrame API：Python driver 透過 Py4J 指示 JVM 執行 task")
    print("   Python UDF：JVM task 還需要把資料交給 Python worker 執行自訂函式")
finally:
    spark.stop()
