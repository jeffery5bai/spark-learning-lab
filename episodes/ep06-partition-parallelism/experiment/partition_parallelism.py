from pyspark.sql import SparkSession
from pyspark.sql import functions as F

C = F.col

spark: SparkSession = (
    SparkSession.builder
    .appName("ep06-partition-parallelism")
    .master("local[2]")
    .config("spark.ui.port", "4040")
    .getOrCreate()
)


def inspect_partitions(num_partitions: int) -> None:
    print(f"\n建立 {num_partitions} 個 partition 的 DataFrame")
    numbers = (
        spark.range(1, 13, numPartitions=num_partitions)
        .where(C("id") % 2 == 0)
        .select(F.spark_partition_id().alias("partition_id"), "id")
    )

    print(f"   DataFrame partitions: {numbers.rdd.getNumPartitions()}")
    rows = numbers.collect()
    print(f"   collect() 收到 {len(rows)} 筆資料: {rows}")


try:
    print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
    print(f"Application ID: {spark.sparkContext.applicationId}")
    print("Spark master: local[2]")
    print(f"Effective default parallelism: {spark.sparkContext.defaultParallelism}")

    print("\n1. 先觀察兩個 partition 對應的 task 數")
    inspect_partitions(num_partitions=2)
    input(
        "\n請到 Spark UI 查看第一個 Job：一個 Stage 有幾個 task？"
        "完成後按 Enter 繼續。"
    )

    print("\n2. 將來源改成六個 partition，再執行相同處理")
    inspect_partitions(num_partitions=6)
    input(
        "\n請回到 Spark UI 查看第二個 Job：task 數如何改變？"
        "也請觀察 Event Timeline；完成後按 Enter 結束 application。"
    )
finally:
    spark.stop()
