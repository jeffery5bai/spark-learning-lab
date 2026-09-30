from pyspark.sql import SparkSession
from pyspark.sql import functions as F

C = F.col

spark: SparkSession = (
    SparkSession.builder
    .appName("ep05-dag-lazy-execution")
    .master("local[2]")
    .config("spark.ui.port", "4040")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

try:
    print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
    print(f"Application ID: {spark.sparkContext.applicationId}\n")
    print("1. 建立兩個 partition 的來源 DataFrame")
    orders = (
        spark.range(1, 13, numPartitions=2)
        .withColumn(
            "region",
            F.when(C("id") % 3 == 0, "TW").when(C("id") % 3 == 1, "US").otherwise("JP"),
        )
        .withColumn("amount", C("id") * F.lit(25))
    )

    print("2. 描述 narrow transformations：篩選與計算")
    cleaned_orders = (
        orders.where(C("amount") >= 100)
        .withColumn("amount_with_fee", C("amount") * F.lit(1.05))
        .select("region", "amount_with_fee")
    )

    print("3. 描述 wide transformation：依地區彙總")
    regional_summary = cleaned_orders.groupBy("region").agg(
        F.sum("amount_with_fee").alias("total_amount")
    )

    print("\n4. action 前的 RDD lineage（尚未執行資料處理）")
    print(regional_summary.rdd.toDebugString().decode("utf-8"))

    print("\n5. action：show()，Spark 現在才開始執行 DAG")
    regional_summary.show()

    input("\n可到 Spark UI 查看 Job 與 Stages。完成觀察後按 Enter 結束 application。")
finally:
    spark.stop()
