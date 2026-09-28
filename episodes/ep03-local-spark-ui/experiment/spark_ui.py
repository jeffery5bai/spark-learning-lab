from pyspark.sql import SparkSession
from pyspark.sql import functions as F

C = F.col

spark: SparkSession = (
    SparkSession.builder
    .appName("ep03-local-spark-ui")
    .master("local[2]")
    .config("spark.ui.port", "4040")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

try:
    print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
    print(f"Application ID: {spark.sparkContext.applicationId}")
    print("\n1. 建立訂單明細 DataFrame")
    transactions = spark.createDataFrame(
        [
            (1001, "TW", 80),
            (1002, "TW", 120),
            (1003, "US", 200),
            (1004, "US", 150),
            (1005, "JP", 90),
            (1006, "JP", 110),
            (1007, "TW", 300),
            (1008, "US", 180),
        ],
        ["order_id", "region", "amount"],
    )

    print("2. 描述 transformation：保留大額訂單，再依地區彙總")
    regional_revenue = (
        transactions.where(C("amount") >= 100)
        .groupBy("region")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("amount").alias("total_amount"),
        )
    )

    print("3. 執行 action：show()")
    regional_revenue.show()

    input(
        "\nJob 已完成。請在瀏覽器開啟上方 Spark UI URL，"
        "查看 Jobs、Stages 與 task；完成後按 Enter 結束 application。"
    )
finally:
    spark.stop()
