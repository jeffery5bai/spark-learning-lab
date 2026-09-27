from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

C = F.col

spark: SparkSession = (
    SparkSession.builder
    .appName("ep02-local-mode-lifecycle")
    .master("local[2]")
    .getOrCreate()
)


def explain_plan(title: str, dataframe: DataFrame) -> None:
    """印出同一段 transformation 在 Catalyst 各階段的計畫。"""
    print(f"\n{'=' * 72}\n{title}\n{'=' * 72}")
    dataframe.explain(mode="extended")


print("1. 建立訂單明細 DataFrame（尚未觸發 Spark job）")
transactions = spark.createDataFrame(
    [
        (1001, 1, "Ada", "TW", 80),
        (1002, 2, "Ben", "US", 120),
        (1003, 3, "Cindy", "TW", 200),
        (1004, 4, "David", "TW", 150),
    ],
    ["order_id", "customer_id", "name", "region", "amount"],
)

print("2. 描述 transformation：先算服務費後金額，再只保留大額訂單")
large_orders_with_fee = (
    transactions.withColumn("amount_with_fee", C("amount") * F.lit(1.05))
    .where(C("amount") >= 100)
    .select("order_id", "name", "amount", "amount_with_fee")
)

explain_plan(
    "3-A. filter 能否移到計算服務費之前？",
    large_orders_with_fee,
)

only_id = transactions.withColumn(
    "debug_label", F.lit("this column is never used")
).select("order_id", "customer_id")

explain_plan(
    "3-B. 沒有被使用的衍生欄位，會留在計畫裡嗎？",
    only_id,
)

customers = transactions.select("customer_id", "name", "region")
orders = transactions.select("order_id", "customer_id", "amount")

tw_large_orders = (
    customers.where(C("region") == "TW")
    .join(orders.where(C("amount") >= 100), on="customer_id")
    .select("customer_id", "name", "amount")
)

explain_plan(
    "3-C. join 前的條件，能否各自先套用到兩側資料？",
    tw_large_orders,
)

print("\n4. 第一次 action：count()，Spark 現在才真正執行 large_orders_with_fee")
print(f"Row count: {large_orders_with_fee.count()}")

print("\n5. 第二次 action：show()，沒有 cache 時會再次執行所需計畫")
large_orders_with_fee.show()

spark.stop()
