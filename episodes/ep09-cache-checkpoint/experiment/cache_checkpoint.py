import shutil
from pathlib import Path
from tempfile import mkdtemp

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

C = F.col

spark: SparkSession = (
    SparkSession.builder.appName("ep09-cache-checkpoint")
    .master("local[2]")
    .config("spark.ui.port", "4040")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.shuffle.partitions", "6")
    .getOrCreate()
)


def build_orders() -> DataFrame:
    """Return a lazy DataFrame with a visible shuffle in its lineage."""
    return (
        spark.range(0, 1_200, numPartitions=4)
        .withColumn("customer_id", C("id") % 24)
        .withColumn(
            "region",
            F.when(C("id") % 3 == 0, "TW").when(C("id") % 3 == 1, "JP").otherwise("US"),
        )
        .withColumn("amount", C("id") * F.lit(10))
        .repartition(6, "customer_id")
        .withColumn("amount_with_fee", C("amount") * F.lit(1.05))
        .select("customer_id", "region", "amount_with_fee")
    )


def regional_summary(dataframe: DataFrame) -> DataFrame:
    return dataframe.groupBy("region").agg(
        F.sum("amount_with_fee").alias("total_amount")
    )


def show_plan(label: str, dataframe: DataFrame) -> None:
    print(f"\n{label}")
    print("   Physical Plan：請找 Exchange、InMemoryTableScan 或 ExistingRDD")
    dataframe.explain(mode="simple")


def run_summary(label: str, dataframe: DataFrame) -> None:
    rows = regional_summary(dataframe).collect()
    print(f"   {label}: {rows}")


checkpoint_root = Path(mkdtemp(prefix="ep09-checkpoints-"))

try:
    spark.sparkContext.setCheckpointDir(str(checkpoint_root))
    print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
    print(f"Checkpoint directory: {checkpoint_root}")
    print("AQE enabled: false")
    print("Shuffle partitions: 6")

    print("\n1. 建立一條含 shuffle 的 lazy lineage")
    print(
        "   來源是 4 個 partitions；依 customer_id repartition 後是 6 個 partitions。"
    )

    print("\n2. 不使用 cache：兩個 action 都會重跑相同的上游 lineage")
    uncached_orders = build_orders()
    show_plan("   uncached orders", uncached_orders)
    print(f"   count: {uncached_orders.count()}")
    input(
        "\n請到 Spark UI 查看第一個 Job：找來源 4 tasks 與依 customer_id shuffle 後的 stage。"
        "按 Enter 執行第二個 action。"
    )
    run_summary("regional summary", uncached_orders)
    input(
        "\n請比較第二個 Job：它是否又從來源資料和 customer_id shuffle 開始？按 Enter 繼續。"
    )

    print("\n3. 使用 cache：第一個 action materialize，第二個 action 讀取快取結果")
    cached_orders = build_orders().cache()
    print(f"   cache storage level: {cached_orders.storageLevel}")
    print("   cache() 是 persist() 使用 DataFrame 預設 storage level 的簡寫。")
    print(f"   count: {cached_orders.count()}")
    input(
        "\n請到 Storage 頁確認 cached dataset，並查看這個 materialization Job。按 Enter 繼續。"
    )
    show_plan("   summary over cached orders", regional_summary(cached_orders))
    run_summary("regional summary", cached_orders)
    input(
        "\n請查看第二個 action 的 Job：是否出現 InMemoryTableScan，且不再重跑來源 lineage？按 Enter 繼續。"
    )
    cached_orders.unpersist()
    print("   cached dataset 已 unpersist。")

    print(
        "\n4. 使用 checkpoint：eager checkpoint 先 materialize，並切斷上游 lineage"
    )
    checkpointed_orders = build_orders().checkpoint(eager=True)
    print("   checkpoint(eager=True) 已觸發一個 checkpoint Job。")
    show_plan("   checkpointed orders", checkpointed_orders)
    input(
        "\n請查看 checkpoint Job：它先完成來源與 shuffle；之後的 DataFrame 已不再保留上游 plan。"
        "按 Enter 執行下一個 action。"
    )
    run_summary("regional summary", checkpointed_orders)
    input(
        "\n請比較 checkpoint 後的 Job：它從 ExistingRDD 開始，而不是重跑原始來源。"
        "按 Enter 結束 application。"
    )
finally:
    shutil.rmtree(checkpoint_root, ignore_errors=True)
    spark.stop()
