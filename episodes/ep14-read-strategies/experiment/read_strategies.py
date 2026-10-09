import shutil
from pathlib import Path
from tempfile import mkdtemp

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

C = F.col

workspace = Path(mkdtemp(prefix="ep14-read-strategies-"))
events_path = workspace / "events"
spark: SparkSession | None = None


def build_events(spark: SparkSession) -> DataFrame:
    """Create three date partitions with deliberately different amount ranges."""
    date_index = (C("id") % 3).cast("int")
    amount_within_date = F.floor(C("id") / 3 % 50).cast("double")

    return (
        spark.range(0, 3_000, numPartitions=3)
        .withColumn("event_date", F.date_add(F.lit("2026-10-01"), date_index))
        .withColumn("amount", amount_within_date + date_index * F.lit(50.0))
        .withColumn(
            "region",
            F.when(C("id") % 3 == 0, "TW")
            .when(C("id") % 3 == 1, "JP")
            .otherwise("US"),
        )
        .withColumn(
            "event_type",
            F.when(C("id") % 4 == 0, "view")
            .when(C("id") % 4 == 1, "click")
            .when(C("id") % 4 == 2, "add_to_cart")
            .otherwise("purchase"),
        )
        .withColumn(
            "description",
            F.concat(
                F.lit("A deliberately repeated event description for row "),
                (C("id") % 20).cast("string"),
            ),
        )
        .select("id", "event_date", "region", "event_type", "amount", "description")
    )


def show_data_files(path: Path) -> None:
    files = sorted(
        file_path
        for file_path in path.rglob("*.parquet")
        if file_path.is_file() and not file_path.name.startswith(".")
    )
    print(f"\n   Parquet data files below: {path}")
    for file_path in files:
        print(f"   - {file_path.relative_to(path)}")


try:
    spark = (
        SparkSession.builder.appName("ep14-read-strategies")
        .master("local[2]")
        .config("spark.ui.port", "4040")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
    print(f"Workspace: {workspace}")

    events = build_events(spark)
    print("\n1. 建立三個 event_date partitions，各自有不同的 amount 範圍")
    (
        events.groupBy("event_date")
        .agg(
            F.count("*").alias("rows"),
            F.min("amount").alias("min_amount"),
            F.max("amount").alias("max_amount"),
        )
        .orderBy("event_date")
        .show()
    )

    (
        events.repartition(3, "event_date")
        .write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(str(events_path))
    )
    show_data_files(events_path)

    input(
        "\n請確認 event_date 目錄與各日期的 amount 範圍。"
        "按 Enter 從檔案讀取，觀察只選 region 時的 scan plan。"
    )

    print("\n2. column pruning：只選 region，Parquet scan 被要求讀哪些欄位？")
    events_from_files = spark.read.parquet(str(events_path))
    region_only = events_from_files.select("region")
    region_only.explain(mode="formatted")
    region_only.show(5, truncate=False)

    input(
        "\n請在 Scan parquet 區塊找 ReadSchema。"
        "按 Enter 加上 event_date 條件，觀察讀取哪些 partition。"
    )

    print("\n3. partition pruning：只查 2026-10-03，scan plan 排除了哪些目錄？")
    one_date = (
        events_from_files.filter(C("event_date") == "2026-10-03")
        .select("region", "amount")
    )
    one_date.explain(mode="formatted")
    one_date.groupBy().agg(
        F.count("*").alias("rows"),
        F.min("amount").alias("min_amount"),
        F.max("amount").alias("max_amount"),
    ).show()

    input(
        "\n請在 Scan parquet 區塊找 PartitionFilters，並確認結果只有 1,000 rows、"
        "amount 為 100.0 到 149.0。按 Enter 用 amount 條件觀察 Parquet reader。"
    )

    print("\n4. predicate pushdown：amount >= 140 的條件是否傳給 Parquet reader？")
    high_amount = (
        events_from_files.filter(C("amount") >= 140)
        .select("region", "amount", F.input_file_name().alias("source_file"))
    )
    high_amount.explain(mode="formatted")
    high_amount.groupBy("source_file").agg(
        F.count("*").alias("rows"),
        F.min("amount").alias("min_amount"),
        F.max("amount").alias("max_amount"),
    ).show(truncate=False)

    input(
        "\n請在 Scan parquet 區塊找 PushedFilters，並查看結果來自哪個 source_file。"
        "按 Enter 比較關閉與開啟 Parquet filter pushdown 的讀取量。"
    )

    print("\n5. file / row-group skipping：到 Spark UI 比較同一條件的 Input Size")
    print("   先關閉 spark.sql.parquet.filterPushdown，執行 amount >= 140。")
    spark.conf.set("spark.sql.parquet.filterPushdown", "false")
    spark.sparkContext.setJobGroup(
        "ep14-no-pushdown",
        "Ep14: amount >= 140 with Parquet filter pushdown disabled",
    )
    without_pushdown = events_from_files.filter(C("amount") >= 140).count()
    print(f"   pushdown disabled: {without_pushdown} matching rows")

    print("   再開啟 spark.sql.parquet.filterPushdown，執行完全相同的條件。")
    spark.conf.set("spark.sql.parquet.filterPushdown", "true")
    spark.sparkContext.setJobGroup(
        "ep14-with-pushdown",
        "Ep14: amount >= 140 with Parquet filter pushdown enabled",
    )
    with_pushdown = events_from_files.filter(C("amount") >= 140).count()
    print(f"   pushdown enabled:  {with_pushdown} matching rows")
    print(f"   Spark UI remains available at: {spark.sparkContext.uiWebUrl}")

    input(
        "\n請開啟 Spark UI 的 Jobs 或 Stages，比較剛才兩個 count job 的 Input Size。"
        "按 Enter 清理本機實驗資料。"
    )
finally:
    if spark is not None:
        spark.stop()
    shutil.rmtree(workspace, ignore_errors=True)
