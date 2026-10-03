import shutil
from pathlib import Path
from tempfile import mkdtemp

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

C = F.col

spark: SparkSession = (
    SparkSession.builder.appName("ep08-output-partitions")
    .master("local[2]")
    .config("spark.ui.port", "4040")
    # 這次先關掉 AQE，讓寫入前 partition 數與 write task 的關係更直接。
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)


def parquet_files(output_path: Path) -> list[Path]:
    return sorted(path for path in output_path.rglob("*.parquet") if path.is_file())


def report_output(output_path: Path) -> None:
    files = parquet_files(output_path)
    total_bytes = sum(path.stat().st_size for path in files)
    print(f"   Parquet files: {len(files)}")
    print(f"   Total size: {total_bytes:,} bytes")
    for path in files:
        relative_path = path.relative_to(output_path)
        print(f"   - {relative_path} ({path.stat().st_size:,} bytes)")


def write_and_observe(
    label: str,
    dataframe: DataFrame,
    output_root: Path,
    partition_columns: tuple[str, ...] = (),
) -> None:
    output_path = output_root / label
    print(f"\n{label}")
    print(f"   Partitions before write: {dataframe.rdd.getNumPartitions()}")
    print("   Physical Plan：請特別找 Exchange 或 Coalesce")
    dataframe.explain(mode="simple")

    writer = dataframe.write.mode("overwrite").option("compression", "snappy")
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.parquet(str(output_path))
    report_output(output_path)


try:
    output_root = Path(mkdtemp(prefix="ep08-output-partitions-"))
    print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
    print(f"Temporary output root: {output_root}")
    print("AQE enabled: false")
    print(
        "\n這份資料刻意很小：今天觀察 task、檔案數與目錄布局；"
        "64–128 MiB 等檔案大小 heuristic 要在真實資料量上量測。"
    )

    print("\n1. 建立 6 個 source partitions 的訂單資料")
    orders = (
        spark.range(0, 600, numPartitions=6)
        .withColumn(
            "region",
            F.when(C("id") % 3 == 0, "TW").when(C("id") % 3 == 1, "JP").otherwise("US"),
        )
        .withColumn("amount", C("id") * F.lit(10))
    )
    print(f"   Source partitions: {orders.rdd.getNumPartitions()}")

    print("\n2. 不調整 partition 直接寫出：基準案例")
    write_and_observe("baseline", orders, output_root)
    input("\n請到 Spark UI 查看 write Job 的 task 數，再按 Enter 繼續。")

    print("\n3. repartition(12) 後寫出：多一次 shuffle，換取更多輸出 task")
    write_and_observe("repartition-12", orders.repartition(12), output_root)
    input("\n請找 Exchange 與 write stage：檔案數和 task 數如何改變？按 Enter 繼續。")

    print("\n4. coalesce(2) 後寫出：減少輸出 task，不新增 shuffle")
    write_and_observe("coalesce-2", orders.coalesce(2), output_root)
    input(
        "\n請確認 plan 的 Coalesce 與 UI：它和 repartition 的代價差在哪裡？按 Enter 繼續。"
    )

    print("\n5. repartition(6) 再 partitionBy(region)：一個輸出目錄可由多個 write task 寫入")
    print(
        "   這裡的 6 是 write task 的目標數，不是每個 region 目錄保證會有 6 個檔案。"
    )
    write_and_observe(
        "round-robin-then-partition-by-region",
        orders.repartition(6),
        output_root,
        ("region",),
    )
    input(
        "\n請查看輸出目錄：每個 region 有幾個 part file？"
        "想想看，什麼條件下它才會剛好等於 6？按 Enter 繼續。"
    )

    print(
        "\n6. 先依 region repartition，再 partitionBy(region)：讓寫入資料分布對準輸出 key"
    )
    write_and_observe(
        "repartition-by-region",
        orders.repartition(3, "region"),
        output_root,
        ("region",),
    )
    input(
        "\n請和上一步比較每個 region 目錄的檔案數，並在 UI 找這次為了對準 region 付出的 shuffle。"
        "按 Enter 清除暫存輸出並結束。"
    )
finally:
    if "output_root" in locals():
        shutil.rmtree(output_root, ignore_errors=True)
    spark.stop()
