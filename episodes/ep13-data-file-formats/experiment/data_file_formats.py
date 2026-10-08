import shutil
from pathlib import Path
from tempfile import mkdtemp

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

C = F.col
FORMATS = ("csv", "json", "parquet", "orc")

workspace = Path(mkdtemp(prefix="ep13-data-file-formats-"))
spark: SparkSession | None = None


def build_events(spark: SparkSession) -> DataFrame:
    """Build typed event rows, including struct, map, and array columns."""
    return (
        spark.range(0, 2_400, numPartitions=2)
        .withColumn(
            "event_ts",
            F.to_timestamp(
                F.concat(
                    F.lit("2026-10-"),
                    F.lpad(((C("id") % 5) + 1).cast("string"), 2, "0"),
                    F.lit(" 09:00:00"),
                )
            ),
        )
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
        .withColumn("amount", F.round((C("id") % 100) * F.lit(1.25), 2))
        .withColumn(
            "context",
            F.struct(
                F.when(C("id") % 2 == 0, "ios")
                .otherwise("android")
                .alias("device"),
                F.concat(F.lit("1."), (C("id") % 4).cast("string")).alias(
                    "app_version"
                ),
            ),
        )
        .withColumn(
            "attributes",
            F.create_map(
                F.lit("campaign"),
                F.concat(F.lit("campaign-"), (C("id") % 8).cast("string")),
                F.lit("channel"),
                F.when(C("id") % 2 == 0, "push").otherwise("organic"),
            ),
        )
        .withColumn(
            "tags",
            F.array(
                F.lit("experiment-a"),
                F.when(C("id") % 2 == 0, "returning").otherwise("new"),
            ),
        )
        .select(
            "id",
            "event_ts",
            "region",
            "event_type",
            "amount",
            "context",
            "attributes",
            "tags",
        )
    )


def flatten_for_csv(events: DataFrame) -> DataFrame:
    """Make the information loss for CSV explicit rather than implicit."""
    return events.select(
        "id",
        "event_ts",
        "region",
        "event_type",
        "amount",
        C("context.device").alias("context_device"),
        C("context.app_version").alias("context_app_version"),
        F.to_json(C("attributes")).alias("attributes_json"),
        F.to_json(C("tags")).alias("tags_json"),
    )


def demonstrate_csv_limitation(events: DataFrame) -> None:
    """CSV has no native encoding for Spark's complex data types."""
    direct_csv_path = workspace / "csv-direct-attempt"
    try:
        events.write.mode("overwrite").option("header", True).csv(str(direct_csv_path))
    except Exception as error:
        error_message = str(error).splitlines()[0]
        print(f"   direct CSV write: {error_message}")


def write_format(events: DataFrame, file_format: str) -> Path:
    output_path = workspace / file_format
    dataframe = flatten_for_csv(events) if file_format == "csv" else events
    writer = dataframe.write.mode("overwrite").format(file_format)
    if file_format == "csv":
        writer.option("header", True).save(str(output_path))
    else:
        writer.save(str(output_path))
    return output_path


def data_files(path: Path) -> list[Path]:
    return sorted(
        file_path
        for file_path in path.rglob("*")
        if file_path.is_file() and not file_path.name.startswith((".", "_"))
    )


def show_layout(file_format: str, path: Path) -> None:
    files = data_files(path)
    total_bytes = sum(file_path.stat().st_size for file_path in files)
    print(f"\n   {file_format}: {len(files)} data files, {total_bytes:,} bytes")
    for file_path in files:
        print(f"   - {file_path.relative_to(workspace)} ({file_path.stat().st_size:,} bytes)")


def read_format(spark: SparkSession, file_format: str, path: Path) -> DataFrame:
    reader = spark.read.format(file_format)
    if file_format == "csv":
        reader = reader.option("header", True)
    return reader.load(str(path))


try:
    spark = (
        SparkSession.builder.appName("ep13-data-file-formats")
        .master("local[2]")
        .config("spark.ui.port", "4040")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
    print(f"Workspace: {workspace}")

    events = build_events(spark)
    print("\n1. 建立含 struct、map、array 的 typed events DataFrame")
    events.printSchema()
    events.show(5, truncate=False)

    input(
        "\n請先確認 context、attributes、tags 的型別。按 Enter 嘗試直接寫入 CSV。"
    )

    print("\n2. CSV 沒有 complex type 的原生表示；先觀察直接寫入的結果")
    demonstrate_csv_limitation(events)
    print("   接著把 struct 攤平、map 與 array 轉成 JSON string，才寫出 CSV。")

    paths = {file_format: write_format(events, file_format) for file_format in FORMATS}
    for file_format, path in paths.items():
        show_layout(file_format, path)

    input(
        "\n請比較副檔名、目錄結構與檔案大小。按 Enter 從各格式重新讀取資料。"
    )

    print("\n3. 從檔案重新讀取：nested schema 是否能直接保留？")
    reread: dict[str, DataFrame] = {}
    for file_format, path in paths.items():
        dataframe = read_format(spark, file_format, path)
        reread[file_format] = dataframe
        print(f"\n   {file_format} schema")
        dataframe.printSchema()
        print(f"   {file_format} count: {dataframe.count()}")

    input(
        "\n請比較 CSV、JSON 與 Parquet、ORC 的 schema。按 Enter 看只選一個 nested 欄位時的 scan plan。"
    )

    print("\n4. 同一個 nested 欄位選取：Physical Plan 要求 scan 讀取哪些欄位？")
    for file_format, dataframe in reread.items():
        column = "context_device" if file_format == "csv" else "context.device"
        print(f"\n   {file_format}: select({column}).explain('formatted')")
        dataframe.select(column).explain(mode="formatted")

    input(
        "\n請在 plan 裡找 ReadSchema 或 ReadSchema-like 資訊。按 Enter 清理本機實驗資料。"
    )
finally:
    if spark is not None:
        spark.stop()
    shutil.rmtree(workspace, ignore_errors=True)
