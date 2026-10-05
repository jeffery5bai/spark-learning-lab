import os
import shutil
from pathlib import Path
from tempfile import mkdtemp
from urllib.parse import unquote, urlparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

C = F.col


original_working_directory = Path.cwd()
workspace = Path(mkdtemp(prefix="ep10-catalog-hive-tables-"))
warehouse_path = workspace / "warehouse"
metastore_path = workspace / "metastore_db"
external_events_path = workspace / "external-events"

# Embedded Derby otherwise writes derby.log into the directory that launched the script.
os.chdir(workspace)

spark: SparkSession = (
    SparkSession.builder.appName("ep10-catalog-hive-tables")
    .master("local[2]")
    .config("spark.ui.port", "4040")
    .config("spark.sql.warehouse.dir", str(warehouse_path))
    # Keep the embedded Derby Hive metastore inside this disposable experiment workspace.
    .config(
        "spark.hadoop.javax.jdo.option.ConnectionURL",
        f"jdbc:derby:{metastore_path};create=true",
    )
    .enableHiveSupport()
    .getOrCreate()
)


def show_table_metadata(table_name: str) -> None:
    """Print the metadata fields that connect a table name to its data location."""
    print(f"\n   DESCRIBE EXTENDED {table_name}")
    details = spark.sql(f"DESCRIBE EXTENDED {table_name}")
    details.filter(
        C("col_name").isin("Type", "Provider", "Location")
    ).show(truncate=False)


def show_files(path: Path) -> None:
    """Show a small, readable view of the files behind one table location."""
    print(f"   Files below: {path}")
    for file_path in sorted(path.rglob("*")):
        if file_path.is_file():
            print(f"   - {file_path.relative_to(path)} ({file_path.stat().st_size} bytes)")


def file_uri_to_path(location: str) -> Path:
    """Convert Spark's local file: URI from table metadata into a Python Path."""
    parsed = urlparse(location)
    if parsed.scheme != "file":
        raise ValueError(f"This local experiment expected a file URI, got: {location}")
    return Path(unquote(parsed.path))


try:
    print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
    print(f"Experiment workspace: {workspace}")
    print(f"Warehouse directory: {warehouse_path}")
    print("Catalog implementation: Hive (embedded Derby metastore for this local experiment)")

    print("\n1. 建立一份資料：現在它還沒有 table name")
    events = (
        spark.range(1, 13, numPartitions=2)
        .withColumn("event_date", F.lit("2026-10-05").cast("date"))
        .withColumn(
            "region",
            F.when(C("id") % 3 == 0, "TW")
            .when(C("id") % 3 == 1, "JP")
            .otherwise("US"),
        )
        .withColumn("event_type", F.when(C("id") % 2 == 0, "view").otherwise("click"))
        .select(C("id").alias("event_id"), "event_date", "region", "event_type")
    )
    events.printSchema()
    events.show()

    input(
        "\n目前 events 只是 DataFrame。按 Enter 建立 database 與兩張內容相同、但管理方式不同的表。"
    )

    print("\n2. Catalog 用 database／table name 記錄資料集")
    spark.sql("CREATE DATABASE IF NOT EXISTS analytics")

    # A managed table lets Spark choose and own its location under the warehouse directory.
    events.write.mode("overwrite").saveAsTable("analytics.managed_events")

    # An external table records a pre-existing, explicitly chosen data location.
    events.write.mode("overwrite").parquet(str(external_events_path))
    spark.sql(
        "CREATE TABLE analytics.external_events "
        "USING PARQUET "
        f"LOCATION '{external_events_path}'"
    )

    print("\n   SHOW DATABASES")
    spark.sql("SHOW DATABASES").show(truncate=False)
    print("   SHOW TABLES IN analytics")
    spark.sql("SHOW TABLES IN analytics").show(truncate=False)

    input(
        "\n請先看這兩個名稱。它們是 catalog 裡的 table entries，不是資料檔名。按 Enter 繼續。"
    )

    print("\n3. 同樣是 table name，managed 與 external 的 location 誰管理？")
    show_table_metadata("analytics.managed_events")
    show_table_metadata("analytics.external_events")

    input(
        "\n請比較兩個 Location：managed table 位於 warehouse；external table 指向明確指定的路徑。"
        "按 Enter 用 spark.table() 讀回資料。"
    )

    print("\n4. spark.table() 先查 catalog metadata，再到 Location 讀取資料")
    spark.table("analytics.managed_events").orderBy("event_id").show()
    print("\n   Physical Plan：目前只需找 Scan parquet；Parquet 的讀取細節會留到 Ep13。")
    spark.table("analytics.managed_events").select("region").explain(mode="simple")

    input(
        "\n請在 Spark UI 的 SQL／Jobs 頁查看讀表的 action。按 Enter 查看 table 背後真正的檔案。"
    )

    print("\n5. table metadata 最後仍會連到實體 data files")
    managed_location = file_uri_to_path(
        spark.sql("DESCRIBE EXTENDED analytics.managed_events")
        .filter(C("col_name") == "Location")
        .select("data_type")
        .first()[0]
    )
    show_files(managed_location)
    show_files(external_events_path)

    input(
        "\n到這裡為止，我們先知道 table name 如何找到 data files。按 Enter 清理此實驗的暫存資料。"
    )
finally:
    spark.sql("DROP DATABASE IF EXISTS analytics CASCADE")
    spark.stop()
    os.chdir(original_working_directory)
    shutil.rmtree(workspace, ignore_errors=True)
