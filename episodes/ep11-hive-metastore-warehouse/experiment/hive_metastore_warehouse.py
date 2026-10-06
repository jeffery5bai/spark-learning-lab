import os
import shutil
from pathlib import Path
from tempfile import mkdtemp
from urllib.parse import unquote, urlparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

C = F.col


original_working_directory = Path.cwd()
workspace = Path(mkdtemp(prefix="ep11-hive-metastore-warehouse-"))
warehouse_path = workspace / "warehouse"
metastore_path = workspace / "metastore_db"
external_events_path = workspace / "external-events"

# Keep Derby's log and all Hive artifacts inside this disposable workspace.
os.chdir(workspace)


def create_spark(app_name: str) -> SparkSession:
    """Create a Hive-enabled session that points at the same metastore and warehouse."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.ui.port", "4040")
        .config("spark.sql.warehouse.dir", str(warehouse_path))
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            f"jdbc:derby:{metastore_path};create=true",
        )
        .enableHiveSupport()
        .getOrCreate()
    )


def file_uri_to_path(location: str) -> Path:
    parsed = urlparse(location)
    if parsed.scheme != "file":
        raise ValueError(f"This local experiment expected a file URI, got: {location}")
    return Path(unquote(parsed.path))


def table_location(spark: SparkSession, table_name: str) -> Path:
    location = (
        spark.sql(f"DESCRIBE EXTENDED {table_name}")
        .filter(C("col_name") == "Location")
        .select("data_type")
        .first()[0]
    )
    return file_uri_to_path(location)


def show_table_metadata(spark: SparkSession, table_name: str) -> None:
    details = spark.sql(f"DESCRIBE EXTENDED {table_name}")
    details.filter(C("col_name").isin("Type", "Provider", "Location")).show(
        truncate=False
    )


def build_events(spark: SparkSession):
    return (
        spark.range(1, 13, numPartitions=2)
        .withColumn("event_date", F.lit("2026-10-06").cast("date"))
        .withColumn(
            "region",
            F.when(C("id") % 3 == 0, "TW").when(C("id") % 3 == 1, "JP").otherwise("US"),
        )
        .select(C("id").alias("event_id"), "event_date", "region")
    )


spark: SparkSession | None = None

try:
    print("Hive-first recap")
    print(
        "   Spark SQL → Hive-compatible catalog / metastore → metadata backend (Derby)"
    )
    print(
        "   metadata 的 Location → warehouse 或 external location → Parquet data files"
    )
    print(f"\nExperiment workspace: {workspace}")
    print(f"Warehouse directory: {warehouse_path}")
    print(f"Derby metastore: {metastore_path}")

    print("\n1. Session A：建立 database 與兩種 table")
    spark = create_spark("ep11-session-a")
    print(f"   Spark UI: {spark.sparkContext.uiWebUrl}")
    spark.sql("CREATE DATABASE analytics")
    events = build_events(spark)

    events.write.mode("overwrite").saveAsTable("analytics.managed_events")
    events.write.mode("overwrite").parquet(str(external_events_path))
    spark.sql(
        "CREATE TABLE analytics.external_events "
        "USING PARQUET "
        f"LOCATION '{external_events_path}'"
    )

    managed_location = table_location(spark, "analytics.managed_events")
    print("\n   DESCRIBE DATABASE EXTENDED analytics")
    spark.sql("DESCRIBE DATABASE EXTENDED analytics").show(truncate=False)
    print("   managed table metadata")
    show_table_metadata(spark, "analytics.managed_events")
    print("   external table metadata")
    show_table_metadata(spark, "analytics.external_events")
    print(f"   managed location exists: {managed_location.exists()}")
    print(f"   external location exists: {external_events_path.exists()}")

    spark.stop()
    spark = None
    input(
        "\nSession A 已停止。metadata 與 data files 都還留在 workspace。"
        "按 Enter 以 Session B 連到同一個 Derby metastore。"
    )

    print("\n2. Session B：同一份 metastore 讓 table name 可以被重新找到")
    spark = create_spark("ep11-session-b")
    print(f"   Spark UI: {spark.sparkContext.uiWebUrl}")
    spark.sql("SHOW DATABASES").show(truncate=False)
    spark.sql("SHOW TABLES IN analytics").show(truncate=False)
    print(f"   managed count: {spark.table('analytics.managed_events').count()}")
    print(f"   external count: {spark.table('analytics.external_events').count()}")

    input(
        "\n請確認：新的 SparkSession 沒有重建 table，卻仍能從 metastore 找到兩個名稱。"
        "按 Enter 比較 DROP TABLE 後的實體路徑。"
    )

    print("\n3. DROP TABLE 先移除 metadata；managed 與 external 的資料生命週期不同")
    spark.sql("DROP TABLE analytics.managed_events")
    print(f"   managed location exists after DROP TABLE: {managed_location.exists()}")

    spark.sql("DROP TABLE analytics.external_events")
    print(
        f"   external location exists after DROP TABLE: {external_events_path.exists()}"
    )

    input(
        "\n請比較兩個 exists 結果。按 Enter 重新登記外部資料路徑，確認 data files 仍可被讀取。"
    )

    print("\n4. external location 還在，可以重新註冊成一張 table")
    spark.sql(
        "CREATE TABLE analytics.external_events_recreated "
        "USING PARQUET "
        f"LOCATION '{external_events_path}'"
    )
    print(
        "   recreated external count: "
        f"{spark.table('analytics.external_events_recreated').count()}"
    )

    input(
        "\n這說明 external table metadata 與 data files 可以各自存在。按 Enter 清理本機實驗資料。"
    )
finally:
    if spark is not None:
        spark.sql("DROP DATABASE IF EXISTS analytics CASCADE")
        spark.stop()
    os.chdir(original_working_directory)
    shutil.rmtree(workspace, ignore_errors=True)
