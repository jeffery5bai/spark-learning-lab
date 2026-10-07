import os
import shutil
from datetime import datetime
from pathlib import Path
from tempfile import mkdtemp

from pyspark.sql import SparkSession

ICEBERG_VERSION = "1.5.2"
ICEBERG_RUNTIME = f"org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:{ICEBERG_VERSION}"
TABLE_NAME = "ice.analytics.events"

original_working_directory = Path.cwd()
workspace = Path(mkdtemp(prefix="ep12-iceberg-table-format-"))
warehouse_path = workspace / "warehouse"

# Keep Ivy's download cache and any JVM-side artifacts out of the repository.
os.chdir(workspace)

spark: SparkSession | None = None


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("ep12-iceberg-table-format")
        .master("local[2]")
        .config("spark.ui.port", "4040")
        .config("spark.jars.packages", ICEBERG_RUNTIME)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", str(warehouse_path))
        .getOrCreate()
    )


def append_rows(rows: list[tuple], columns: list[str]) -> None:
    dataframe = spark.createDataFrame(rows, columns)
    dataframe.writeTo(TABLE_NAME).append()


def show_snapshots() -> list[int]:
    snapshots = spark.sql(
        f"SELECT snapshot_id, committed_at, operation "
        f"FROM {TABLE_NAME}.snapshots ORDER BY committed_at"
    )
    snapshots.show(truncate=False)
    return [row.snapshot_id for row in snapshots.collect()]


def show_iceberg_files() -> None:
    table_root = warehouse_path / "analytics" / "events"
    print(f"\n   Files below: {table_root}")
    for file_path in sorted(table_root.rglob("*")):
        if file_path.is_file():
            print(f"   - {file_path.relative_to(table_root)}")


try:
    spark = create_spark()
    print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
    print(f"Iceberg runtime: {ICEBERG_RUNTIME}")
    print(f"Hadoop catalog warehouse: {warehouse_path}")
    print(
        "\nCatalog recap: ice is a Hadoop catalog in this experiment; it finds the current "
        "Iceberg metadata for ice.analytics.events."
    )

    print("\n1. 建立一張以 days(event_ts) 分區的 Iceberg table，並寫入第一批資料")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.analytics")
    spark.sql(
        f"CREATE TABLE {TABLE_NAME} ("
        "event_id BIGINT, event_ts TIMESTAMP, region STRING"
        ") USING iceberg PARTITIONED BY (days(event_ts))"
    )
    append_rows(
        [
            (1, datetime(2026, 10, 1, 9), "TW"),
            (2, datetime(2026, 10, 1, 10), "JP"),
        ],
        ["event_id", "event_ts", "region"],
    )
    print("   snapshots after first append")
    first_snapshot_id = show_snapshots()[0]
    show_iceberg_files()

    input(
        "\n請先查看 metadata/ 與 data/ 目錄：Iceberg table 會同時保存 metadata files 和 data files。"
        "按 Enter 做 schema evolution。"
    )

    print("\n2. schema evolution：新增 source 欄位，不重寫先前 data files")
    spark.sql(f"ALTER TABLE {TABLE_NAME} ADD COLUMN source STRING")
    append_rows(
        [(3, datetime(2026, 10, 2, 9), "US", "mobile")],
        ["event_id", "event_ts", "region", "source"],
    )
    spark.table(TABLE_NAME).orderBy("event_id").show()
    print("   snapshots after schema change and append")
    show_snapshots()

    input(
        "\n請觀察舊 rows 的 source 是 null，新資料有值。按 Enter 做 partition evolution。"
    )

    print(
        "\n3. partition evolution：新增 bucket(4, event_id) partition field，再寫入新資料"
    )
    spark.sql(f"ALTER TABLE {TABLE_NAME} ADD PARTITION FIELD bucket(4, event_id)")
    append_rows(
        [(4, datetime(2026, 10, 3, 9), "TW", "web")],
        ["event_id", "event_ts", "region", "source"],
    )
    print("   table partition fields")
    spark.sql(f"DESCRIBE TABLE {TABLE_NAME}").show(truncate=False)
    print("   Iceberg files metadata：新舊 data files 帶著不同 spec_id")
    spark.sql(
        f"SELECT spec_id, partition, record_count, file_path "
        f"FROM {TABLE_NAME}.files ORDER BY spec_id, file_path"
    ).show(truncate=False)
    print("   snapshots after partition evolution and append")
    show_snapshots()

    input(
        "\n請比較 files metadata 中的 spec_id。按 Enter 讀取第一個 snapshot，驗證 time travel。"
    )

    print("\n4. time travel：目前版本有 4 rows，舊 snapshot 保留第一批的 2 rows")
    current_rows = spark.table(TABLE_NAME).orderBy("event_id").collect()
    original_rows = (
        spark.read.option("snapshot-id", first_snapshot_id)
        .table(TABLE_NAME)
        .orderBy("event_id")
        .collect()
    )
    print(f"   current rows: {current_rows}")
    print(f"   rows at first snapshot {first_snapshot_id}: {original_rows}")

    input(
        "\n請確認 current table 和 first snapshot 讀到不同版本。按 Enter 清理本機實驗資料。"
    )
finally:
    if spark is not None:
        spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        spark.sql("DROP NAMESPACE IF EXISTS ice.analytics CASCADE")
        spark.stop()
    os.chdir(original_working_directory)
    shutil.rmtree(workspace, ignore_errors=True)
