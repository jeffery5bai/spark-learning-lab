"""Write equivalent small-file and compacted Parquet inputs for file scan planning."""

import argparse
import json
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


ROW_COUNT = 160_000
SMALL_FILES = 160
COMPACT_FILES = 8
DEFAULT_DATA_ROOT = "/opt/ep30-shared"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default=DEFAULT_DATA_ROOT)
    args = parser.parse_args()
    root = Path(args.data_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    run_root = Path(tempfile.mkdtemp(prefix="input-", dir=root))

    spark = SparkSession.builder.appName("ep30-prepare-file-scan-input").getOrCreate()
    try:
        events = spark.range(ROW_COUNT, numPartitions=8).select(
            "id",
            (F.col("id") % F.lit(97)).cast("int").alias("value"),
            F.sha2(F.concat(F.lit("event-"), F.col("id").cast("string")), 256)
            .alias("payload"),
        )
        small_path = run_root / "small-files"
        compact_path = run_root / "compact-files"
        events.repartition(SMALL_FILES).write.parquet(str(small_path))
        spark.read.schema(events.schema).parquet(str(small_path)).repartition(
            COMPACT_FILES
        ).write.parquet(str(compact_path))

        datasets = {}
        for name, path, expected_files in (
            ("small", small_path, SMALL_FILES),
            ("compact", compact_path, COMPACT_FILES),
        ):
            files = sorted(path.glob("*.parquet"))
            if len(files) != expected_files:
                raise RuntimeError(f"{name}: expected {expected_files} files, got {len(files)}")
            datasets[name] = {
                "relative_path": str(path.relative_to(root)),
                "files": len(files),
                "bytes": sum(file.stat().st_size for file in files),
            }
            print(
                f"Prepared input: dataset={name}, rows={ROW_COUNT}, "
                f"files={len(files)}, bytes={datasets[name]['bytes']}",
                flush=True,
            )

        manifest = {"row_count": ROW_COUNT, "datasets": datasets}
        pending = run_root / "manifest.json"
        pending.write_text(json.dumps(manifest, indent=2) + "\n")
        pending.replace(root / "prepared-input.json")
        print("Prepared input manifest: prepared-input.json", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
