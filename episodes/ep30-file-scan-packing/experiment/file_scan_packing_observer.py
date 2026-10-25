"""Compare file scan packing settings with unchanged files and a compacted input."""

import argparse
import json
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DEFAULT_DATA_ROOT = "/opt/ep30-shared"
SCHEMA = "id long, value int, payload string"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default=DEFAULT_DATA_ROOT)
    parser.add_argument("--observation-seconds", type=int, default=900)
    args = parser.parse_args()
    if args.observation_seconds < 0:
        parser.error("--observation-seconds must be nonnegative")
    root = Path(args.data_root).resolve()
    manifest_path = root / "prepared-input.json"
    if not manifest_path.is_file():
        parser.error("Missing prepared-input.json; run prepare_input.py first")
    manifest = json.loads(manifest_path.read_text())
    rows = manifest["row_count"]
    cycles, remaining = divmod(rows, 97)
    expected = (rows, rows * (rows - 1) // 2,
                cycles * sum(range(97)) + sum(range(remaining)), rows * 64)

    spark = SparkSession.builder.appName("ep30-file-scan-packing").getOrCreate()
    sc = spark.sparkContext
    try:
        print(f"Driver UI: {sc.uiWebUrl}", flush=True)
        for key in (
            "spark.sql.files.maxPartitionBytes",
            "spark.sql.files.openCostInBytes",
            "spark.sql.files.minPartitionNum",
            "spark.sql.adaptive.enabled",
        ):
            value = spark.conf.get(key)
            if key == "spark.sql.files.minPartitionNum" and value is None:
                value = "unset (uses leaf-node default parallelism)"
            print(f"Startup setting: {key}={value}", flush=True)
        print(f"Default parallelism: {sc.defaultParallelism}", flush=True)
        print(
            f"Experiment constants: rows={rows}, "
            f"small_files={manifest['datasets']['small']['files']}, "
            f"compact_files={manifest['datasets']['compact']['files']}",
            flush=True,
        )

        # Disable AQE only to keep the final, one-row aggregation easy to compare.
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        phases = (
            ("phase 1: tiny scan budget", "small", 1, 4, 4),
            ("phase 2: larger scan budget", "small", 128, 4, 4),
            ("phase 3: higher estimated open cost", "small", 128, 16, 4),
            ("phase 4: request more scan parallelism", "small", 128, 4, 32),
            ("phase 5: compacted files", "compact", 128, 4, 4),
        )
        planned_counts = []
        for label, dataset_name, max_mib, open_mib, min_partitions in phases:
            spark.conf.set("spark.sql.files.maxPartitionBytes", f"{max_mib}m")
            spark.conf.set("spark.sql.files.openCostInBytes", f"{open_mib}m")
            spark.conf.set("spark.sql.files.minPartitionNum", str(min_partitions))
            metadata = manifest["datasets"][dataset_name]
            path = (root / metadata["relative_path"]).resolve()
            try:
                path.relative_to(root)
            except ValueError as exc:
                raise RuntimeError("Input path must be inside data root") from exc
            files = sorted(path.glob("*.parquet"))
            input_bytes = sum(file.stat().st_size for file in files)
            if (len(files), input_bytes) != (metadata["files"], metadata["bytes"]):
                raise RuntimeError("Prepared input changed; run prepare_input.py again")
            max_bytes = max_mib * 1024 * 1024
            open_bytes = open_mib * 1024 * 1024
            # Spark 3.4.4 FilePartition.maxSplitBytes includes estimated file open costs.
            split_budget = min(
                max_bytes,
                max(open_bytes, (input_bytes + len(files) * open_bytes) // min_partitions),
            )
            events = spark.read.schema(SCHEMA).parquet(str(path))
            input_files = len(events.inputFiles())
            planned = events.rdd.getNumPartitions()
            if input_files != metadata["files"]:
                raise RuntimeError("Spark discovered an unexpected number of input files")
            if dataset_name == "small" and max(file.stat().st_size for file in files) >= 1024 * 1024:
                raise RuntimeError("This controlled experiment requires files smaller than 1 MiB")
            planned_counts.append(planned)
            print(f"\n=== {label} ===", flush=True)
            print(
                f"Phase settings: maxPartitionBytes={max_mib}m, "
                f"openCostInBytes={open_mib}m, minPartitionNum={min_partitions}, aqe=false",
                flush=True,
            )
            print(
                f"Scan planning: dataset={dataset_name}, input_files={input_files}, "
                f"input_bytes={input_bytes}, computed_max_split_bytes={split_budget}, "
                f"planned_scan_partitions={planned}",
                flush=True,
            )
            result = events.agg(
                F.count(F.lit(1)).alias("rows"),
                F.sum("id").alias("id_sum"),
                F.sum("value").alias("value_sum"),
                F.sum(F.length("payload")).alias("payload_chars"),
            )
            sc.setJobDescription(label)
            started = time.monotonic()
            try:
                summary = result.collect()[0]
            finally:
                sc.setJobDescription(None)
            elapsed = time.monotonic() - started
            if tuple(summary) != expected:
                raise RuntimeError(f"{label}: unexpected result {summary}")
            print("Physical plan:", flush=True)
            result.explain(mode="formatted")
            print(
                f"Completed {label}: input_files={input_files}, scan_partitions={planned}, "
                f"rows={summary['rows']}, id_sum={summary['id_sum']}, "
                f"value_sum={summary['value_sum']}, payload_chars={summary['payload_chars']}, "
                f"elapsed_seconds={elapsed:.2f}",
                flush=True,
            )

        tiny, larger, high_open, high_parallelism, compact = planned_counts
        if not (tiny > larger and high_open > larger and high_parallelism > larger):
            raise RuntimeError(f"Unexpected scan planning comparisons: {planned_counts}")
        if not compact < larger:
            raise RuntimeError(f"Compacted input did not reduce scan partitions: {planned_counts}")
        print(f"Scan partition comparison: {planned_counts}", flush=True)
        print(
            f"All phases completed. Keeping the application alive for {args.observation_seconds} seconds.",
            flush=True,
        )
        time.sleep(args.observation_seconds)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
