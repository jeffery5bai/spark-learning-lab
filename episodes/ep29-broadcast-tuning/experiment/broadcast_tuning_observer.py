"""Compare static broadcast thresholds, an explicit hint, and AQE conversion."""

import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


FACT_ROWS = 1_000_000
DIMENSION_ROWS = 2_000
SOURCE_PARTITIONS = 8
LOW_THRESHOLD = "1k"
HIGH_THRESHOLD = "1m"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observation-seconds", type=int, default=900)
    args = parser.parse_args()
    if args.observation_seconds < 0:
        parser.error("--observation-seconds must be nonnegative")

    spark = SparkSession.builder.appName("ep29-broadcast-tuning").getOrCreate()
    sc = spark.sparkContext

    try:
        print(f"Driver UI: {sc.uiWebUrl}", flush=True)
        for key in (
            "spark.sql.autoBroadcastJoinThreshold",
            "spark.sql.adaptive.autoBroadcastJoinThreshold",
            "spark.sql.broadcastTimeout",
            "spark.sql.adaptive.enabled",
            "spark.sql.shuffle.partitions",
        ):
            if key == "spark.sql.adaptive.autoBroadcastJoinThreshold":
                configured_value = spark.conf.get(key, None)
                value = (
                    configured_value
                    if configured_value is not None
                    else "unset (inherits static threshold)"
                )
            else:
                value = spark.conf.get(key)
            print(f"Startup setting: {key}={value}", flush=True)
        print(
            "Experiment constants: "
            f"fact_rows={FACT_ROWS}, dimension_rows={DIMENSION_ROWS}, "
            f"source_partitions={SOURCE_PARTITIONS}, "
            f"low_threshold={LOW_THRESHOLD}, high_threshold={HIGH_THRESHOLD}",
            flush=True,
        )

        phases = (
            ("phase 1: low static threshold", False, LOW_THRESHOLD, "-1", False),
            ("phase 2: higher static threshold", False, HIGH_THRESHOLD, "-1", False),
            ("phase 3: low threshold with broadcast hint", False, LOW_THRESHOLD, "-1", True),
            ("phase 4: AQE converts shuffle join", True, "-1", HIGH_THRESHOLD, False),
        )
        for label, aqe, static_threshold, adaptive_threshold, use_hint in phases:
            spark.conf.set("spark.sql.adaptive.enabled", str(aqe).lower())
            spark.conf.set("spark.sql.autoBroadcastJoinThreshold", static_threshold)
            spark.conf.set(
                "spark.sql.adaptive.autoBroadcastJoinThreshold", adaptive_threshold
            )
            spark.conf.set("spark.sql.broadcastTimeout", "300")
            spark.conf.set("spark.sql.shuffle.partitions", "8")

            fact = spark.range(FACT_ROWS, numPartitions=SOURCE_PARTITIONS).select(
                (F.col("id") % F.lit(DIMENSION_ROWS)).cast("int").alias("product_id"),
                (F.col("id") % F.lit(97)).cast("long").alias("event_value"),
            )
            dimension = spark.range(
                DIMENSION_ROWS, numPartitions=SOURCE_PARTITIONS
            ).select(
                F.col("id").cast("int").alias("product_id"),
                F.sha2(F.concat(F.lit("product-"), F.col("id").cast("string")), 256)
                .alias("segment"),
            )
            dimension_estimate = int(
                dimension._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()
            )
            if not 1024 < dimension_estimate < 1024 * 1024:
                raise RuntimeError(
                    f"Dimension estimate {dimension_estimate} is outside the comparison thresholds."
                )

            build_side = F.broadcast(dimension) if use_hint else dimension
            joined = fact.join(build_side, "product_id", "inner")
            # Retain a dimension column in the executed query; count alone can prune it.
            result = joined.agg(
                F.count(F.lit(1)).alias("joined_rows"),
                F.sum("event_value").alias("event_sum"),
                F.sum(F.length("segment")).alias("segment_chars"),
            )
            print(f"\n=== {label} ===", flush=True)
            print(
                "Phase settings: "
                f"aqe={aqe}, static_threshold={static_threshold}, "
                f"adaptive_threshold={adaptive_threshold}, broadcast_hint={use_hint}, "
                "broadcast_timeout_seconds=300, shuffle_partitions=8",
                flush=True,
            )
            print(f"Dimension estimated size: {dimension_estimate} bytes", flush=True)
            initial_plan = result._jdf.queryExecution().executedPlan().toString()
            print("Physical plan before action:", flush=True)
            result.explain(mode="formatted")
            expected_initial = (
                "BroadcastHashJoin"
                if use_hint or label.startswith("phase 2")
                else "SortMergeJoin"
            )
            if expected_initial not in initial_plan:
                raise RuntimeError(f"{label}: expected {expected_initial} in initial plan")

            sc.setJobDescription(label)
            started_at = time.monotonic()
            try:
                summary = result.collect()[0]
            finally:
                sc.setJobDescription(None)
            elapsed = time.monotonic() - started_at
            whole_cycles, remaining = divmod(FACT_ROWS, 97)
            expected_sum = whole_cycles * sum(range(97)) + sum(range(remaining))
            if (
                summary["joined_rows"] != FACT_ROWS
                or summary["event_sum"] != expected_sum
                or summary["segment_chars"] != FACT_ROWS * 64
            ):
                raise RuntimeError(f"Unexpected join result: {summary}")

            # Adaptive plans include the initial plan as well as the final plan.
            final_plan = result._jdf.queryExecution().executedPlan().toString()
            final_section = final_plan.split("== Initial Plan ==", 1)[0]
            expected_final = (
                "SortMergeJoin" if label.startswith("phase 1") else "BroadcastHashJoin"
            )
            if expected_final not in final_section:
                raise RuntimeError(f"{label}: expected {expected_final} in final plan")
            print("Physical plan after action:", flush=True)
            result.explain(mode="formatted")
            print(
                f"Completed {label}: joined_rows={summary['joined_rows']}, "
                f"event_sum={summary['event_sum']}, segment_chars={summary['segment_chars']}, "
                f"initial_join={expected_initial}, final_join={expected_final}, "
                f"elapsed_seconds={elapsed:.2f}",
                flush=True,
            )

        print(
            f"All phases completed. Keeping the application alive for {args.observation_seconds} seconds.",
            flush=True,
        )
        time.sleep(args.observation_seconds)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
