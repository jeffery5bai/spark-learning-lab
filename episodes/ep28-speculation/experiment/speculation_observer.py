"""Observe Spark speculative execution with a temporary and persistent straggler."""

import os
import time
from typing import Iterator, Tuple

from pyspark import TaskContext
from pyspark.sql import SparkSession


PARTITION_COUNT = 8
FAST_TASK_SECONDS = 1.0
SLOW_TASK_SECONDS = 12.0
BACKUP_TASK_SECONDS = 0.2
OBSERVATION_SECONDS = 420


spark = SparkSession.builder.appName("ep28-speculation").getOrCreate()
sc = spark.sparkContext


def run_partition(
    partition_id: int, mode: str
) -> Iterator[Tuple[int, int, str, float, str]]:
    """Make partition zero slow according to the selected straggler shape."""
    context = TaskContext.get()
    if context is None:
        raise RuntimeError("TaskContext is required inside a Spark task.")

    attempt = context.attemptNumber()
    executor_id = os.environ.get("SPARK_EXECUTOR_ID", "unknown")

    if partition_id != 0:
        delay = FAST_TASK_SECONDS
        reason = "normal work"
    elif mode == "temporary" and attempt == 0:
        delay = SLOW_TASK_SECONDS
        reason = "temporary delay on the original attempt"
    elif mode == "temporary":
        delay = BACKUP_TASK_SECONDS
        reason = "backup attempt after the temporary delay"
    elif mode == "persistent":
        delay = SLOW_TASK_SECONDS
        reason = "persistent slow work on every attempt"
    else:
        raise ValueError(f"Unknown mode: {mode}")

    print(
        "task_started "
        f"mode={mode} partition={partition_id} attempt={attempt} "
        f"executor={executor_id} delay_seconds={delay} reason={reason}",
        flush=True,
    )
    time.sleep(delay)
    print(
        "task_finished "
        f"mode={mode} partition={partition_id} attempt={attempt} "
        f"executor={executor_id}",
        flush=True,
    )
    yield partition_id, attempt, executor_id, delay, reason


def run_case(label: str, mode: str) -> None:
    sc.setJobDescription(label)
    started_at = time.monotonic()
    results = (
        sc.parallelize(range(PARTITION_COUNT), PARTITION_COUNT)
        .mapPartitionsWithIndex(lambda partition_id, _: run_partition(partition_id, mode))
        .collect()
    )
    elapsed_seconds = time.monotonic() - started_at
    sc.setJobDescription(None)

    by_partition = {partition_id: result for partition_id, *result in results}
    if len(by_partition) != PARTITION_COUNT:
        raise RuntimeError(
            f"Expected {PARTITION_COUNT} winning task results, found {len(by_partition)}."
        )

    slow_partition = by_partition[0]
    print(
        f"Completed {label}: elapsed_seconds={elapsed_seconds:.1f}, "
        f"partition_0_winning_attempt={slow_partition[0]}, "
        f"winning_executor={slow_partition[1]}, "
        f"winning_delay_seconds={slow_partition[2]}, "
        f"winning_reason={slow_partition[3]}",
        flush=True,
    )


print(f"Driver UI: {sc.uiWebUrl}", flush=True)
print(
    "Experiment constants: "
    f"partitions={PARTITION_COUNT}, fast_task_seconds={FAST_TASK_SECONDS}, "
    f"slow_task_seconds={SLOW_TASK_SECONDS}, "
    f"backup_task_seconds={BACKUP_TASK_SECONDS}",
    flush=True,
)
print(
    "Speculation settings: "
    f"enabled={sc.getConf().get('spark.speculation')}, "
    f"interval={sc.getConf().get('spark.speculation.interval')}, "
    f"multiplier={sc.getConf().get('spark.speculation.multiplier')}, "
    f"quantile={sc.getConf().get('spark.speculation.quantile')}, "
    f"min_task_runtime={sc.getConf().get('spark.speculation.minTaskRuntime')}",
    flush=True,
)
print(
    "Both cases use the same eight partitions and four task slots. "
    "Only partition zero changes: the temporary case is fast for a backup attempt; "
    "the persistent case remains slow for every attempt.",
    flush=True,
)

try:
    run_case(
        "case 1: temporary straggler, speculative backup can win",
        mode="temporary",
    )
    run_case(
        "case 2: persistent slow work, speculative backup repeats the cost",
        mode="persistent",
    )
    print(
        "Both cases completed. Keep the application alive for "
        f"{OBSERVATION_SECONDS} seconds so completed Stage details remain available "+
        "in the Driver UI.",
        flush=True,
    )
    time.sleep(OBSERVATION_SECONDS)
finally:
    spark.stop()
