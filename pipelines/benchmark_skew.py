#!/usr/bin/env python3
"""Benchmark no-salt, full-salt, and hot-only-salt two-stage aggregation.

Experiment contract
-------------------
* Input: the persisted DWD Parquet table (default 1,000,000 rows).
* All three variants use the same two-stage aggregation shape.  The treatment
  difference is the salt scope: none, all items, or only the hot item.
* Fixed Spark settings: 16 shuffle partitions and AQE disabled.  The settings
  are printed into the result JSON and must not drift between approaches.
* Each approach runs twice; the second run is the reported measurement.
* The longest task is collected from a real JVM SparkListener.  PySpark 3.5
  has no Python-side SparkListener base class, so a small Java listener is
  compiled against the installed Spark jars and attached through py4j.

The report also records actual distinct ``(item_id, _salt)`` cardinality, not
just the theoretical maximum.  Bucket sensitivity is measured separately by
``pipelines/benchmark_salt_sweep.py``.

If listener compilation/loading fails, the benchmark still runs and records
``collection_method="fallback_total_time_only"`` with null listener metrics.
It never invents a task-duration number.

Usage:
    python pipelines/benchmark_skew.py
    python pipelines/benchmark_skew.py --salt-buckets 10 --shuffle-partitions 16
    python pipelines/benchmark_salt_sweep.py --salt-buckets 2 5 20
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Importing common configures JAVA_HOME/PYSPARK_PYTHON/winutils before the JVM
# starts.  It does not create a SparkSession.
from spark_jobs.common import (  # noqa: E402
    BENCHMARK_RESULTS_PATH,
    DWD_PATH,
    build_spark_session,
    ensure_dwd_exists,
    fs_path,
)
from spark_jobs.skew_aggregation import add_salt_key, build_item_stats  # noqa: E402
from pyspark.sql import DataFrame, SparkSession, functions as F  # noqa: E402
from pyspark.storagelevel import StorageLevel  # noqa: E402

LISTENER_SOURCE = PROJECT_ROOT / "spark_jobs" / "jvm" / "TaskTimingCollector.java"
LISTENER_JAR = PROJECT_ROOT / ".bootstrap" / "listener" / "SparkInsightListener.jar"
LISTENER_CLASSES = PROJECT_ROOT / ".bootstrap" / "listener" / "classes"
RUNS_PER_APPROACH = 2
SALT_BUCKETS = 10
SHUFFLE_PARTITIONS = 16
HOT_ITEM_ID = 9999
APPROACH_SCOPES = {
    "unsalted_two_phase": "none",
    "salted_two_phase": "all",
    "salted_hot_only_two_phase": "hot_only",
}


def _find_java_tool(name: str) -> str | None:
    java_home = os.environ.get("JAVA_HOME")
    suffix = ".exe" if os.name == "nt" else ""
    if java_home:
        candidate = Path(java_home) / "bin" / f"{name}{suffix}"
        if candidate.exists():
            return str(candidate)
    return shutil.which(name)


def ensure_listener_jar() -> tuple[Path | None, str | None]:
    """Compile the tiny Java SparkListener if needed.

    Returns ``(jar_or_none, error_or_none)``.  A compile failure is reported to
    stderr but does not stop the wall-clock benchmark; the output then records
    the explicit fallback collection method.
    """

    if not LISTENER_SOURCE.exists():
        return None, f"listener source missing: {LISTENER_SOURCE}"

    if LISTENER_JAR.exists() and LISTENER_JAR.stat().st_mtime >= LISTENER_SOURCE.stat().st_mtime:
        return LISTENER_JAR, None

    javac = _find_java_tool("javac")
    jar_tool = _find_java_tool("jar")
    if not javac or not jar_tool:
        return None, "javac/jar not found; set JAVA_HOME to a JDK 17 installation"

    try:
        import pyspark

        spark_jars = Path(pyspark.__file__).resolve().parent / "jars"
        classpath = str(spark_jars / "*")
        LISTENER_JAR.parent.mkdir(parents=True, exist_ok=True)
        if LISTENER_CLASSES.exists():
            shutil.rmtree(LISTENER_CLASSES)
        LISTENER_CLASSES.mkdir(parents=True, exist_ok=True)
        compile_cmd = [
            javac,
            "-encoding",
            "UTF-8",
            "-cp",
            classpath,
            "-d",
            str(LISTENER_CLASSES),
            str(LISTENER_SOURCE),
        ]
        subprocess.run(compile_cmd, check=True, capture_output=True, text=True)
        jar_cmd = [jar_tool, "cf", str(LISTENER_JAR), "-C", str(LISTENER_CLASSES), "."]
        subprocess.run(jar_cmd, check=True, capture_output=True, text=True)
    except Exception as exc:  # noqa: BLE001 - report the real compiler error
        detail = ""
        if isinstance(exc, subprocess.CalledProcessError):
            detail = f" stdout={exc.stdout!r} stderr={exc.stderr!r}"
        return None, f"{type(exc).__name__}: {exc}{detail}"
    return LISTENER_JAR, None


def enable_listener_on_driver(jar: Path) -> None:
    """Prepend the listener jar to the JVM driver classpath.

    PySpark starts its JVM from ``spark-submit`` when the first SparkSession is
    created.  Setting PYSPARK_SUBMIT_ARGS before that point is the supported
    way for the driver JVM to see a class that is not inside PySpark's own
    jars.  This must happen before build_spark_session().
    """

    jar_arg = str(jar).replace("\\", "/")
    existing = os.environ.get("PYSPARK_SUBMIT_ARGS", "").strip()
    if not existing:
        existing = "pyspark-shell"
    elif "pyspark-shell" not in existing:
        existing += " pyspark-shell"
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f'--driver-class-path "{jar_arg}" {existing}'
    )


def attach_listener(spark: SparkSession):
    """Attach the compiled Java SparkListener, or return (None, reason)."""

    try:
        listener = spark._jvm.com.sparkinsight.TaskTimingCollector()
        spark.sparkContext._jsc.sc().addSparkListener(listener)
        return listener, None
    except Exception as exc:  # noqa: BLE001 - fallback must remain observable
        return None, f"{type(exc).__name__}: {exc}"


def collect_listener_metrics(listener) -> dict:
    return {
        "task_count": int(listener.getTaskCount()),
        "failed_task_count": int(listener.getFailedTaskCount()),
        "longest_task_ms": int(listener.getMaxTaskDurationMs()),
        "longest_task_stage_id": int(listener.getMaxTaskStageId()),
        "longest_task_task_id": int(listener.getMaxTaskTaskId()),
        "sum_task_duration_ms": int(listener.getTotalTaskDurationMs()),
        "shuffle_read_bytes": int(listener.getShuffleReadBytes()),
        "shuffle_write_bytes": int(listener.getShuffleWriteBytes()),
        "shuffle_read_records": int(listener.getShuffleReadRecords()),
        "shuffle_write_records": int(listener.getShuffleWriteRecords()),
        "stage_stats": json.loads(listener.getStageStatsJson()),
    }


def measure_once(
    spark: SparkSession,
    source: DataFrame,
    *,
    salt_scope: str,
    salt_buckets: int,
    hot_item_id: int | None,
    run_index: int,
) -> dict:
    listener, listener_error = attach_listener(spark)
    started = time.perf_counter()
    output_rows = 0
    try:
        aggregate = build_item_stats(
            source,
            salt_scope=salt_scope,
            salt_buckets=salt_buckets,
            hot_item_id=hot_item_id,
        )
        # Collect the complete 20,000-row result rather than calling count().
        # Spark's Count optimizer can push down/eliminate parts of the second
        # aggregation and would not exercise the full two-stage plan.  collect()
        # forces every item metric to be produced and transferred, which is the
        # workload the benchmark claims to compare.  The result is only ~20k
        # rows, so driver memory is not a confounder.
        result_rows = aggregate.collect()
        output_rows = len(result_rows)
        elapsed = time.perf_counter() - started
        if listener is not None:
            # Listener events are delivered asynchronously by Spark's listener
            # bus.  Give the bus a short, deterministic drain window.
            time.sleep(0.5)
            metrics = collect_listener_metrics(listener)
        else:
            metrics = {
                "task_count": None,
                "failed_task_count": None,
                "longest_task_ms": None,
                "longest_task_stage_id": None,
                "longest_task_task_id": None,
                "sum_task_duration_ms": None,
                "shuffle_read_bytes": None,
                "shuffle_write_bytes": None,
                "shuffle_read_records": None,
                "shuffle_write_records": None,
                "stage_stats": None,
            }
    finally:
        if listener is not None:
            try:
                spark.sparkContext._jsc.sc().removeSparkListener(listener)
            except Exception:  # noqa: BLE001 - cleanup must not mask benchmark result
                pass

    if listener is not None and metrics["task_count"]:
        collection_method = (
            "SparkListener: com.sparkinsight.TaskTimingCollector."
            "onTaskEnd -> TaskInfo.duration()"
        )
        task_over_60s = metrics["longest_task_ms"] > 60_000
    else:
        collection_method = "fallback_total_time_only"
        task_over_60s = None

    return {
        "run_index": run_index,
        "salted": salt_scope != "none",
        "salt_scope": salt_scope,
        "hot_item_id": hot_item_id if salt_scope == "hot_only" else None,
        "wall_clock_sec": round(elapsed, 3),
        "output_rows": output_rows,
        **metrics,
        "task_over_60s": task_over_60s,
        "collection_method": collection_method,
        "listener_error": listener_error,
    }


def js_mb(value: int | None) -> float | None:
    return None if value is None else round(value / 1024 / 1024, 3)


def pct_change(baseline: float | int | None, treatment: float | int | None) -> float | None:
    if baseline in (None, 0) or treatment is None:
        return None
    return round((treatment - baseline) / baseline * 100, 2)


def build_comparison(
    baseline: dict, treatment: dict, *, treatment_prefix: str
) -> dict:
    """Build the same delta block for every salted treatment."""

    return {
        "unsalted_wall_clock_sec": baseline["wall_clock_sec"],
        f"{treatment_prefix}_wall_clock_sec": treatment["wall_clock_sec"],
        "wall_clock_change_pct": pct_change(
            baseline["wall_clock_sec"], treatment["wall_clock_sec"]
        ),
        "unsalted_longest_task_ms": baseline["longest_task_ms"],
        f"{treatment_prefix}_longest_task_ms": treatment["longest_task_ms"],
        "longest_task_change_pct": pct_change(
            baseline["longest_task_ms"], treatment["longest_task_ms"]
        ),
        "unsalted_shuffle_read_mb": js_mb(baseline["shuffle_read_bytes"]),
        f"{treatment_prefix}_shuffle_read_mb": js_mb(
            treatment["shuffle_read_bytes"]
        ),
        "unsalted_shuffle_write_mb": js_mb(baseline["shuffle_write_bytes"]),
        f"{treatment_prefix}_shuffle_write_mb": js_mb(
            treatment["shuffle_write_bytes"]
        ),
    }


def load_benchmark_source(spark: SparkSession) -> tuple[DataFrame, dict]:
    """Load, filter, cache, and profile the shared DWD input."""

    source = (
        spark.read.parquet(fs_path(DWD_PATH))
        .select("user_id", "item_id", "behavior_type", "event_time")
        .filter(
            F.col("user_id").isNotNull()
            & F.col("item_id").isNotNull()
            & F.col("event_time").isNotNull()
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    input_rows = source.count()
    distinct_items = source.select("item_id").distinct().count()
    hot_rows = source.filter(F.col("item_id") == HOT_ITEM_ID).count()
    source.count()  # Ensure the selected columns are fully materialized.

    if input_rows <= 0:
        raise AssertionError("benchmark input is empty")
    if hot_rows <= 0:
        raise ValueError(f"hot item {HOT_ITEM_ID} not found in DWD")

    print(
        f"[benchmark] input rows={input_rows:,} distinct_items={distinct_items:,} "
        f"hot_item={HOT_ITEM_ID} hot_rows={hot_rows:,} "
        f"hot_share={hot_rows / input_rows:.2%}",
        file=sys.stderr,
    )
    return source, {
        "path": fs_path(DWD_PATH),
        "rows": input_rows,
        "distinct_items": distinct_items,
        "hot_item_id": HOT_ITEM_ID,
        "hot_item_rows": hot_rows,
        "hot_item_share_pct": round(hot_rows / input_rows * 100, 2),
    }


def distinct_group_keys(
    source: DataFrame,
    *,
    salt_scope: str,
    salt_buckets: int,
    hot_item_id: int | None,
) -> int:
    """Count the actual logical stage-1 groups, not the theoretical maximum."""

    if salt_scope == "none":
        return source.select("item_id").distinct().count()
    salted = add_salt_key(
        source,
        salt_scope=salt_scope,
        salt_buckets=salt_buckets,
        hot_item_id=hot_item_id,
    )
    return salted.select("item_id", "_salt").distinct().count()


def run_approach_set(
    spark: SparkSession,
    source: DataFrame,
    *,
    salt_buckets: int,
    input_stats: dict,
) -> dict:
    """Run all three approaches twice and return one comparable result block."""

    hot_item_id = input_stats["hot_item_id"]
    distinct_items = input_stats["distinct_items"]
    run_records: dict[str, list[dict]] = {
        name: [] for name in APPROACH_SCOPES
    }

    # Interleave the approaches within each round.  Run 1 is warm-up; run 2 is
    # the reported measurement.  The order is fixed so reruns are comparable.
    sequence = [
        (name, scope, run_index)
        for run_index in range(1, RUNS_PER_APPROACH + 1)
        for name, scope in APPROACH_SCOPES.items()
    ]
    for name, scope, run_index in sequence:
        print(
            f"[benchmark] running {name} run {run_index}/{RUNS_PER_APPROACH} ...",
            file=sys.stderr,
            flush=True,
        )
        record = measure_once(
            spark,
            source,
            salt_scope=scope,
            salt_buckets=salt_buckets,
            hot_item_id=hot_item_id if scope == "hot_only" else None,
            run_index=run_index,
        )
        run_records[name].append(record)
        print(
            f"[benchmark] {name} run {run_index}: "
            f"wall_clock_sec={record['wall_clock_sec']} "
            f"longest_task_ms={record['longest_task_ms']} "
            f"task_count={record['task_count']} "
            f"output_rows={record['output_rows']}",
            file=sys.stderr,
            flush=True,
        )
        if record["stage_stats"]:
            compact = ", ".join(
                f"stage {sid}: tasks={s['task_count']} "
                f"max_ms={s['longest_task_ms']}"
                for sid, s in record["stage_stats"].items()
            )
            print(
                f"[benchmark] {name} run {run_index} stages: {compact}",
                file=sys.stderr,
                flush=True,
            )

    measured = {
        name: runs[1] for name, runs in run_records.items()
    }
    for name, row in measured.items():
        if row["output_rows"] != distinct_items:
            raise AssertionError(
                "aggregation output row count does not equal distinct item count: "
                f"{name}={row['output_rows']}, distinct_items={distinct_items}"
            )

    group_cardinality = {}
    for name, scope in APPROACH_SCOPES.items():
        if scope == "none":
            max_possible = distinct_items
        elif scope == "all":
            max_possible = distinct_items * salt_buckets
        else:
            # Non-hot items keep one salt value; the hot item gets N buckets.
            max_possible = distinct_items + salt_buckets - 1
        group_cardinality[name] = {
            "salt_scope": scope,
            "actual": distinct_group_keys(
                source,
                salt_scope=scope,
                salt_buckets=salt_buckets,
                hot_item_id=hot_item_id if scope == "hot_only" else None,
            ),
            "max_possible": max_possible,
        }

    unsalted = measured["unsalted_two_phase"]
    salted = measured["salted_two_phase"]
    hot_only = measured["salted_hot_only_two_phase"]
    return {
        "approaches": {
            "unsalted_two_phase": {
                "description": (
                    "Stage 1 groupBy(item_id), stage 2 groupBy(item_id); "
                    "no salt, same two-stage shape as each treatment."
                ),
                "salt_scope": "none",
                "runs": run_records["unsalted_two_phase"],
                "measured_run": 2,
                "measured": unsalted,
            },
            "salted_two_phase": {
                "description": (
                    "Stage 1 groupBy(item_id, deterministic_salt), stage 2 "
                    "groupBy(item_id) summing partial aggregates; every item "
                    "is salted."
                ),
                "salt_scope": "all",
                "runs": run_records["salted_two_phase"],
                "measured_run": 2,
                "measured": salted,
            },
            "salted_hot_only_two_phase": {
                "description": (
                    f"Only item {hot_item_id} receives a deterministic salt; "
                    "every other item gets _salt=0.  Stage 1 groups by "
                    "(item_id, _salt), stage 2 restores item_id."
                ),
                "salt_scope": "hot_only",
                "hot_item_id": hot_item_id,
                "runs": run_records["salted_hot_only_two_phase"],
                "measured_run": 2,
                "measured": hot_only,
            },
        },
        "comparison": build_comparison(
            unsalted, salted, treatment_prefix="salted"
        ),
        "comparison_hot_only": build_comparison(
            unsalted, hot_only, treatment_prefix="salted_hot_only"
        ),
        "group_cardinality": group_cardinality,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--salt-buckets", type=int, default=SALT_BUCKETS)
    parser.add_argument("--shuffle-partitions", type=int, default=SHUFFLE_PARTITIONS)
    args = parser.parse_args()

    if args.salt_buckets < 2:
        parser.error("--salt-buckets must be >= 2")
    if args.shuffle_partitions < 2:
        parser.error("--shuffle-partitions must be >= 2")

    ensure_dwd_exists()

    listener_jar, build_error = ensure_listener_jar()
    if listener_jar is not None:
        enable_listener_on_driver(listener_jar)
        print(f"[benchmark] SparkListener jar: {listener_jar}", file=sys.stderr)
    else:
        print(
            f"[benchmark] WARNING: SparkListener unavailable: {build_error}. "
            "Longest-task metrics will be null and collection_method will say "
            "fallback_total_time_only.",
            file=sys.stderr,
        )

    spark = build_spark_session(
        "SparkInsight-SkewBenchmark",
        shuffle_partitions=args.shuffle_partitions,
        enable_aqe=False,
    )
    results_path = BENCHMARK_RESULTS_PATH
    source = None
    try:
        print(
            "[benchmark] fixed config: "
            f"shuffle_partitions={args.shuffle_partitions}, aqe=false, "
            f"salt_buckets={args.salt_buckets}, runs_per_approach=2 "
            "(second run is measured)",
            file=sys.stderr,
        )

        source, input_stats = load_benchmark_source(spark)
        result = run_approach_set(
            spark,
            source,
            salt_buckets=args.salt_buckets,
            input_stats=input_stats,
        )

        report = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "experiment": (
                "no-salt vs full-salt vs hot-only-salt two-stage item "
                "aggregation"
            ),
            "environment": {
                "spark_version": spark.version,
                "java_version": str(
                    spark.sparkContext._jvm.System.getProperty("java.version")
                ),
                "python_version": platform.python_version(),
                "platform": platform.platform(),
                "master": spark.sparkContext.master,
                "shuffle_partitions": args.shuffle_partitions,
                "aqe_enabled": False,
                "salt_buckets": args.salt_buckets,
                "runs_per_approach": RUNS_PER_APPROACH,
                "measured_run_index": 2,
                "input_cache": "MEMORY_AND_DISK",
                "approaches": list(APPROACH_SCOPES),
                "listener_jar": str(listener_jar) if listener_jar else None,
                "listener_build_error": build_error,
            },
            "input": input_stats,
            **result,
            "notes": [
                "Each approach ran twice; only run 2 is used in comparison.",
                "AQE and adaptive skew-join handling are disabled so the raw "
                "shuffle skew is observable.",
                "Longest task is the maximum successful TaskInfo.duration() "
                "across all tasks in the measured action, collected by a JVM "
                "SparkListener.",
                "Shuffle bytes/records are summed from TaskMetrics across the "
                "measured action.",
                "Group cardinality is the measured number of distinct "
                "(item_id, _salt) keys; max_possible is the theoretical upper "
                "bound, not an observed value.",
                "Salt scope changes stage-1 key cardinality, shuffle records, "
                "and the number of shuffle stages; it is not a single isolated "
                "cost knob.",
            ],
        }

        results_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        measured = {
            name: data["measured"]
            for name, data in result["approaches"].items()
        }
        print("\n=== Skew benchmark (measured run 2) ===")
        print(
            f"{'approach':<32}{'wall_s':>10}{'longest_task_ms':>18}"
            f"{'shuffle_read_MB':>18}{'shuffle_write_MB':>19}"
        )
        for name, row in measured.items():
            print(
                f"{name:<32}{row['wall_clock_sec']:>10.3f}"
                f"{str(row['longest_task_ms']):>18}"
                f"{str(js_mb(row['shuffle_read_bytes'])):>18}"
                f"{str(js_mb(row['shuffle_write_bytes'])):>19}"
            )

        for label, comparison in (
            ("full salt", result["comparison"]),
            ("hot-only salt", result["comparison_hot_only"]),
        ):
            wall_change = comparison["wall_clock_change_pct"]
            task_change = comparison["longest_task_change_pct"]
            print(
                f"\n{label} vs unsalted: wall "
                f"{'n/a' if wall_change is None else f'{wall_change:+.2f}%'}, "
                "longest task "
                f"{'n/a' if task_change is None else f'{task_change:+.2f}%'}"
            )

        print("\nStage-1 logical group cardinality:")
        for name, cardinality in result["group_cardinality"].items():
            print(
                f"  {name}: actual={cardinality['actual']:,} "
                f"max_possible={cardinality['max_possible']:,}"
            )

        print(f"Results JSON: {results_path}")
        print(json.dumps(report, ensure_ascii=False, indent=2))
    finally:
        if source is not None:
            try:
                source.unpersist()
            except Exception:  # noqa: BLE001
                pass
        spark.stop()


if __name__ == "__main__":
    main()
