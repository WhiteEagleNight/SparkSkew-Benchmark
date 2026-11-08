#!/usr/bin/env python3
"""Measure salt-bucket sensitivity for the three aggregation scopes.

This script reuses the exact measurement code in ``benchmark_skew.py``: the
same cached DWD input, the same JVM SparkListener, the same two-run protocol,
and the same output-row assertions.  It only changes ``salt_buckets`` between
measurement blocks.

The default sweep is ``2, 5, 20``.  The 10-bucket point is already produced by
``benchmark_skew.py``; keeping it out of the default sweep avoids two different
runs both claiming to be "the 10-bucket result".  Pass ``--salt-buckets 2 5 10
20`` if you want the sweep to include 10 as well.

Usage:
    python pipelines/benchmark_salt_sweep.py
    python pipelines/benchmark_salt_sweep.py --salt-buckets 2 5 10 20
"""

from __future__ import annotations

import argparse
import json
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark_jobs.common import (  # noqa: E402
    BENCHMARK_SWEEP_RESULTS_PATH,
    build_spark_session,
    ensure_dwd_exists,
)
from pipelines.benchmark_skew import (  # noqa: E402
    APPROACH_SCOPES,
    RUNS_PER_APPROACH,
    SHUFFLE_PARTITIONS,
    enable_listener_on_driver,
    ensure_listener_jar,
    load_benchmark_source,
    run_approach_set,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--salt-buckets",
        type=int,
        nargs="+",
        default=[2, 5, 20],
        help="Salt bucket counts to measure (default: 2 5 20).",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=SHUFFLE_PARTITIONS,
    )
    args = parser.parse_args()

    buckets = list(dict.fromkeys(args.salt_buckets))
    if any(bucket < 2 for bucket in buckets):
        parser.error("every --salt-buckets value must be >= 2")
    if args.shuffle_partitions < 2:
        parser.error("--shuffle-partitions must be >= 2")

    ensure_dwd_exists()

    listener_jar, build_error = ensure_listener_jar()
    if listener_jar is not None:
        enable_listener_on_driver(listener_jar)
        print(f"[sweep] SparkListener jar: {listener_jar}", file=sys.stderr)
    else:
        print(
            f"[sweep] WARNING: SparkListener unavailable: {build_error}. "
            "Longest-task metrics will be null and collection_method will say "
            "fallback_total_time_only.",
            file=sys.stderr,
        )

    spark = build_spark_session(
        "SparkInsight-SaltSweep",
        shuffle_partitions=args.shuffle_partitions,
        enable_aqe=False,
    )
    source = None
    try:
        print(
            "[sweep] fixed config: "
            f"shuffle_partitions={args.shuffle_partitions}, aqe=false, "
            f"salt_buckets={buckets}, runs_per_approach={RUNS_PER_APPROACH} "
            "(second run is measured)",
            file=sys.stderr,
        )
        source, input_stats = load_benchmark_source(spark)

        sweep = {}
        for bucket_count in buckets:
            print(
                f"\n[sweep] === salt_buckets={bucket_count} ===",
                file=sys.stderr,
                flush=True,
            )
            sweep[str(bucket_count)] = run_approach_set(
                spark,
                source,
                salt_buckets=bucket_count,
                input_stats=input_stats,
            )

        report = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "experiment": (
                "salt-bucket sensitivity for no-salt, full-salt, and "
                "hot-only-salt two-stage item aggregation"
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
                "salt_buckets": buckets,
                "runs_per_approach": RUNS_PER_APPROACH,
                "measured_run_index": 2,
                "input_cache": "MEMORY_AND_DISK",
                "approaches": list(APPROACH_SCOPES),
                "listener_jar": str(listener_jar) if listener_jar else None,
                "listener_build_error": build_error,
            },
            "input": input_stats,
            "sweep": sweep,
            "notes": [
                "Each approach ran twice per bucket count; only run 2 is used.",
                "The DWD input was cached once and shared across every bucket.",
                "Group cardinality is measured as distinct (item_id, _salt) "
                "keys, not inferred from distinct_items * salt_buckets.",
                "The default 10-bucket point lives in benchmark_results.json; "
                "this file is the sensitivity check around it.",
            ],
        }

        BENCHMARK_SWEEP_RESULTS_PATH.write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        def fmt_pct(value: float | None) -> str:
            return "n/a" if value is None else f"{value:+.2f}%"

        print("\n=== Salt-bucket sweep (measured run 2) ===")
        print(
            f"{'buckets':>8}{'unsalted_s':>14}{'all_s':>12}"
            f"{'hot_only_s':>14}{'all_delta':>12}{'hot_delta':>12}"
        )
        for bucket_count in buckets:
            block = sweep[str(bucket_count)]
            measured = {
                name: data["measured"]
                for name, data in block["approaches"].items()
            }
            u = measured["unsalted_two_phase"]["wall_clock_sec"]
            a = measured["salted_two_phase"]["wall_clock_sec"]
            h = measured["salted_hot_only_two_phase"]["wall_clock_sec"]
            print(
                f"{bucket_count:>8}{u:>14.3f}{a:>12.3f}{h:>14.3f}"
                f"{fmt_pct(block['comparison']['wall_clock_change_pct']):>12}"
                f"{fmt_pct(block['comparison_hot_only']['wall_clock_change_pct']):>12}"
            )

        print(f"\nSweep JSON: {BENCHMARK_SWEEP_RESULTS_PATH}")
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
