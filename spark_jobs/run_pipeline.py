#!/usr/bin/env python3
"""Run the complete ODS -> DWD -> DWS batch pipeline on one Spark session.

Usage:
    python spark_jobs/run_pipeline.py [--salted]

DWS defaults to the unsalted path.  ``--salted`` exercises the salted
reference path for demos/regression; for this count/sum workload the benchmark
shows that path is slower.

The DWD/DWS assertion failures propagate and make the process exit non-zero.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark_jobs.common import build_spark_session  # noqa: E402
from spark_jobs.dwd_build import run as run_dwd  # noqa: E402
from spark_jobs.dws_build import SHUFFLE_PARTITIONS, run as run_dws  # noqa: E402
from spark_jobs.ods_ingest import run as run_ods  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--salted",
        action="store_true",
        help=(
            "Use the salted reference path for DWS. It is slower for this "
            "count/sum workload and is intended for demos/regression only."
        ),
    )
    args = parser.parse_args()
    spark = build_spark_session(
        "SparkInsight-Pipeline", shuffle_partitions=SHUFFLE_PARTITIONS
    )
    try:
        ods = run_ods(spark)
        dwd = run_dwd(spark)
        dws = run_dws(spark, salted=args.salted)
        print(
            json.dumps(
                {
                    "status": "PIPELINE_OK",
                    "ods_rows": ods["rows"],
                    "dwd_rows": dwd["rows"],
                    "dws_rows": dws["rows"],
                    "dws_event_total": dws["event_total"],
                    "dws_aggregation": dws["aggregation"],
                },
                ensure_ascii=False,
                indent=2,
            ),
            flush=True,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
