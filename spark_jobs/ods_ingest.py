#!/usr/bin/env python3
"""ODS job: land the immutable raw CSV as Parquet.

ODS deliberately keeps every source column as a string and adds ingestion
metadata.  Type parsing and business validation belong to DWD, so a malformed
source row can be rejected there without losing the original evidence here.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark_jobs.common import (  # noqa: E402
    ODS_PATH,
    RAW_DATA_PATH,
    build_spark_session,
    fs_path,
)
from pyspark.sql import SparkSession, functions as F  # noqa: E402


def run(spark: SparkSession) -> dict:
    if not RAW_DATA_PATH.exists():
        raise FileNotFoundError(
            f"Raw data missing: {RAW_DATA_PATH}. "
            "Run python examples/generate_mock_data.py first."
        )

    # Preserve raw text in ODS.  CSV's header is removed once; all values stay
    # strings so DWD owns schema enforcement and quarantine decisions.
    raw = spark.read.option("header", True).option("inferSchema", False).csv(
        fs_path(RAW_DATA_PATH)
    )
    ods = (
        raw.withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.lit(RAW_DATA_PATH.name))
    )

    # Rerun policy: static overwrite replaces the complete ODS directory.  It
    # cannot leave a stale part file from a previous, larger run behind.
    # For incremental production ingestion, use dynamic partition overwrite
    # keyed by ingest_date plus idempotent source-file deduplication.
    ods.write.mode("overwrite").parquet(fs_path(ODS_PATH))

    # Read back the persisted layer instead of trusting the writer.  This
    # catches path/permission problems before DWD consumes a missing table.
    persisted = spark.read.parquet(fs_path(ODS_PATH))
    rows = persisted.count()
    if rows <= 0:
        raise AssertionError(f"ODS at {ODS_PATH} is empty after write")
    columns = persisted.columns
    summary = {
        "layer": "ODS",
        "output": fs_path(ODS_PATH),
        "rows": rows,
        "columns": columns,
        "status": "OK",
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return summary


def main() -> None:
    spark = build_spark_session("SparkInsight-ODS", shuffle_partitions=16)
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
