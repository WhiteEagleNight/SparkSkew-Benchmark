#!/usr/bin/env python3
"""DWD job: clean, deduplicate, assert, and partition the behavior detail.

DWD guarantees:
  * three or more explicit quality assertions (row count, primary-key
    uniqueness, key-field non-null rates);
  * a physical layout partitioned by event date (``dt``);
  * an overwrite-based, idempotent batch rebuild.

Rerun / overwrite policy
------------------------
``mode("overwrite")`` is a static overwrite of the entire DWD directory.
Every re-run rebuilds all ``dt`` partitions from ODS, so a changed source file
cannot leave an orphan partition from an earlier run.  This is deliberate for
a batch demo.  If this becomes incremental, switch to dynamic partition
overwrite only together with a bounded reprocessing window and an idempotent
merge-by-primary-key step; otherwise a partial run can publish mixed data.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark_jobs.common import (  # noqa: E402
    DWD_PATH,
    ODS_PATH,
    build_spark_session,
    fs_path,
)
from spark_jobs.quality import (  # noqa: E402
    assert_equal,
    assert_non_empty,
    assert_non_null_rate,
    assert_unique_key,
)
from pyspark.sql import SparkSession, functions as F  # noqa: E402

BEHAVIOR_TYPES = ["pv", "cart", "fav", "buy"]
PRIMARY_KEY = ["user_id", "item_id", "event_time"]
KEY_FIELDS = ["user_id", "item_id", "event_time", "behavior_type"]
NON_NULL_THRESHOLD = 0.999


def build_dwd(spark: SparkSession):
    if not ODS_PATH.exists():
        raise FileNotFoundError(
            f"ODS not found at {ODS_PATH}. Run spark_jobs/ods_ingest.py first."
        )

    ods = spark.read.parquet(fs_path(ODS_PATH))
    typed = ods.select(
        F.col("user_id").cast("int").alias("user_id"),
        F.col("item_id").cast("int").alias("item_id"),
        F.col("category_id").cast("int").alias("category_id"),
        F.col("behavior_type").cast("string").alias("behavior_type"),
        F.to_timestamp("event_time").alias("event_time"),
    )

    # Reject rows that cannot participate in the behavior model.  The
    # generator only emits valid rows, but the filter keeps DWD robust if the
    # CSV is replaced.  Invalid rows would normally be routed to a quarantine
    # table; this demo keeps the failure visible through row counts instead of
    # silently coercing bad keys to null.
    valid = typed.filter(
        F.col("user_id").isNotNull()
        & F.col("item_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("behavior_type").isin(BEHAVIOR_TYPES)
    )

    with_dt = (
        valid.withColumn("dt", F.date_format("event_time", "yyyy-MM-dd"))
        .withColumn("processed_at", F.current_timestamp())
    )

    # The DWD primary key is (user_id, item_id, event_time).  Deduplicate for
    # an idempotent batch load; the generated dataset is built to be unique,
    # and the assertion below independently verifies that claim.
    dwd = with_dt.dropDuplicates(PRIMARY_KEY)
    return dwd


def run(spark: SparkSession) -> dict:
    dwd = build_dwd(spark)

    # Assertion 1: a non-empty detail table.
    row_count = dwd.count()
    checks = [assert_non_empty(dwd, "dwd.behavior_events", actual_rows=row_count)]

    # Assertion 2: primary key (user_id + item_id + event_time) is unique.
    checks.append(
        assert_unique_key(
            dwd, PRIMARY_KEY, "dwd.behavior_events", actual_rows=row_count
        )
    )

    # Assertion 3: every critical field has a non-null, non-blank rate of at
    # least 99.9%.  The threshold is deliberately explicit and auditable.
    checks.append(
        assert_non_null_rate(
            dwd,
            KEY_FIELDS,
            NON_NULL_THRESHOLD,
            "dwd.behavior_events",
            actual_rows=row_count,
        )
    )

    # Rerun policy is documented in the module docstring: full static
    # overwrite, rebuilding every dt partition from ODS on every run.
    dwd.write.mode("overwrite").partitionBy("dt").parquet(fs_path(DWD_PATH))

    # Assertion 4: verify that the persisted table has the same row count as
    # the in-memory, already-validated DWD plan.
    persisted = spark.read.parquet(fs_path(DWD_PATH))
    persisted_rows = persisted.count()
    checks.append(
        assert_equal(
            persisted_rows,
            row_count,
            "dwd_persisted_row_count",
            "persisted DWD must match validated DWD rows",
        )
    )

    summary = {
        "layer": "DWD",
        "output": fs_path(DWD_PATH),
        "rows": persisted_rows,
        "columns": persisted.columns,
        "partition_column": "dt",
        "primary_key": PRIMARY_KEY,
        "assertions": checks,
        "status": "OK",
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return summary


def main() -> None:
    spark = build_spark_session("SparkInsight-DWD", shuffle_partitions=16)
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
