#!/usr/bin/env python3
"""Build user/item behavior features with StringIndexer and VectorAssembler.

The feature tables are deliberately separate from the ALS model.  ALS learns
latent factors from the user-item interaction matrix, while these tables
describe *why* a user or item looks interesting: behavior mix, sequence
transitions, recency, category diversity and conversion.  They are useful for
cold-start fallback, dashboard exploration and future content-based candidate
generation; feeding them into ALS directly would change the model contract.

Behavior-sequence features
--------------------------
For each user, events are ordered by ``event_time`` and the previous behavior is
attached with a window function.  The counts of transitions such as
``pv -> cart`` and ``pv -> buy`` capture intent flow that a plain aggregate
would lose.  Gaps between consecutive events give a simple session-intensity
signal.

Feature leakage
---------------
These tables are built from the complete DWD for exploration.  The ALS
evaluation job does *not* use them, so there is no train/test leakage in the
reported ranking metrics.  If a supervised cold-start model is added later, it
must be rebuilt on the training window only.

Usage:
    python -m evaluation.features
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
    FEATURE_RESULTS_PATH,
    ITEM_FEATURES_PATH,
    USER_FEATURES_PATH,
    build_spark_session,
    fs_path,
)
from spark_jobs.quality import assert_non_empty, assert_unique_key  # noqa: E402
from pyspark.ml.feature import StringIndexer, VectorAssembler  # noqa: E402
from pyspark.ml.functions import vector_to_array  # noqa: E402
from pyspark.sql import DataFrame, SparkSession, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

USER_NUMERIC_FEATURES = [
    "event_count",
    "pv_count",
    "cart_count",
    "fav_count",
    "buy_count",
    "distinct_items",
    "distinct_categories",
    "active_days",
    "buy_rate",
    "avg_gap_sec",
    "max_gap_sec",
    "recency_hours",
    "pv_to_cart",
    "pv_to_fav",
    "pv_to_buy",
    "cart_to_buy",
    "pv_to_buy_rate",
]

ITEM_NUMERIC_FEATURES = [
    "event_count",
    "pv_count",
    "cart_count",
    "fav_count",
    "buy_count",
    "unique_users",
    "buy_rate",
    "conversion_rate",
    "recency_hours",
    "category_index",
]


def _rate(numerator: str, denominator: str):
    """Safe ratio: zero when the denominator is zero, never NaN/Infinity."""

    return (
        F.when(F.col(denominator) > 0, F.col(numerator) / F.col(denominator))
        .otherwise(F.lit(0.0))
    )


def build_user_features(behaviors: DataFrame) -> DataFrame:
    """Aggregate per-user behavior mix plus ordered sequence transitions."""

    max_event_time = behaviors.agg(F.max("event_time")).first()[0]
    ordered = Window.partitionBy("user_id").orderBy("event_time")

    sequenced = (
        behaviors.withColumn("prev_behavior", F.lag("behavior_type").over(ordered))
        .withColumn("prev_event_time", F.lag("event_time").over(ordered))
        .withColumn(
            "gap_sec",
            F.unix_timestamp("event_time") - F.unix_timestamp("prev_event_time"),
        )
    )

    aggregated = sequenced.groupBy("user_id").agg(
        F.count(F.lit(1)).alias("event_count"),
        F.sum(F.when(F.col("behavior_type") == "pv", 1).otherwise(0)).alias(
            "pv_count"
        ),
        F.sum(F.when(F.col("behavior_type") == "cart", 1).otherwise(0)).alias(
            "cart_count"
        ),
        F.sum(F.when(F.col("behavior_type") == "fav", 1).otherwise(0)).alias(
            "fav_count"
        ),
        F.sum(F.when(F.col("behavior_type") == "buy", 1).otherwise(0)).alias(
            "buy_count"
        ),
        F.countDistinct("item_id").alias("distinct_items"),
        F.countDistinct("category_id").alias("distinct_categories"),
        F.countDistinct(F.date_format("event_time", "yyyy-MM-dd")).alias(
            "active_days"
        ),
        F.avg("gap_sec").alias("avg_gap_sec_raw"),
        F.max("gap_sec").alias("max_gap_sec_raw"),
        F.max("event_time").alias("last_event_time"),
        F.sum(
            F.when(
                (F.col("prev_behavior") == "pv")
                & (F.col("behavior_type") == "cart"),
                1,
            ).otherwise(0)
        ).alias("pv_to_cart"),
        F.sum(
            F.when(
                (F.col("prev_behavior") == "pv")
                & (F.col("behavior_type") == "fav"),
                1,
            ).otherwise(0)
        ).alias("pv_to_fav"),
        F.sum(
            F.when(
                (F.col("prev_behavior") == "pv")
                & (F.col("behavior_type") == "buy"),
                1,
            ).otherwise(0)
        ).alias("pv_to_buy"),
        F.sum(
            F.when(
                (F.col("prev_behavior") == "cart")
                & (F.col("behavior_type") == "buy"),
                1,
            ).otherwise(0)
        ).alias("cart_to_buy"),
    )

    enriched = (
        aggregated.withColumn(
            "avg_gap_sec", F.coalesce(F.col("avg_gap_sec_raw"), F.lit(0.0))
        )
        .withColumn(
            "max_gap_sec", F.coalesce(F.col("max_gap_sec_raw"), F.lit(0.0))
        )
        .withColumn("buy_rate", _rate("buy_count", "event_count"))
        .withColumn("pv_to_buy_rate", _rate("pv_to_buy", "event_count"))
        .withColumn(
            "recency_hours",
            (
                F.unix_timestamp(F.lit(max_event_time))
                - F.unix_timestamp("last_event_time")
            )
            / F.lit(3600.0),
        )
        .drop("avg_gap_sec_raw", "max_gap_sec_raw", "last_event_time")
    )

    typed = enriched
    for column in USER_NUMERIC_FEATURES:
        typed = typed.withColumn(column, F.col(column).cast("double"))

    assembler = VectorAssembler(
        inputCols=USER_NUMERIC_FEATURES,
        outputCol="features",
        handleInvalid="error",
    )
    return assembler.transform(typed)


def build_item_features(behaviors: DataFrame) -> DataFrame:
    """Aggregate per-item behavior and index the category dimension.

    ``category_id`` is categorical, so StringIndexer converts it to a
    contiguous index before VectorAssembler sees it.  Treating the raw integer
    as a numeric feature would falsely imply that category 99 is "larger" than
    category 1.
    """

    max_event_time = behaviors.agg(F.max("event_time")).first()[0]

    aggregated = behaviors.groupBy("item_id", "category_id").agg(
        F.count(F.lit(1)).alias("event_count"),
        F.sum(F.when(F.col("behavior_type") == "pv", 1).otherwise(0)).alias(
            "pv_count"
        ),
        F.sum(F.when(F.col("behavior_type") == "cart", 1).otherwise(0)).alias(
            "cart_count"
        ),
        F.sum(F.when(F.col("behavior_type") == "fav", 1).otherwise(0)).alias(
            "fav_count"
        ),
        F.sum(F.when(F.col("behavior_type") == "buy", 1).otherwise(0)).alias(
            "buy_count"
        ),
        F.countDistinct("user_id").alias("unique_users"),
        F.max("event_time").alias("last_event_time"),
    )

    enriched = (
        aggregated.withColumn("buy_rate", _rate("buy_count", "event_count"))
        .withColumn("conversion_rate", _rate("buy_count", "pv_count"))
        .withColumn(
            "recency_hours",
            (
                F.unix_timestamp(F.lit(max_event_time))
                - F.unix_timestamp("last_event_time")
            )
            / F.lit(3600.0),
        )
        .drop("last_event_time")
    )

    for column in ITEM_NUMERIC_FEATURES:
        if column == "category_index":
            continue
        enriched = enriched.withColumn(column, F.col(column).cast("double"))

    indexer = StringIndexer(
        inputCol="category_id",
        outputCol="category_index",
        handleInvalid="keep",
    ).fit(enriched)
    indexed = indexer.transform(enriched).withColumn(
        "category_index", F.col("category_index").cast("double")
    )

    assembler = VectorAssembler(
        inputCols=ITEM_NUMERIC_FEATURES,
        outputCol="features",
        handleInvalid="error",
    )
    return assembler.transform(indexed)


def run(spark: SparkSession) -> dict:
    if not DWD_PATH.exists():
        raise FileNotFoundError(
            f"DWD not found at {DWD_PATH}. Run: python spark_jobs/run_pipeline.py"
        )

    behaviors = (
        spark.read.parquet(fs_path(DWD_PATH))
        .select("user_id", "item_id", "category_id", "behavior_type", "event_time")
        .cache()
    )
    input_rows = behaviors.count()
    input_users = behaviors.select("user_id").distinct().count()
    input_items = behaviors.select("item_id").distinct().count()

    user_features = build_user_features(behaviors).cache()
    item_features = build_item_features(behaviors).cache()
    user_rows = user_features.count()
    item_rows = item_features.count()

    checks = [
        assert_non_empty(
            user_features, "features.user_features", actual_rows=user_rows
        ),
        assert_unique_key(
            user_features, ["user_id"], "features.user_features", actual_rows=user_rows
        ),
        assert_non_empty(
            item_features, "features.item_features", actual_rows=item_rows
        ),
        assert_unique_key(
            item_features, ["item_id"], "features.item_features", actual_rows=item_rows
        ),
    ]

    user_feature_size = len(USER_NUMERIC_FEATURES)
    item_feature_size = len(ITEM_NUMERIC_FEATURES)
    if user_rows != input_users:
        raise AssertionError(
            f"user feature rows {user_rows} != distinct users {input_users}"
        )
    if item_rows != input_items:
        raise AssertionError(
            f"item feature rows {item_rows} != distinct items {input_items}"
        )

    # Vector size is part of the published contract; verify it rather than
    # trusting the assembler input list by eye.
    user_vector_size = user_features.select(
        F.size(vector_to_array("features")).alias("n")
    ).distinct().collect()
    item_vector_size = item_features.select(
        F.size(vector_to_array("features")).alias("n")
    ).distinct().collect()
    if [row["n"] for row in user_vector_size] != [user_feature_size]:
        raise AssertionError(f"unexpected user vector sizes: {user_vector_size}")
    if [row["n"] for row in item_vector_size] != [item_feature_size]:
        raise AssertionError(f"unexpected item vector sizes: {item_vector_size}")

    # Full snapshot overwrite: these tables are derived only from DWD, so a
    # rerun must not leave stale users/items from an earlier source file.
    user_features.write.mode("overwrite").parquet(fs_path(USER_FEATURES_PATH))
    item_features.write.mode("overwrite").parquet(fs_path(ITEM_FEATURES_PATH))

    summary = {
        "layer": "FEATURES",
        "input": {
            "path": fs_path(DWD_PATH),
            "rows": input_rows,
            "distinct_users": input_users,
            "distinct_items": input_items,
        },
        "outputs": {
            "user_features": {
                "path": fs_path(USER_FEATURES_PATH),
                "rows": user_rows,
                "feature_columns": USER_NUMERIC_FEATURES,
                "vector_size": user_feature_size,
            },
            "item_features": {
                "path": fs_path(ITEM_FEATURES_PATH),
                "rows": item_rows,
                "feature_columns": ITEM_NUMERIC_FEATURES,
                "vector_size": item_feature_size,
                "categorical_indexer": "StringIndexer(category_id -> category_index)",
                "vector_assembler": "VectorAssembler(item numeric columns -> features)",
            },
        },
        "assertions": checks,
        "status": "OK",
    }
    FEATURE_RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    FEATURE_RESULTS_PATH.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)

    user_features.unpersist()
    item_features.unpersist()
    behaviors.unpersist()
    return summary


def main() -> None:
    spark = build_spark_session("SparkInsight-Features", shuffle_partitions=16)
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
