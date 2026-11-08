#!/usr/bin/env python3
"""Time-split implicit ALS evaluation with ranking metrics.

Protocol
--------
1. Split DWD at the 80th percentile of ``event_time``.  Everything at or
   before the quantile is training data; everything after it is test data.
2. Build implicit user-item confidence from weighted behavior counts
   (pv=1, fav=2, cart=3, buy=5) in the training window.
3. Fit Spark MLlib ALS with ``implicitPrefs=True``.
4. Recommend only to test users that also appear in training.  Users that
   appear only after the split are genuine cold-start users and are counted,
   not silently dropped.
5. Remove training-seen items from the candidate list, then compare the top-K
   recommendations with post-split items that the user had not seen before.
6. Report NDCG@K, MAP@K, Precision@K and Recall@K, plus a popularity fallback
   for cold-start users.

Why no RMSE
-----------
Implicit feedback has no explicit ratings and no trustworthy negative samples:
the absence of a click is not a zero-star rating.  RMSE is therefore not a
valid metric for this experiment and is intentionally not reported.  A separate
explicit-feedback control would be required before RMSE could be cited.

Usage:
    python -m evaluation.evaluate_als
    python -m evaluation.evaluate_als --rank 16 --k-values 10,20
"""

from __future__ import annotations

import argparse
import json
import platform
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark_jobs.common import (  # noqa: E402
    ALS_MODEL_PATH,
    DWD_PATH,
    EVALUATION_RESULTS_PATH,
    build_spark_session,
    fs_path,
)
from pyspark.ml.recommendation import ALS  # noqa: E402
from pyspark.mllib.evaluation import RankingMetrics  # noqa: E402
from pyspark.sql import DataFrame, SparkSession, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

BEHAVIOR_WEIGHTS = {"pv": 1.0, "fav": 2.0, "cart": 3.0, "buy": 5.0}
DEFAULT_K_VALUES = (10, 20)
MAX_K = 20
CANDIDATE_BUFFER = 50
SPLIT_QUANTILE = 0.80
DEFAULT_RANK = 32
DEFAULT_MAX_ITER = 10
DEFAULT_REG_PARAM = 0.08
DEFAULT_ALPHA = 40.0
DEFAULT_SEED = 42


def behavior_weight_column():
    expression = None
    for behavior, weight in BEHAVIOR_WEIGHTS.items():
        condition = F.col("behavior_type") == behavior
        expression = (
            F.when(condition, F.lit(weight))
            if expression is None
            else expression.when(condition, F.lit(weight))
        )
    return expression.otherwise(F.lit(0.0))


def time_split(behaviors: DataFrame, quantile: float):
    """Split on the timestamp quantile, not on a random row sample.

    A random split leaks future behavior into training.  The quantile split
    simulates the real deployment question: given history up to time T,
    recommend what happens after T.
    """

    row = behaviors.agg(
        F.percentile_approx(
            F.col("event_time").cast("long"), quantile, 10000
        ).alias("split_epoch")
    ).first()
    if row is None or row["split_epoch"] is None:
        raise ValueError("cannot compute a timestamp split from empty input")

    split_epoch = int(row["split_epoch"])
    boundary = F.timestamp_seconds(F.lit(split_epoch).cast("long"))
    train = behaviors.filter(F.col("event_time") <= boundary)
    test = behaviors.filter(F.col("event_time") > boundary)
    return train, test, split_epoch


def ranking_metrics(joined: DataFrame, k_values: tuple[int, ...]) -> dict:
    """Compute RankingMetrics plus Recall@K for one predictions/actual table."""

    base = joined.select("pred_items", "actual_items").rdd.map(
        lambda row: (list(row["pred_items"]), list(row["actual_items"]))
    )
    base.cache()
    users_evaluated = base.count()
    if users_evaluated == 0:
        base.unpersist()
        raise AssertionError("no evaluable users: predictions and actuals did not join")

    results: dict[str, float | int] = {"users_evaluated": users_evaluated}
    for k in k_values:
        truncated = base.map(lambda pair, k=k: (pair[0][:k], pair[1]))
        metrics = RankingMetrics(truncated)
        results[f"ndcg@{k}"] = round(float(metrics.ndcgAt(k)), 6)
        results[f"map@{k}"] = round(float(metrics.meanAveragePrecision), 6)
        results[f"precision@{k}"] = round(float(metrics.precisionAt(k)), 6)
        recall = truncated.map(
            lambda pair: (
                len(set(pair[0]) & set(pair[1])) / len(pair[1])
                if pair[1]
                else 0.0
            )
        ).mean()
        results[f"recall@{k}"] = round(float(recall), 6)
    base.unpersist()
    return results


def build_predictions(
    recs: DataFrame, seen: DataFrame, max_k: int
) -> DataFrame:
    """Explode recommendations, remove seen items, keep top ``max_k``."""

    exploded = (
        recs.select(
            "user_id",
            F.posexplode("recommendations").alias("position", "recommendation"),
        )
        .select(
            "user_id",
            "position",
            F.col("recommendation.item_id").alias("item_id"),
        )
    )
    unseen = exploded.join(seen, ["user_id", "item_id"], "left_anti")
    ranked = unseen.withColumn(
        "rank", F.row_number().over(
            Window.partitionBy("user_id").orderBy("position")
        )
    ).filter(F.col("rank") <= max_k)

    grouped = ranked.groupBy("user_id").agg(
        F.collect_list(F.struct("rank", "item_id")).alias("ranked_items")
    )
    return grouped.select(
        "user_id",
        F.expr("transform(sort_array(ranked_items), x -> x.item_id)").alias(
            "pred_items"
        ),
    )


def cold_start_popularity_baseline(
    *,
    test: DataFrame,
    cold_users: DataFrame,
    train_items: DataFrame,
    train: DataFrame,
    k_values: tuple[int, ...],
    max_k: int,
) -> dict:
    """Evaluate a non-personalized popularity fallback for cold-start users.

    This is not ALS.  It exists to show how the cold-start cohort is handled
    operationally and to give the ranking numbers a reference point.
    """

    cold_user_count = cold_users.count()
    if cold_user_count == 0:
        return {
            "method": "global popularity from training window",
            "users_evaluated": 0,
            "metrics": None,
            "note": "no cold-start users in the test window",
        }

    cold_test_pairs = (
        test.select("user_id", "item_id")
        .join(cold_users, "user_id", "inner")
        .distinct()
    )
    known_pairs = cold_test_pairs.join(train_items, "item_id", "inner")
    cold_actual = known_pairs.groupBy("user_id").agg(
        F.collect_set("item_id").alias("actual_items")
    )

    top_items = [
        int(row["item_id"])
        for row in (
            train.groupBy("item_id")
            .count()
            .orderBy(F.desc("count"), F.asc("item_id"))
            .limit(max_k)
            .select("item_id")
            .collect()
        )
    ]
    cold_pred = cold_actual.select("user_id").withColumn(
        "pred_items", F.array([F.lit(item) for item in top_items])
    )
    joined = cold_pred.join(cold_actual, "user_id", "inner").cache()
    users_evaluated = joined.count()
    if users_evaluated == 0:
        joined.unpersist()
        return {
            "method": "global popularity from training window",
            "users_evaluated": 0,
            "metrics": None,
            "note": "cold-start users had no test items present in the training catalog",
        }

    actual_stats = joined.select(
        F.avg(F.size("actual_items")).alias("avg_actual_items"),
        F.max(F.size("actual_items")).alias("max_actual_items"),
    ).first()
    metrics = ranking_metrics(joined, k_values)
    joined.unpersist()
    return {
        "method": "global popularity from training window",
        "top_items": top_items,
        "users_evaluated": users_evaluated,
        "avg_actual_items": round(float(actual_stats["avg_actual_items"]), 4),
        "max_actual_items": int(actual_stats["max_actual_items"]),
        "metrics": metrics,
        "note": (
            "Non-personalized fallback; evaluated separately from ALS because "
            "these users have no training history."
        ),
    }


def emit(result: dict) -> None:
    print("\n=== Time-split implicit ALS evaluation ===")
    split = result["split"]
    print(
        f"split quantile={split['quantile']}: "
        f"train_rows={split['train_rows']:,} "
        f"test_rows={split['test_rows']:,} "
        f"(actual test share={split['test_ratio']:.2%})"
    )
    users = result["users"]
    print(
        f"train_users={users['train_users']:,} "
        f"test_users={users['test_users']:,} "
        f"cold_start_users={users['cold_start_users']:,} "
        f"({users['cold_start_user_ratio']:.2%} of test users)"
    )
    training = result["training"]
    config = result["config"]
    print(
        f"ALS rank={config['rank']} maxIter={config['max_iter']} "
        f"alpha={config['alpha']} implicitPrefs=true "
        f"fit_seconds={training['fit_seconds']}"
    )
    evaluation = result["evaluation"]
    recommendation = result["recommendation"]
    print(
        f"evaluated_users={evaluation['users_evaluated']:,} "
        f"avg_actual_items={evaluation['avg_actual_items']} "
        f"avg_predictions={recommendation['avg_predictions']}"
    )
    for k in result["config"]["k_values"]:
        print(
            f"K={k:>2}: NDCG={evaluation['metrics'][f'ndcg@{k}']:.6f} "
            f"MAP={evaluation['metrics'][f'map@{k}']:.6f} "
            f"Precision={evaluation['metrics'][f'precision@{k}']:.6f} "
            f"Recall={evaluation['metrics'][f'recall@{k}']:.6f}"
        )
    baseline = result.get("cold_start_baseline") or {}
    if baseline.get("metrics"):
        primary_k = config["k_values"][0]
        print(
            f"cold-start popularity baseline: "
            f"users={baseline['users_evaluated']:,} "
            f"NDCG@{primary_k}={baseline['metrics'][f'ndcg@{primary_k}']:.6f} "
            f"MAP@{primary_k}={baseline['metrics'][f'map@{primary_k}']:.6f} "
            f"Precision@{primary_k}="
            f"{baseline['metrics'][f'precision@{primary_k}']:.6f}"
        )
    print(f"Results JSON: {EVALUATION_RESULTS_PATH}")
    print(json.dumps(result, ensure_ascii=False, indent=2))


def run(
    spark: SparkSession,
    *,
    rank: int = DEFAULT_RANK,
    max_iter: int = DEFAULT_MAX_ITER,
    reg_param: float = DEFAULT_REG_PARAM,
    alpha: float = DEFAULT_ALPHA,
    split_quantile: float = SPLIT_QUANTILE,
    k_values: tuple[int, ...] = DEFAULT_K_VALUES,
    seed: int = DEFAULT_SEED,
) -> dict:
    if not DWD_PATH.exists():
        raise FileNotFoundError(
            f"DWD not found at {DWD_PATH}. Run: python spark_jobs/run_pipeline.py"
        )
    if not 0.0 < split_quantile < 1.0:
        raise ValueError("split_quantile must be strictly between 0 and 1")
    if not k_values or min(k_values) <= 0:
        raise ValueError("k_values must contain positive integers")
    if max(k_values) > MAX_K:
        raise ValueError(f"k_values cannot exceed {MAX_K} in this script")

    behaviors = (
        spark.read.parquet(fs_path(DWD_PATH))
        .select("user_id", "item_id", "category_id", "behavior_type", "event_time")
        .cache()
    )
    total_rows = behaviors.count()
    if total_rows == 0:
        raise AssertionError("DWD is empty")

    train, test, split_epoch = time_split(behaviors, split_quantile)
    train_rows = train.count()
    test_rows = test.count()
    if train_rows == 0 or test_rows == 0:
        raise AssertionError(
            f"empty split: train_rows={train_rows}, test_rows={test_rows}"
        )

    split_display = (
        spark.createDataFrame([(split_epoch,)], ["epoch"])
        .select(F.timestamp_seconds("epoch").alias("ts"))
        .first()["ts"]
        .isoformat()
    )

    train_users = train.select("user_id").distinct().cache()
    test_users = test.select("user_id").distinct().cache()
    overlap_users = train_users.join(test_users, "user_id", "inner").cache()
    cold_users = test_users.join(train_users, "user_id", "left_anti").cache()

    train_user_count = train_users.count()
    test_user_count = test_users.count()
    overlap_user_count = overlap_users.count()
    cold_user_count = cold_users.count()
    cold_test_rows = test.join(cold_users, "user_id", "inner").count()

    train_ratings = (
        train.withColumn("weight", behavior_weight_column())
        .groupBy("user_id", "item_id")
        .agg(F.sum("weight").cast("double").alias("rating"))
        .cache()
    )
    interaction_count = train_ratings.count()
    train_distinct_users = train_ratings.select("user_id").distinct().count()
    train_distinct_items = train_ratings.select("item_id").distinct().count()
    rating_stats = train_ratings.agg(
        F.min("rating").alias("min_rating"),
        F.max("rating").alias("max_rating"),
        F.avg("rating").alias("avg_rating"),
    ).first()

    als = (
        ALS(
            userCol="user_id",
            itemCol="item_id",
            ratingCol="rating",
            implicitPrefs=True,
            alpha=alpha,
            rank=rank,
            maxIter=max_iter,
            regParam=reg_param,
            coldStartStrategy="drop",
            nonnegative=False,
            seed=seed,
        )
        .setCheckpointInterval(-1)
    )
    fit_started = time.perf_counter()
    model = als.fit(train_ratings)
    fit_seconds = round(time.perf_counter() - fit_started, 3)

    model_save_error = None
    try:
        model.write().overwrite().save(fs_path(ALS_MODEL_PATH))
    except Exception as exc:  # noqa: BLE001 - artifact save must not hide metrics
        model_save_error = f"{type(exc).__name__}: {exc}"

    candidate_items = max(k_values) + CANDIDATE_BUFFER
    recs = model.recommendForUserSubset(overlap_users, candidate_items).cache()
    users_with_recommendations = recs.count()

    seen = train_ratings.select("user_id", "item_id").distinct()
    predictions = build_predictions(recs, seen, max(k_values)).cache()
    prediction_stats = predictions.select(
        F.min(F.size("pred_items")).alias("min_predictions"),
        F.avg(F.size("pred_items")).alias("avg_predictions"),
        F.max(F.size("pred_items")).alias("max_predictions"),
        F.count(F.lit(1)).alias("users"),
    ).first()

    train_items = train_ratings.select("item_id").distinct().cache()
    test_pairs = test.select("user_id", "item_id").distinct().cache()
    known_test_pairs = test_pairs.join(train_items, "item_id", "inner")
    unknown_test_pairs = test_pairs.join(train_items, "item_id", "left_anti").count()
    actual = (
        known_test_pairs.join(seen, ["user_id", "item_id"], "left_anti")
        .groupBy("user_id")
        .agg(F.collect_set("item_id").alias("actual_items"))
        .cache()
    )
    joined = predictions.join(actual, "user_id", "inner").cache()
    users_evaluated = joined.count()
    if users_evaluated == 0:
        raise AssertionError(
            "no evaluable users after filtering cold-start users and seen items"
        )

    actual_stats = joined.select(
        F.avg(F.size("actual_items")).alias("avg_actual_items"),
        F.max(F.size("actual_items")).alias("max_actual_items"),
        F.sum(F.size("actual_items")).alias("relevant_pairs"),
    ).first()
    covered_items = (
        joined.select(F.explode("pred_items").alias("item_id"))
        .distinct()
        .count()
    )
    metrics = ranking_metrics(joined, k_values)

    cold_start = cold_start_popularity_baseline(
        test=test,
        cold_users=cold_users,
        train_items=train_items,
        train=train,
        k_values=k_values,
        max_k=max(k_values),
    )

    result = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "experiment": "time-split implicit ALS with RankingMetrics",
        "environment": {
            "spark_version": spark.version,
            "java_version": str(
                spark.sparkContext._jvm.System.getProperty("java.version")
            ),
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "master": spark.sparkContext.master,
            "shuffle_partitions": int(
                spark.conf.get("spark.sql.shuffle.partitions")
            ),
        },
        "config": {
            "implicit_prefs": True,
            "rank": rank,
            "max_iter": max_iter,
            "reg_param": reg_param,
            "alpha": alpha,
            "seed": seed,
            "split_quantile": split_quantile,
            "k_values": list(k_values),
            "candidate_items_before_seen_filter": candidate_items,
            "behavior_weights": BEHAVIOR_WEIGHTS,
        },
        "split": {
            "quantile": split_quantile,
            "split_epoch_seconds": split_epoch,
            "split_timestamp": split_display,
            "total_rows": total_rows,
            "train_rows": train_rows,
            "test_rows": test_rows,
            "train_ratio": round(train_rows / total_rows, 6),
            "test_ratio": round(test_rows / total_rows, 6),
        },
        "users": {
            "train_users": train_user_count,
            "test_users": test_user_count,
            "overlap_users": overlap_user_count,
            "train_only_users": train_user_count - overlap_user_count,
            "cold_start_users": cold_user_count,
            "cold_start_user_ratio": round(
                cold_user_count / test_user_count, 6
            ),
            "cold_start_test_rows": cold_test_rows,
            "cold_start_test_row_ratio": round(
                cold_test_rows / test_rows, 6
            ),
        },
        "training": {
            "user_item_interactions": interaction_count,
            "distinct_users": train_distinct_users,
            "distinct_items": train_distinct_items,
            "density": round(
                interaction_count
                / (train_distinct_users * train_distinct_items),
                8,
            ),
            "min_rating": round(float(rating_stats["min_rating"]), 4),
            "max_rating": round(float(rating_stats["max_rating"]), 4),
            "avg_rating": round(float(rating_stats["avg_rating"]), 4),
            "fit_seconds": fit_seconds,
            "model_path": fs_path(ALS_MODEL_PATH),
            "model_save_error": model_save_error,
        },
        "recommendation": {
            "users_with_recommendations": users_with_recommendations,
            "avg_predictions": round(float(prediction_stats["avg_predictions"]), 4),
            "min_predictions": int(prediction_stats["min_predictions"]),
            "max_predictions": int(prediction_stats["max_predictions"]),
            "catalog_items": train_distinct_items,
            "distinct_recommended_items": covered_items,
            "catalog_coverage_at_max_k": round(
                covered_items / train_distinct_items, 6
            ),
        },
        "evaluation": {
            "users_evaluated": users_evaluated,
            "avg_actual_items": round(
                float(actual_stats["avg_actual_items"]), 4
            ),
            "max_actual_items": int(actual_stats["max_actual_items"]),
            "relevant_pairs": int(actual_stats["relevant_pairs"]),
            "unknown_test_pairs_filtered": unknown_test_pairs,
            "metrics": metrics,
            "note": (
                "Actuals are post-split items not seen in training. "
                "Training-seen items are removed from the candidate list."
            ),
        },
        "cold_start_baseline": cold_start,
        "notes": [
            "Implicit feedback has no explicit ratings or reliable negative "
            "samples; RMSE is not applicable and is not reported.",
            "The split is by event timestamp quantile, not random row sampling.",
            "Test users unseen in training are excluded from ALS evaluation and "
            "their ratio is reported.",
            "K=10 is the primary reported cutoff; K=20 is included for context.",
        ],
    }

    EVALUATION_RESULTS_PATH.write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    emit(result)

    predictions.unpersist()
    joined.unpersist()
    actual.unpersist()
    recs.unpersist()
    train_ratings.unpersist()
    test_pairs.unpersist()
    train_items.unpersist()
    overlap_users.unpersist()
    cold_users.unpersist()
    test_users.unpersist()
    train_users.unpersist()
    behaviors.unpersist()
    return result


def parse_k_values(value: str) -> tuple[int, ...]:
    try:
        parsed = tuple(sorted({int(part.strip()) for part in value.split(",") if part.strip()}))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("k-values must be comma-separated integers") from exc
    if not parsed:
        raise argparse.ArgumentTypeError("k-values cannot be empty")
    return parsed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rank", type=int, default=DEFAULT_RANK)
    parser.add_argument("--max-iter", type=int, default=DEFAULT_MAX_ITER)
    parser.add_argument("--reg-param", type=float, default=DEFAULT_REG_PARAM)
    parser.add_argument("--alpha", type=float, default=DEFAULT_ALPHA)
    parser.add_argument("--split-quantile", type=float, default=SPLIT_QUANTILE)
    parser.add_argument("--k-values", type=parse_k_values, default=DEFAULT_K_VALUES)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = parser.parse_args()

    spark = build_spark_session(
        "SparkInsight-ALSEvaluation", shuffle_partitions=16
    )
    try:
        run(
            spark,
            rank=args.rank,
            max_iter=args.max_iter,
            reg_param=args.reg_param,
            alpha=args.alpha,
            split_quantile=args.split_quantile,
            k_values=args.k_values,
            seed=args.seed,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
