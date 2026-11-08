"""Two-stage item aggregation, with configurable salt scope.

Why the baseline keeps two stages
---------------------------------
The benchmark writes every variant as the same *logical* two-stage aggregation:
partial metrics, then key restoration.  The controlled treatment is the salt
scope (none / all / hot-only).  This is not a claim that the physical plans are
identical: the unsalted two ``groupBy("item_id")`` calls can be collapsed by
Catalyst into one shuffle, while any salted variant must shuffle twice because
stage 1 groups by ``(item_id, _salt)`` and stage 2 groups by ``item_id``.  That
extra shuffle is a real cost of salting, so the benchmark reports stage counts
and shuffle bytes rather than pretending the plans differ only in one key bit.

The hot-key problem
-------------------
``groupBy("item_id")`` hashes every row by item_id.  When one item owns 38% of
a 1,000,000-row table, one reducer receives about 380,000 rows while the other
15 reducers receive comparatively little.  The longest task dominates the
stage, and in a real cluster that reducer may spill or OOM.

Salt scopes
-----------
``none``
    Keep the original ``item_id`` key.  This is the control.
``all``
    Give every row a deterministic salt in ``[0, N)`` and group by
    ``(item_id, _salt)``.  This is the full-salting treatment.
``hot_only``
    Give the selected ``hot_item_id`` the same deterministic salt, and give
    every other item ``_salt = 0``.  Only the hot key is split; the other keys
    keep one logical group.  This is selective salting.

How salting works
-----------------
1. Append a deterministic salt to the grouping key:
   ``(item_id, _salt)``.  In ``all`` mode every item is split; in
   ``hot_only`` mode only the hot item is split.
2. Aggregate once per ``(item_id, _salt)``.  The hot key's 380k rows are spread
   over N reducers instead of one.
3. Remove the salt in stage 2.  Group by the original ``item_id`` and sum the
   partial aggregates.  This is the key-restoration step: stage 2 sees at
   most N partial rows per item, not the raw behavior rows.

Choosing N
----------
There is no universal salt count.  A practical rule is:

    N ~= ceil(hot_key_rows / target_rows_per_task)

For this local dataset: 380k hot rows / ~38k target rows = 10.  More buckets
spread the hot key further but increase the number of distinct stage-1 groups,
serialization cost, and the number of rows shuffled for the final reduction.
In production N is usually sized with the actual shuffle-partition count and
the hottest-key cardinality from a skew probe; monitor added shuffle bytes and
driver planning overhead before increasing it.

Determinism
-----------
The salt is calculated from high-cardinality row fields (user_id and
event_time), not from ``rand()``.  Re-running the same job therefore assigns
the same rows to the same salt buckets, which makes performance runs and result
reconciliation repeatable.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F

SALT_SCOPES = ("none", "all", "hot_only")


def _salt_expression(salt_buckets: int) -> Column:
    """Return a deterministic salt in [0, salt_buckets).

    user_id alone is not enough: one user's repeated activity on the hot item
    would all land in one bucket.  Adding event_time (cast to epoch seconds)
    gives each behavior row a high-cardinality, deterministic component.
    """

    return (
        F.pmod(
            F.xxhash64(
                F.col("user_id"),
                F.col("behavior_type"),
                F.col("event_time").cast("long"),
            ),
            F.lit(salt_buckets),
        )
        .cast("int")
    )


def add_salt_key(
    behavior_df: DataFrame,
    *,
    salt_scope: str,
    salt_buckets: int,
    hot_item_id: int | None = None,
) -> DataFrame:
    """Add a deterministic ``_salt`` column according to ``salt_scope``.

    ``none`` returns the input unchanged; callers should use
    :func:`build_item_stats` rather than adding a salt key alone.
    """

    if salt_scope not in SALT_SCOPES:
        raise ValueError(
            f"salt_scope must be one of {SALT_SCOPES}, got {salt_scope!r}"
        )
    if salt_buckets < 1:
        raise ValueError("salt_buckets must be >= 1")
    if salt_scope == "none":
        return behavior_df
    if salt_buckets < 2:
        raise ValueError("salted aggregation needs salt_buckets >= 2")

    if salt_scope == "all":
        salt_expression = _salt_expression(salt_buckets)
    else:
        if hot_item_id is None:
            raise ValueError("hot_item_id is required when salt_scope='hot_only'")
        salt_expression = (
            F.when(
                F.col("item_id") == F.lit(hot_item_id),
                _salt_expression(salt_buckets),
            )
            .otherwise(F.lit(0))
            .cast("int")
        )

    return behavior_df.withColumn("_salt", salt_expression)


def _partial_metrics() -> list[Column]:
    return [
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
        F.min("event_time").alias("first_event_time"),
        F.max("event_time").alias("last_event_time"),
    ]


def build_item_stats(
    behavior_df: DataFrame,
    *,
    salt_scope: str = "none",
    salt_buckets: int = 10,
    hot_item_id: int | None = None,
) -> DataFrame:
    """Build per-item event metrics using a two-stage aggregation.

    Parameters
    ----------
    behavior_df:
        DWD behavior rows with ``user_id``, ``item_id``, ``behavior_type``,
        and ``event_time`` columns.
    salt_scope:
        ``none`` runs the unsalted control; ``all`` salts every item;
        ``hot_only`` salts only ``hot_item_id`` and gives every other item
        ``_salt = 0``.
    salt_buckets:
        N in the explanation above.  Ignored for ``salt_scope='none'``.
    hot_item_id:
        Required when ``salt_scope='hot_only'``.  The single hot item to split.
    """

    enriched = add_salt_key(
        behavior_df,
        salt_scope=salt_scope,
        salt_buckets=salt_buckets,
        hot_item_id=hot_item_id,
    )
    stage_one_keys = ["item_id"]
    if salt_scope != "none":
        stage_one_keys.append("_salt")

    # Stage 1: partial aggregation.  With salting enabled the hot item is
    # intentionally split into salt_buckets independent groups (or, for
    # hot_only, only the selected hot item is split).
    partial = enriched.groupBy(*stage_one_keys).agg(*_partial_metrics())

    # Stage 2: restore the original business key by dropping "_salt" and
    # summing/counting the partial rows per item_id.  This stage handles at
    # most one row per item per salt bucket, never the raw 380k hot rows.
    final = partial.groupBy("item_id").agg(
        F.sum("event_count").alias("event_count"),
        F.sum("pv_count").alias("pv_count"),
        F.sum("cart_count").alias("cart_count"),
        F.sum("fav_count").alias("fav_count"),
        F.sum("buy_count").alias("buy_count"),
        F.min("first_event_time").alias("first_event_time"),
        F.max("last_event_time").alias("last_event_time"),
    )
    return final
