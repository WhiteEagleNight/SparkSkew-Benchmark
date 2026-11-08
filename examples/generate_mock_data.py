#!/usr/bin/env python3
"""Generate a reproducible e-commerce behavior log for SparkSkew-Benchmark.

Design goals
------------
1. At least 1,000,000 rows by default so that shuffle behavior is observable.
2. One hot item (default 9999) receives >= 35% of all rows, which intentionally
   creates a single oversized shuffle key for the skew benchmark.
3. About 10% of users are only active after the nominal 80% time boundary.
   They become genuine cold-start users for the time-based ALS evaluation.
4. The same seed always produces the same file (byte-for-byte, given Python's
   deterministic random module).

The file is intentionally generated with the Python standard library only so
that data generation does not require a Spark session or third-party packages.

Schema of data/raw/behavior.csv:
    user_id,item_id,category_id,behavior_type,event_time

event_time is written with microsecond precision. The primary key used in the
DWD job is (user_id, item_id, event_time).
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "raw" / "behavior.csv"

START_TIME = datetime(2025, 1, 1, 0, 0, 0)
WINDOW_DAYS = 90
WINDOW_SECONDS = WINDOW_DAYS * 24 * 60 * 60


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rows",
        type=int,
        default=1_000_000,
        help="Number of behavior rows (default: 1,000,000).",
    )
    parser.add_argument(
        "--users",
        type=int,
        default=50_000,
        help="Size of the user id pool, ids are 0..users-1 (default: 50000).",
    )
    parser.add_argument(
        "--items",
        type=int,
        default=20_000,
        help="Number of item ids, 1..items (default: 20000).",
    )
    parser.add_argument(
        "--hot-item",
        type=int,
        default=9_999,
        help="Item id that receives the engineered traffic spike (default: 9999).",
    )
    parser.add_argument(
        "--hot-ratio",
        type=float,
        default=0.38,
        help="Target share of rows for the hot item, must be >= 0.35 (default: 0.38).",
    )
    parser.add_argument(
        "--late-user-share",
        type=float,
        default=0.10,
        help="Share of users whose events are all after the 80%% time boundary (default: 0.10).",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42).")
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Output CSV path (default: data/raw/behavior.csv).",
    )
    args = parser.parse_args()

    if args.rows <= 0:
        parser.error("--rows must be positive")
    if args.users < 2:
        parser.error("--users must be >= 2")
    if args.items < 2:
        parser.error("--items must be >= 2")
    if not 1 <= args.hot_item <= args.items:
        parser.error("--hot-item must be within 1..--items")
    if not 0.0 <= args.hot_ratio <= 1.0:
        parser.error("--hot-ratio must be within [0, 1]")
    if not 0.0 <= args.late_user_share < 1.0:
        parser.error("--late-user-share must be within [0, 1)")
    return args


def generate(args: argparse.Namespace) -> dict:
    args.output.parent.mkdir(parents=True, exist_ok=True)
    rng = random.Random(args.seed)

    late_user_count = int(args.users * args.late_user_share)
    late_user_start = args.users - late_user_count
    split_second = int(WINDOW_SECONDS * 0.8)
    behaviors = ["pv", "cart", "fav", "buy"]
    behavior_weights = [0.85, 0.07, 0.03, 0.05]
    # Pre-drawing behavior labels is materially faster than a weighted choice
    # per row and keeps the generated distribution exactly reproducible.
    behavior_labels = rng.choices(
        behaviors, weights=behavior_weights, k=args.rows
    )

    seen_users: set[int] = set()
    seen_items: set[int] = set()
    behavior_counts: Counter[str] = Counter()
    used_timestamps: set[datetime] = set()
    hot_rows = 0
    written = 0
    started = time.perf_counter()

    header = "user_id,item_id,category_id,behavior_type,event_time\n"
    with args.output.open("w", encoding="utf-8", newline="") as fh:
        fh.write(header)
        for i in range(args.rows):
            user_id = rng.randrange(args.users)
            seen_users.add(user_id)

            if rng.random() < args.hot_ratio:
                item_id = args.hot_item
                hot_rows += 1
            else:
                item_id = rng.randrange(1, args.items)
                if item_id >= args.hot_item:
                    item_id += 1
            seen_items.add(item_id)
            category_id = (item_id * 37 + 11) % 100

            # Users in the late cohort never appear before the 80% boundary.
            # Therefore every one of them is a real cold-start user under a
            # timestamp-quantile split, not a user removed by an artificial
            # dataframe filter.
            if user_id >= late_user_start and late_user_count > 0:
                second_offset = rng.randrange(split_second, WINDOW_SECONDS)
            else:
                second_offset = rng.randrange(0, split_second)

            event_time = START_TIME + timedelta(
                seconds=second_offset, microseconds=rng.randrange(1_000_000)
            )
            # The probability of a timestamp collision at microsecond
            # resolution over 90 days is negligible, but avoiding duplicates
            # keeps the DWD primary-key assertion honest for this dataset.
            if event_time in used_timestamps:
                event_time += timedelta(microseconds=1)
                while event_time in used_timestamps:
                    event_time += timedelta(microseconds=1)
            used_timestamps.add(event_time)

            behavior = behavior_labels[i]
            behavior_counts[behavior] += 1
            fh.write(
                f"{user_id},{item_id},{category_id},{behavior},"
                f"{event_time:%Y-%m-%d %H:%M:%S.%f}\n"
            )
            written += 1

            if written % 200_000 == 0:
                print(
                    f"[generate_mock_data] progress={written:,}/{args.rows:,} "
                    f"elapsed={time.perf_counter() - started:.1f}s",
                    file=sys.stderr,
                    flush=True,
                )

    elapsed = time.perf_counter() - started
    hot_share = hot_rows / written if written else 0.0
    result = {
        "status": "ok",
        "output": str(args.output.resolve()),
        "rows": written,
        "unique_users": len(seen_users),
        "unique_items": len(seen_items),
        "hot_item_id": args.hot_item,
        "hot_item_rows": hot_rows,
        "hot_item_share": round(hot_share, 6),
        "hot_item_share_pct": f"{hot_share * 100:.2f}%",
        "behavior_counts": dict(sorted(behavior_counts.items())),
        "late_user_share_configured": args.late_user_share,
        "late_user_count": late_user_count,
        "seed": args.seed,
        "time_window": [
            START_TIME.isoformat(),
            (START_TIME + timedelta(seconds=WINDOW_SECONDS - 1)).isoformat(),
        ],
        "elapsed_sec": round(elapsed, 3),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return result


def main() -> None:
    args = parse_args()
    generate(args)


if __name__ == "__main__":
    main()
