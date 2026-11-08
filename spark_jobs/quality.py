"""Data-quality assertions used by the DWD and DWS jobs.

Every check raises AssertionError on failure.  A failed pipeline therefore
returns a non-zero exit code instead of silently publishing a broken table.
The returned dictionaries are printed as a small JSON audit record.
"""

from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _pass(name: str, details: str) -> None:
    print(f"[ASSERT][PASS] {name}: {details}", flush=True)


def assert_non_empty(
    df: DataFrame, table_name: str, *, actual_rows: int | None = None
) -> dict:
    """Assertion 1: a published table must contain at least one row."""

    rows = df.count() if actual_rows is None else actual_rows
    if rows <= 0:
        raise AssertionError(f"{table_name} is empty (rows={rows})")
    details = f"rows={rows:,} > 0"
    _pass("row_count_positive", f"{table_name} {details}")
    return {
        "assertion": "row_count_positive",
        "table": table_name,
        "rows": rows,
        "status": "PASS",
    }


def assert_unique_key(
    df: DataFrame,
    columns: Iterable[str],
    table_name: str,
    *,
    actual_rows: int | None = None,
) -> dict:
    """Assertion 2: the declared primary key must be unique."""

    columns = list(columns)
    total = df.count() if actual_rows is None else actual_rows
    distinct = df.select(*columns).distinct().count()
    duplicates = total - distinct
    if duplicates != 0:
        examples = (
            df.groupBy(*columns)
            .count()
            .filter(F.col("count") > 1)
            .orderBy(F.desc("count"))
            .limit(5)
            .collect()
        )
        raise AssertionError(
            f"{table_name} primary key {columns} has {duplicates:,} duplicate "
            f"key(s). Examples: {[row.asDict() for row in examples]}"
        )
    details = f"key={columns} distinct={distinct:,} total={total:,} duplicates=0"
    _pass("primary_key_unique", f"{table_name} {details}")
    return {
        "assertion": "primary_key_unique",
        "table": table_name,
        "columns": columns,
        "distinct_keys": distinct,
        "rows": total,
        "duplicates": duplicates,
        "status": "PASS",
    }


def assert_non_null_rate(
    df: DataFrame,
    columns: Iterable[str],
    threshold: float,
    table_name: str,
    *,
    actual_rows: int | None = None,
) -> dict:
    """Assertion 3: key-field non-null (and non-blank) rate >= threshold.

    Blank strings are treated as missing so that an empty CSV value cannot
    sneak through a technically non-null check.
    """

    columns = list(columns)
    total = df.count() if actual_rows is None else actual_rows
    dtypes = dict(df.dtypes)
    invalid_exprs = []
    for column in columns:
        if column not in dtypes:
            raise KeyError(f"{table_name}: column {column!r} does not exist")
        if dtypes[column] == "string":
            invalid = F.when(
                F.col(column).isNull() | (F.trim(F.col(column)) == ""), 1
            ).otherwise(0)
        else:
            invalid = F.when(F.col(column).isNull(), 1).otherwise(0)
        invalid_exprs.append(F.sum(invalid).alias(column))

    invalid_counts = df.select(*invalid_exprs).first().asDict()
    rates = {
        column: round((total - invalid_counts[column]) / total, 6)
        for column in columns
    }
    failing = {column: rate for column, rate in rates.items() if rate < threshold}
    if failing:
        raise AssertionError(
            f"{table_name} non-null rate below {threshold}: {failing}"
        )
    details = f"threshold={threshold} rates={rates}"
    _pass("key_non_null_rate", f"{table_name} {details}")
    return {
        "assertion": "key_non_null_rate",
        "table": table_name,
        "columns": columns,
        "threshold": threshold,
        "non_null_rates": rates,
        "status": "PASS",
    }


def assert_equal(
    actual: int, expected: int, name: str, description: str = ""
) -> dict:
    """Assertion 4 for DWS: reconciliation against the DWD detail table."""

    if actual != expected:
        raise AssertionError(
            f"{name} failed: actual={actual:,}, expected={expected:,}. "
            f"{description}"
        )
    details = f"actual={actual:,} == expected={expected:,}"
    _pass(name, details)
    return {
        "assertion": name,
        "actual": actual,
        "expected": expected,
        "status": "PASS",
    }
