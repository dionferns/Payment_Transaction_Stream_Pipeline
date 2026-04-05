"""
PySpark data quality check definitions.

Each check is a pure function that takes a DataFrame and returns a
``CheckResult`` — a dataclass capturing whether the check passed and any
diagnostic metadata.  Checks do not raise exceptions on failure; they return
structured results so the runner can emit a complete report even when multiple
checks fail.

Adding a new check:
  1. Define a function with signature ``(df: DataFrame, **kwargs) -> CheckResult``.
  2. Register it in ``ALL_CHECKS`` at the bottom of this module.

SQL implementation
------------------
Three checks have been rewritten to use Spark SQL (see ``checks_sql.py``):
  - ``check_nulls``         — one scan replaces N separate ``.count()`` actions
  - ``check_duplicates``    — one scan replaces two ``.count()`` actions
  - ``check_amount_ranges`` — one scan replaces a per-type loop of ``.count()`` actions
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, TimestampType

from src.quality.checks_sql import (
    AMOUNT_RANGES_CHECK_SQL,
    DUPLICATES_CHECK_SQL,
    NULLS_CHECK_SQL,
)


@dataclass
class CheckResult:
    """Result of a single data quality check."""

    check_name: str
    status: str           # "PASS" | "FAIL" | "WARN"
    message: str
    metric_value: Optional[float] = None
    threshold: Optional[float] = None
    run_timestamp: str = field(
        default_factory=lambda: datetime.now(tz=timezone.utc).isoformat()
    )
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return self.status == "PASS"


# ---------------------------------------------------------------------------
# Check: Schema validation
# ---------------------------------------------------------------------------

EXPECTED_COLUMNS = {
    "transaction_id":         StringType(),
    "card_hash":              StringType(),
    "merchant_id":            StringType(),
    "merchant_category_code": StringType(),
    "amount":                 DecimalType(18, 2),
    "currency":               StringType(),
    "timestamp":              TimestampType(),
    "transaction_type":       StringType(),
    "country_code":           StringType(),
    "acquirer_id":            StringType(),
    "issuer_id":              StringType(),
    "response_code":          StringType(),
}


def check_schema(df: DataFrame, **kwargs: Any) -> CheckResult:
    """Validate that all expected columns are present.

    Does not enforce exact column types to allow for upstream schema evolution;
    missing columns are a harder failure than type drift.
    """
    actual_cols = set(df.columns)
    expected_cols = set(EXPECTED_COLUMNS.keys())
    missing = expected_cols - actual_cols

    if missing:
        return CheckResult(
            check_name="schema_validation",
            status="FAIL",
            message=f"Missing columns: {sorted(missing)}",
            extra={"missing_columns": sorted(missing)},
        )
    return CheckResult(
        check_name="schema_validation",
        status="PASS",
        message="All expected columns present",
    )


# ---------------------------------------------------------------------------
# Check: Null values on critical fields
# ---------------------------------------------------------------------------

REQUIRED_NON_NULL = [
    "transaction_id",
    "amount",
    "timestamp",
    "card_hash",
    "merchant_id",
]


def check_nulls(
    df: DataFrame,
    required_columns: Optional[list[str]] = None,
    **kwargs: Any,
) -> CheckResult:
    """Fail if any required column contains NULL values.

    Executes a single SQL scan that counts NULLs across all required columns
    simultaneously, replacing the original per-column ``.count()`` loop.
    """
    cols = required_columns or REQUIRED_NON_NULL
    present_cols = [c for c in cols if c in df.columns]

    if not present_cols:
        return CheckResult(
            check_name="null_check",
            status="WARN",
            message="No required columns present in DataFrame — check skipped",
        )

    # Build one CASE WHEN expression per column: single scan for all null counts
    null_exprs = ", ".join(
        f"COUNT(CASE WHEN {col} IS NULL THEN 1 END) AS null_count_{col}"
        for col in present_cols
    )
    df.createOrReplaceTempView("_nulls_check_input")
    row = df.sparkSession.sql(
        NULLS_CHECK_SQL.format(
            view_name="_nulls_check_input",
            null_count_exprs=null_exprs,
        )
    ).collect()[0]

    null_counts = {
        col: row[f"null_count_{col}"]
        for col in present_cols
        if row[f"null_count_{col}"] > 0
    }

    if null_counts:
        return CheckResult(
            check_name="null_check",
            status="FAIL",
            message=f"NULL values found in critical columns: {null_counts}",
            extra={"null_counts": null_counts},
        )
    return CheckResult(
        check_name="null_check",
        status="PASS",
        message="No NULLs in critical columns",
    )


# ---------------------------------------------------------------------------
# Check: Referential integrity (merchant_id in dimension)
# ---------------------------------------------------------------------------

def check_referential_integrity(
    df: DataFrame,
    dimension_df: Optional[DataFrame] = None,
    key_column: str = "merchant_id",
    **kwargs: Any,
) -> CheckResult:
    """Verify all ``key_column`` values exist in the dimension table.

    If ``dimension_df`` is not provided, the check is skipped with a WARN.
    """
    if dimension_df is None:
        return CheckResult(
            check_name="referential_integrity",
            status="WARN",
            message="Dimension DataFrame not provided — check skipped",
        )

    total = df.count()
    orphans = (
        df.join(dimension_df.select(key_column), key_column, "left_anti")
        .count()
    )
    rate = orphans / total if total > 0 else 0.0

    if orphans > 0:
        return CheckResult(
            check_name="referential_integrity",
            status="FAIL",
            message=f"{orphans} records ({rate:.2%}) have unknown {key_column}",
            metric_value=rate,
            extra={"orphan_count": orphans, "key_column": key_column},
        )
    return CheckResult(
        check_name="referential_integrity",
        status="PASS",
        message=f"All {key_column} values found in dimension",
    )


# ---------------------------------------------------------------------------
# Check: Amount range by transaction type
# ---------------------------------------------------------------------------

AMOUNT_RULES: dict[str, tuple[float, float]] = {
    "purchase":    (0.01,      50_000.0),
    "refund":      (-50_000.0, -0.01),
    "chargeback":  (-50_000.0, -0.01),
    "p2p":         (0.01,      10_000.0),
}


def check_amount_ranges(df: DataFrame, **kwargs: Any) -> CheckResult:
    """Validate that amounts conform to per-type sign and magnitude rules.

    Executes a single SQL scan with conditional aggregation across all four
    transaction types simultaneously, replacing the original per-type
    ``.count()`` loop.
    """
    if "transaction_type" not in df.columns or "amount" not in df.columns:
        return CheckResult(
            check_name="amount_range",
            status="WARN",
            message="Required columns not present — check skipped",
        )

    df.createOrReplaceTempView("_amount_ranges_check_input")
    row = df.sparkSession.sql(
        AMOUNT_RANGES_CHECK_SQL.format(view_name="_amount_ranges_check_input")
    ).collect()[0]

    details = {
        txn_type: row[f"{txn_type}_violations"]
        for txn_type in ("purchase", "refund", "chargeback", "p2p")
        if row[f"{txn_type}_violations"] > 0
    }
    violations = sum(details.values())

    if violations:
        return CheckResult(
            check_name="amount_range",
            status="FAIL",
            message=f"{violations} records violate amount range rules",
            extra={"violations_by_type": details},
        )
    return CheckResult(
        check_name="amount_range",
        status="PASS",
        message="All amounts within expected ranges",
    )


# ---------------------------------------------------------------------------
# Check: Duplicate transaction IDs
# ---------------------------------------------------------------------------

def check_duplicates(df: DataFrame, **kwargs: Any) -> CheckResult:
    """Fail if ``transaction_id`` is not unique in this batch.

    Executes a single SQL scan that computes total, distinct, and duplicate
    counts in one pass, replacing the original two separate ``.count()`` calls.
    """
    if "transaction_id" not in df.columns:
        return CheckResult(
            check_name="duplicate_check",
            status="WARN",
            message="transaction_id column not present — check skipped",
        )

    df.createOrReplaceTempView("_duplicates_check_input")
    row = df.sparkSession.sql(
        DUPLICATES_CHECK_SQL.format(view_name="_duplicates_check_input")
    ).collect()[0]

    total = row["total_count"]
    distinct = row["unique_count"]
    duplicates = row["duplicate_count"]

    if duplicates > 0:
        return CheckResult(
            check_name="duplicate_check",
            status="FAIL",
            message=f"{duplicates} duplicate transaction_id(s) detected",
            metric_value=float(duplicates),
            extra={"total": total, "distinct": distinct},
        )
    return CheckResult(
        check_name="duplicate_check",
        status="PASS",
        message=f"All {total} transaction_ids are unique",
    )


# ---------------------------------------------------------------------------
# Check: Data freshness
# ---------------------------------------------------------------------------

def check_freshness(
    df: DataFrame,
    max_age_hours: float = 2.0,
    **kwargs: Any,
) -> CheckResult:
    """Warn if the most recent transaction is older than ``max_age_hours``."""
    if "timestamp" not in df.columns:
        return CheckResult(
            check_name="freshness_check",
            status="WARN",
            message="timestamp column not present — check skipped",
        )

    latest_row = df.agg(F.max("timestamp").alias("latest")).collect()[0]
    latest_ts = latest_row["latest"]
    if latest_ts is None:
        return CheckResult(
            check_name="freshness_check",
            status="WARN",
            message="No records found — cannot assess freshness",
        )

    now = datetime.now(tz=timezone.utc)
    age_hours = (now - latest_ts.replace(tzinfo=timezone.utc)).total_seconds() / 3600

    if age_hours > max_age_hours:
        return CheckResult(
            check_name="freshness_check",
            status="FAIL",
            message=f"Most recent record is {age_hours:.1f}h old (threshold: {max_age_hours}h)",
            metric_value=age_hours,
            threshold=max_age_hours,
        )
    return CheckResult(
        check_name="freshness_check",
        status="PASS",
        message=f"Most recent record is {age_hours:.2f}h old — within threshold",
        metric_value=age_hours,
        threshold=max_age_hours,
    )


# ---------------------------------------------------------------------------
# Check: Valid transaction types
# ---------------------------------------------------------------------------

VALID_TRANSACTION_TYPES = {"purchase", "refund", "chargeback", "p2p"}


def check_transaction_types(
    df: DataFrame,
    valid_types: Optional[set[str]] = None,
    **kwargs: Any,
) -> CheckResult:
    """Fail if any transaction_type value is outside the allowed set."""
    allowed = valid_types or VALID_TRANSACTION_TYPES
    if "transaction_type" not in df.columns:
        return CheckResult(
            check_name="transaction_type_check",
            status="WARN",
            message="transaction_type column not present — check skipped",
        )

    invalid = (
        df.filter(~F.col("transaction_type").isin(list(allowed)))
        .groupBy("transaction_type")
        .count()
        .collect()
    )

    if invalid:
        breakdown = {row["transaction_type"]: row["count"] for row in invalid}
        return CheckResult(
            check_name="transaction_type_check",
            status="FAIL",
            message=f"Invalid transaction_type values: {breakdown}",
            extra={"invalid_types": breakdown},
        )
    return CheckResult(
        check_name="transaction_type_check",
        status="PASS",
        message="All transaction_type values are valid",
    )


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

ALL_CHECKS = [
    check_schema,
    check_nulls,
    check_amount_ranges,
    check_duplicates,
    check_freshness,
    check_transaction_types,
    # check_referential_integrity requires a dimension DF — called explicitly
    # by the runner when the dimension is available.
]
