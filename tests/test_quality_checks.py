"""
Unit tests for data quality checks.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.quality.checks import (
    check_amount_ranges,
    check_duplicates,
    check_freshness,
    check_nulls,
    check_referential_integrity,
    check_schema,
    check_transaction_types,
)
from tests.conftest import TRANSACTION_SCHEMA, _txn

_TS = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)


class TestSchemaCheck:
    def test_passes_with_correct_schema(self, spark, sample_transactions):
        result = check_schema(sample_transactions)
        assert result.passed

    def test_fails_with_missing_column(self, spark, sample_transactions):
        df_missing = sample_transactions.drop("transaction_id")
        result = check_schema(df_missing)
        assert not result.passed
        assert "transaction_id" in result.message


class TestNullCheck:
    def test_passes_when_no_nulls(self, spark, sample_transactions):
        result = check_nulls(sample_transactions)
        assert result.passed

    def test_fails_when_null_in_required_column(self, spark):
        rows = [
            (None, "card_a", "MER0001", "5411", Decimal("50.0"), "USD",
             _TS, "purchase", "US", "ACQ0001", "ISS0001", "00", False),
        ]
        df = spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)
        result = check_nulls(df, required_columns=["transaction_id"])
        assert not result.passed
        assert "transaction_id" in result.message


class TestAmountRangeCheck:
    def test_passes_for_valid_amounts(self, spark, sample_transactions):
        result = check_amount_ranges(sample_transactions)
        assert result.passed

    def test_fails_for_positive_refund(self, spark):
        """A refund with a positive amount violates the sign convention."""
        rows = [_txn("bad_refund", amount=50.0, txn_type="refund")]
        # Manually override the amount sign to create a violation
        df = spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)
        result = check_amount_ranges(df)
        assert not result.passed

    def test_fails_for_negative_purchase(self, spark):
        rows = [_txn("bad_purchase", amount=-50.0, txn_type="purchase")]
        df = spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)
        result = check_amount_ranges(df)
        assert not result.passed


class TestDuplicateCheck:
    def test_passes_with_unique_ids(self, spark, sample_transactions):
        result = check_duplicates(sample_transactions)
        assert result.passed

    def test_fails_with_duplicate_id(self, spark):
        rows = [
            _txn("dup-001", "card_a"),
            _txn("dup-001", "card_b"),   # Same transaction_id
        ]
        df = spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)
        result = check_duplicates(df)
        assert not result.passed
        assert result.metric_value == 1.0


class TestFreshnessCheck:
    def test_passes_for_recent_data(self, spark):
        now = datetime.now(tz=timezone.utc)
        rows = [_txn("fresh", ts=now)]
        df = spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)
        result = check_freshness(df, max_age_hours=2.0)
        assert result.passed

    def test_fails_for_stale_data(self, spark):
        old_ts = datetime(2020, 1, 1, tzinfo=timezone.utc)
        rows = [_txn("stale", ts=old_ts)]
        df = spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)
        result = check_freshness(df, max_age_hours=2.0)
        assert not result.passed


class TestTransactionTypeCheck:
    def test_passes_for_valid_types(self, spark, sample_transactions):
        result = check_transaction_types(sample_transactions)
        assert result.passed

    def test_fails_for_unknown_type(self, spark):
        rows = [_txn("bad_type", txn_type="wire_transfer")]
        df = spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)
        result = check_transaction_types(df)
        assert not result.passed
        assert "wire_transfer" in result.message


class TestReferentialIntegrity:
    def test_passes_when_all_merchants_exist(self, spark, sample_transactions):
        dim_schema = StructType([StructField("merchant_id", StringType(), False)])
        dim_df = spark.createDataFrame(
            [("MER0000001",)], schema=dim_schema
        )
        result = check_referential_integrity(sample_transactions, dim_df)
        assert result.passed

    def test_fails_with_unknown_merchant(self, spark, sample_transactions):
        # Dimension table is empty — all merchants will be orphaned
        dim_schema = StructType([StructField("merchant_id", StringType(), False)])
        empty_dim = spark.createDataFrame([], schema=dim_schema)
        result = check_referential_integrity(sample_transactions, empty_dim)
        assert not result.passed

    def test_warns_when_no_dimension_provided(self, spark, sample_transactions):
        result = check_referential_integrity(sample_transactions, dimension_df=None)
        assert result.status == "WARN"
