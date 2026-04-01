"""
Unit tests for fraud detection rules.

All tests use local DataFrames — no Spark cluster required.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from src.streaming.fraud_rules import (
    apply_all_fraud_rules,
    flag_amount_anomaly,
    flag_geo_impossibility,
    flag_velocity,
)
from tests.conftest import TRANSACTION_SCHEMA, _txn

_TS = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)


class TestVelocityRule:
    def test_flags_card_exceeding_threshold(self, spark, velocity_transactions):
        """Six transactions in 60s from the same card should be flagged."""
        result = flag_velocity(velocity_transactions, window_seconds=60, max_txns=5)
        flagged = result.filter(F.col("fraud_velocity")).count()
        assert flagged > 0

    def test_does_not_flag_normal_velocity(self, spark, sample_transactions):
        """Sample data has at most 2 txns per card — should not be flagged."""
        result = flag_velocity(sample_transactions, window_seconds=60, max_txns=5)
        flagged = result.filter(F.col("fraud_velocity")).count()
        assert flagged == 0

    def test_respects_window_boundary(self, spark):
        """Transactions spread over 2 minutes should not trigger a 60s window."""
        rows = [
            _txn(f"v{i}", "spread_card", ts=_TS + timedelta(seconds=i * 30), amount=10.0)
            for i in range(6)  # One every 30s = 2.5 minutes total
        ]
        df = spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)
        result = flag_velocity(df, window_seconds=60, max_txns=5)
        # Within any 60s window there are at most 2 transactions
        flagged = result.filter(F.col("fraud_velocity")).count()
        assert flagged == 0


class TestAmountAnomalyRule:
    def test_flags_large_outlier(self, spark):
        """A single transaction that is 10x the rolling average should be flagged."""
        rows = [
            _txn(f"a{i}", "avg_card", ts=_TS + timedelta(seconds=i), amount=50.0)
            for i in range(5)
        ] + [
            _txn("anomaly", "avg_card", ts=_TS + timedelta(seconds=10), amount=5000.0)
        ]
        df = spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)
        result = flag_amount_anomaly(df, multiplier=3.0, min_samples=3)
        flagged = result.filter(F.col("fraud_amount_anomaly")).count()
        assert flagged >= 1

    def test_does_not_flag_without_min_samples(self, spark):
        """Rule should not fire if the card has fewer than min_samples prior txns."""
        rows = [
            _txn("b1", "new_card", ts=_TS, amount=50.0),
            _txn("b2", "new_card", ts=_TS + timedelta(seconds=1), amount=5000.0),
        ]
        df = spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)
        result = flag_amount_anomaly(df, multiplier=3.0, min_samples=3)
        flagged = result.filter(F.col("fraud_amount_anomaly")).count()
        assert flagged == 0


class TestGeoImpossibilityRule:
    def test_flags_two_countries_in_window(self, spark, geo_impossible_transactions):
        """Same card in two countries within 30 minutes should be flagged."""
        result = flag_geo_impossibility(geo_impossible_transactions, window_minutes=30)
        flagged = result.filter(F.col("fraud_geo_impossibility")).count()
        assert flagged > 0

    def test_does_not_flag_domestic_travel(self, spark):
        """Transactions in the same country should never trigger geo-impossibility."""
        rows = [
            _txn("d1", "domestic_card", ts=_TS, country="US"),
            _txn("d2", "domestic_card", ts=_TS + timedelta(minutes=10), country="US"),
        ]
        df = spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)
        result = flag_geo_impossibility(df, window_minutes=30)
        flagged = result.filter(F.col("fraud_geo_impossibility")).count()
        assert flagged == 0

    def test_does_not_flag_outside_window(self, spark):
        """Two-country activity separated by >30 min should not trigger the rule."""
        rows = [
            _txn("w1", "slow_card", ts=_TS, country="US"),
            _txn("w2", "slow_card", ts=_TS + timedelta(hours=2), country="GB"),
        ]
        df = spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)
        result = flag_geo_impossibility(df, window_minutes=30)
        flagged = result.filter(F.col("fraud_geo_impossibility")).count()
        assert flagged == 0


class TestApplyAllFraudRules:
    def test_composite_output_columns(self, spark, sample_transactions):
        """apply_all_fraud_rules must always produce is_fraud_flagged and fraud_reasons."""
        result = apply_all_fraud_rules(sample_transactions)
        assert "is_fraud_flagged" in result.columns
        assert "fraud_reasons" in result.columns

    def test_no_intermediate_columns_leaked(self, spark, sample_transactions):
        """Internal working columns should be dropped from the final output."""
        result = apply_all_fraud_rules(sample_transactions)
        for col in ("epoch", "velocity_count", "rolling_avg", "amount_dbl"):
            assert col not in result.columns

    def test_row_count_preserved(self, spark, sample_transactions):
        """Applying fraud rules must not change the row count."""
        result = apply_all_fraud_rules(sample_transactions)
        assert result.count() == sample_transactions.count()
