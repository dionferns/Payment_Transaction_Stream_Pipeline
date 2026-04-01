"""
Unit tests for windowed streaming aggregation logic.

Aggregations are tested against static DataFrames — the window functions
behave identically in batch and streaming contexts.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from src.streaming.aggregations import (
    card_daily_running_total,
    issuer_cross_border_ratio_1h,
    merchant_volume_5min,
)
from tests.conftest import TRANSACTION_SCHEMA, _txn

_TS = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)


class TestMerchantVolume5Min:
    def test_counts_purchases_only(self, spark):
        """Refunds and chargebacks should not be included in merchant volume."""
        rows = [
            _txn("p1", merchant="MER0000001", txn_type="purchase", amount=100.0, ts=_TS),
            _txn("r1", merchant="MER0000001", txn_type="refund",   amount=-50.0, ts=_TS),
        ]
        df = spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)
        result = merchant_volume_5min(df)
        row = result.filter(F.col("merchant_id") == "MER0000001").first()
        assert row["txn_count"] == 1
        assert float(row["total_value"]) == pytest.approx(100.0, rel=1e-3)

    def test_aggregates_per_merchant(self, spark):
        """Two merchants in the same window should produce two rows."""
        rows = [
            _txn("p1", merchant="MER0000001", ts=_TS, amount=100.0),
            _txn("p2", merchant="MER0000002", ts=_TS + timedelta(seconds=10), amount=200.0),
        ]
        df = spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)
        result = merchant_volume_5min(df)
        assert result.count() == 2

    def test_output_schema_contains_expected_columns(self, spark, sample_transactions):
        result = merchant_volume_5min(sample_transactions)
        expected = {"window_start", "window_end", "merchant_id", "txn_count", "total_value", "unique_cards"}
        assert expected.issubset(set(result.columns))


class TestIssuerCrossBorderRatio:
    def test_calculates_ratio_correctly(self, spark):
        rows = [
            _txn("x1", issuer="ISS0001", ts=_TS, cross_border=True),
            _txn("x2", issuer="ISS0001", ts=_TS + timedelta(minutes=5), cross_border=False),
            _txn("x3", issuer="ISS0001", ts=_TS + timedelta(minutes=10), cross_border=False),
            _txn("x4", issuer="ISS0001", ts=_TS + timedelta(minutes=15), cross_border=True),
        ]
        df = spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)
        result = issuer_cross_border_ratio_1h(df)
        row = result.filter(F.col("issuer_id") == "ISS0001").first()
        assert row["total_txns"] == 4
        assert row["cross_border_txns"] == 2
        assert float(row["cross_border_ratio"]) == pytest.approx(0.5, abs=0.001)

    def test_zero_cross_border(self, spark):
        rows = [
            _txn("d1", issuer="ISS0002", ts=_TS, cross_border=False),
            _txn("d2", issuer="ISS0002", ts=_TS + timedelta(minutes=5), cross_border=False),
        ]
        df = spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)
        result = issuer_cross_border_ratio_1h(df)
        row = result.filter(F.col("issuer_id") == "ISS0002").first()
        assert float(row["cross_border_ratio"]) == pytest.approx(0.0, abs=0.001)


class TestCardDailyRunningTotal:
    def test_aggregates_daily_spend_per_card(self, spark):
        rows = [
            _txn("c1", card="card_x", ts=_TS, amount=100.0, txn_type="purchase"),
            _txn("c2", card="card_x", ts=_TS + timedelta(hours=1), amount=200.0, txn_type="purchase"),
            _txn("c3", card="card_y", ts=_TS, amount=50.0, txn_type="purchase"),
        ]
        df = spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)
        result = card_daily_running_total(df)
        card_x = result.filter(F.col("card_hash") == "card_x").first()
        assert card_x["daily_txn_count"] == 2
        assert float(card_x["daily_spend"]) == pytest.approx(300.0, rel=1e-3)

    def test_excludes_refunds_from_spend(self, spark):
        rows = [
            _txn("r1", card="card_z", ts=_TS, amount=100.0, txn_type="purchase"),
            _txn("r2", card="card_z", ts=_TS + timedelta(minutes=5), amount=-50.0, txn_type="refund"),
        ]
        df = spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)
        result = card_daily_running_total(df)
        # Only purchases are included
        row = result.filter(F.col("card_hash") == "card_z").first()
        assert row["daily_txn_count"] == 1
