"""
Pytest configuration and shared fixtures for the payment stream pipeline test suite.

Fixtures create small, local SparkSessions and sample DataFrames so that tests
run quickly without requiring a Spark cluster.  ``chispa`` is used for
DataFrame equality assertions.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Single local SparkSession shared across the entire test run."""
    return (
        SparkSession.builder.master("local[2]")
        .appName("payment-pipeline-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "false")  # Deterministic in tests
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",         StringType(),       False),
    StructField("card_hash",              StringType(),       False),
    StructField("merchant_id",            StringType(),       False),
    StructField("merchant_category_code", StringType(),       True),
    StructField("amount",                 DecimalType(18, 2), False),
    StructField("currency",               StringType(),       True),
    StructField("timestamp",              TimestampType(),    False),
    StructField("transaction_type",       StringType(),       False),
    StructField("country_code",           StringType(),       True),
    StructField("acquirer_id",            StringType(),       True),
    StructField("issuer_id",              StringType(),       True),
    StructField("response_code",          StringType(),       True),
    StructField("is_cross_border",        BooleanType(),      True),
])

_TS = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)


def _txn(
    txn_id: str = "txn-001",
    card: str = "abc123",
    merchant: str = "MER0000001",
    mcc: str = "5411",
    amount: float = 50.0,
    currency: str = "USD",
    ts: datetime = _TS,
    txn_type: str = "purchase",
    country: str = "US",
    acquirer: str = "ACQ0001",
    issuer: str = "ISS0001",
    response: str = "00",
    cross_border: bool = False,
) -> tuple:
    return (
        txn_id,
        card,
        merchant,
        mcc,
        Decimal(str(amount)),
        currency,
        ts,
        txn_type,
        country,
        acquirer,
        issuer,
        response,
        cross_border,
    )


@pytest.fixture
def sample_transactions(spark: SparkSession):
    """A small mixed-type transaction DataFrame for broad tests."""
    rows = [
        _txn("t1", "card_a", amount=100.0,   txn_type="purchase"),
        _txn("t2", "card_b", amount=200.0,   txn_type="purchase"),
        _txn("t3", "card_a", amount=-50.0,   txn_type="refund"),
        _txn("t4", "card_c", amount=-80.0,   txn_type="chargeback"),
        _txn("t5", "card_d", amount=30.0,    txn_type="p2p"),
        _txn("t6", "card_b", amount=150.0,   txn_type="purchase", cross_border=True, country="GB"),
    ]
    return spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)


@pytest.fixture
def velocity_transactions(spark: SparkSession):
    """Six transactions from the same card within one minute — triggers velocity rule."""
    from datetime import timedelta
    base = _TS
    rows = [
        _txn(f"v{i}", "hot_card", ts=base + timedelta(seconds=i * 5), amount=10.0)
        for i in range(6)
    ]
    return spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)


@pytest.fixture
def geo_impossible_transactions(spark: SparkSession):
    """Same card in US and GB within 20 minutes — triggers geo-impossibility rule."""
    from datetime import timedelta
    rows = [
        _txn("g1", "travel_card", ts=_TS,                              country="US"),
        _txn("g2", "travel_card", ts=_TS + timedelta(minutes=15),      country="GB"),
    ]
    return spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)
