"""
Windowed aggregation logic for the streaming layer.

All aggregations use Spark's built-in window functions rather than UDFs to
keep execution on the JVM and avoid Python serialisation overhead.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


def merchant_volume_5min(df: DataFrame) -> DataFrame:
    """Rolling 5-minute transaction volume and value aggregated per merchant.

    Emits one row per (merchant_id, window) with:
      - ``txn_count``  : number of transactions in the window
      - ``total_value``: sum of absolute amounts in the window
      - ``window_start``, ``window_end``: inclusive bounds of the 5-min window

    Parameters
    ----------
    df:
        Streaming DataFrame with at least ``merchant_id``, ``amount``,
        ``timestamp`` (TimestampType) columns.
    """
    return (
        df.filter(F.col("transaction_type").isin("purchase", "p2p"))
        .withWatermark("timestamp", "10 minutes")
        .groupBy(
            F.window(F.col("timestamp"), "5 minutes"),
            F.col("merchant_id"),
        )
        .agg(
            F.count("*").alias("txn_count"),
            F.sum(F.abs(F.col("amount").cast(DoubleType()))).alias("total_value"),
            F.countDistinct("card_hash").alias("unique_cards"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "merchant_id",
            "txn_count",
            "total_value",
            "unique_cards",
        )
    )


def issuer_cross_border_ratio_1h(df: DataFrame) -> DataFrame:
    """Rolling 1-hour cross-border transaction ratio per issuer.

    Emits one row per (issuer_id, window) with:
      - ``total_txns``      : all transactions in the window
      - ``cross_border_txns``: transactions with ``is_cross_border=True``
      - ``cross_border_ratio``: proportion of cross-border transactions

    A high ratio can indicate compromised cards being used abroad.
    """
    return (
        df.withWatermark("timestamp", "15 minutes")
        .groupBy(
            F.window(F.col("timestamp"), "1 hour", "15 minutes"),
            F.col("issuer_id"),
        )
        .agg(
            F.count("*").alias("total_txns"),
            F.sum(F.col("is_cross_border").cast("int")).alias("cross_border_txns"),
        )
        .withColumn(
            "cross_border_ratio",
            F.round(
                F.col("cross_border_txns") / F.col("total_txns"),
                4,
            ),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "issuer_id",
            "total_txns",
            "cross_border_txns",
            "cross_border_ratio",
        )
    )


def card_daily_running_total(df: DataFrame) -> DataFrame:
    """Running daily total spend per card hash.

    Groups by (card_hash, date) so the downstream query can detect cards
    exceeding daily limits.  Note: this is a session-window approximation
    using ``date``-level partitioning; for exact running totals use a
    stateful foreachBatch with a persistent key-value store.
    """
    return (
        df.filter(F.col("transaction_type").isin("purchase", "p2p"))
        .withColumn("date", F.to_date(F.col("timestamp")))
        .withWatermark("timestamp", "10 minutes")
        .groupBy("card_hash", "date")
        .agg(
            F.count("*").alias("daily_txn_count"),
            F.sum(F.abs(F.col("amount").cast(DoubleType()))).alias("daily_spend"),
            F.max("timestamp").alias("last_seen"),
        )
    )
