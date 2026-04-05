"""
Windowed aggregation logic for the streaming layer.

All aggregations use Spark's built-in window functions rather than UDFs to
keep execution on the JVM and avoid Python serialisation overhead.

SQL implementation
------------------
All three aggregations are now expressed as Spark SQL queries (see
``aggregations_sql.py``).  Watermarks are still applied via the DataFrame API
before the temporary view is registered — this is necessary because watermark
semantics are a streaming property that cannot be set inside a SQL string.
Each function then executes the corresponding SQL template and returns the
result DataFrame with an identical output schema to the original implementation.
"""

from __future__ import annotations

from pyspark.sql import DataFrame

from src.streaming.aggregations_sql import (
    CARD_DAILY_SQL,
    ISSUER_XBORDER_SQL,
    MERCHANT_VOLUME_SQL,
)


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
    df.withWatermark("timestamp", "10 minutes").createOrReplaceTempView(
        "_merchant_volume_input"
    )
    return df.sparkSession.sql(
        MERCHANT_VOLUME_SQL.format(view_name="_merchant_volume_input")
    )


def issuer_cross_border_ratio_1h(df: DataFrame) -> DataFrame:
    """Rolling 1-hour cross-border transaction ratio per issuer.

    Emits one row per (issuer_id, window) with:
      - ``total_txns``      : all transactions in the window
      - ``cross_border_txns``: transactions with ``is_cross_border=True``
      - ``cross_border_ratio``: proportion of cross-border transactions

    A high ratio can indicate compromised cards being used abroad.
    """
    df.withWatermark("timestamp", "15 minutes").createOrReplaceTempView(
        "_issuer_xborder_input"
    )
    return df.sparkSession.sql(
        ISSUER_XBORDER_SQL.format(view_name="_issuer_xborder_input")
    )


def card_daily_running_total(df: DataFrame) -> DataFrame:
    """Running daily total spend per card hash.

    Groups by (card_hash, date) so the downstream query can detect cards
    exceeding daily limits.  Note: this is a session-window approximation
    using ``date``-level partitioning; for exact running totals use a
    stateful foreachBatch with a persistent key-value store.
    """
    df.withWatermark("timestamp", "10 minutes").createOrReplaceTempView(
        "_card_daily_input"
    )
    return df.sparkSession.sql(
        CARD_DAILY_SQL.format(view_name="_card_daily_input")
    )
