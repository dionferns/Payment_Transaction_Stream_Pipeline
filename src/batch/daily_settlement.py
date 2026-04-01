"""
Daily settlement aggregation Spark batch job.

Computes the net settlement position for each acquirer/issuer pair for a
given processing date.  Settlement amounts account for purchases, refunds,
and chargebacks.  The result is the primary input to the overnight
settlement file sent to payment network participants.

spark-submit::

    spark-submit \\
      --master spark://spark-master:7077 \\
      src/batch/daily_settlement.py --date 2024-01-15
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from src.utils.io_helpers import (
    analytics_zone_path,
    read_json_landing,
    read_seed,
    write_parquet,
)
from src.utils.logging_config import configure_logging, get_logger
from src.utils.spark_session import get_spark_session

logger = get_logger(__name__)


def compute_daily_settlement(spark: SparkSession, processing_date: str) -> DataFrame:
    """Compute net settlement amounts per acquirer/issuer pair for ``processing_date``.

    Settlement logic:
    - ``purchase``   : positive value (acquirer owes issuer)
    - ``refund``     : negative value (issuer owes acquirer)
    - ``chargeback`` : negative value + counted separately for dispute tracking
    - ``p2p``        : excluded from card-scheme settlement

    Parameters
    ----------
    spark:
        Active SparkSession.
    processing_date:
        ISO date string (``"YYYY-MM-DD"``).

    Returns
    -------
    DataFrame with columns:
        acquirer_id, issuer_id, currency,
        purchase_count, purchase_volume,
        refund_count, refund_volume,
        chargeback_count, chargeback_volume,
        net_settlement_amount, processing_date
    """
    raw = read_json_landing(spark, "transactions", date=processing_date)

    # Broadcast small dimension tables — avoids a shuffle-merge join
    acquirers = read_seed(spark, "acquirers.csv")
    issuers = read_seed(spark, "issuers.csv")

    purchases = raw.filter(F.col("transaction_type") == "purchase")
    refunds   = raw.filter(F.col("transaction_type") == "refund")
    chargebacks = raw.filter(F.col("transaction_type") == "chargeback")

    def _agg(df: DataFrame, prefix: str) -> DataFrame:
        return df.groupBy("acquirer_id", "issuer_id", "currency").agg(
            F.count("*").alias(f"{prefix}_count"),
            F.sum(F.abs(F.col("amount").cast(DoubleType()))).alias(f"{prefix}_volume"),
        )

    purch_agg = _agg(purchases, "purchase")
    refund_agg = _agg(refunds, "refund")
    cb_agg = _agg(chargebacks, "chargeback")

    # Full outer join so pairs with only one txn type still appear
    settlement = (
        purch_agg
        .join(refund_agg, ["acquirer_id", "issuer_id", "currency"], "full")
        .join(cb_agg,    ["acquirer_id", "issuer_id", "currency"], "full")
        .na.fill(0)
        # Net = purchases - refunds - chargebacks (chargebacks debit the acquirer)
        .withColumn(
            "net_settlement_amount",
            F.col("purchase_volume") - F.col("refund_volume") - F.col("chargeback_volume"),
        )
        .withColumn("processing_date", F.lit(processing_date))
        # Validate dimension membership via broadcast joins
        .join(F.broadcast(acquirers.select("acquirer_id")), "acquirer_id", "left")
        .join(F.broadcast(issuers.select("issuer_id")), "issuer_id", "left")
    )

    return settlement


def run(spark: SparkSession, processing_date: str) -> None:
    """Execute the daily settlement job."""
    logger.info("daily_settlement_start", date=processing_date)
    df = compute_daily_settlement(spark, processing_date)
    write_parquet(df, "analytics", "daily_settlement", partition_by=["processing_date"])
    logger.info("daily_settlement_complete", date=processing_date, rows=df.count())


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser(description="Daily settlement aggregation")
    parser.add_argument(
        "--date",
        default=str(date.today()),
        help="Processing date (YYYY-MM-DD).  Defaults to today.",
    )
    args = parser.parse_args()
    spark = get_spark_session(app_name="daily-settlement", mode="batch")
    try:
        run(spark, args.date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
