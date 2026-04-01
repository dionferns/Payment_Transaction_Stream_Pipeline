"""
Merchant category (MCC) spending analysis batch job.

Produces per-merchant and per-category aggregations that power the
analytics team's merchant performance dashboards.

spark-submit::

    spark-submit \\
      --master spark://spark-master:7077 \\
      src/batch/merchant_analytics.py --date 2024-01-15
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

from src.utils.io_helpers import read_json_landing, read_seed, write_parquet
from src.utils.logging_config import configure_logging, get_logger
from src.utils.spark_session import get_spark_session

logger = get_logger(__name__)


def compute_mcc_spending(spark: SparkSession, processing_date: str) -> DataFrame:
    """Aggregate spend by merchant category code for ``processing_date``.

    Returns one row per (merchant_category_code, currency) with spend
    metrics and approval-rate statistics.

    Parameters
    ----------
    spark:
        Active SparkSession.
    processing_date:
        ISO date string.
    """
    raw = read_json_landing(spark, "transactions", date=processing_date)
    merchants = read_seed(spark, "merchants.csv")

    # Broadcast join — merchants.csv is small (< 5 k rows)
    enriched = raw.join(
        F.broadcast(merchants.select("merchant_id", "merchant_category_code", "merchant_name")),
        on="merchant_id",
        how="left",
    )

    # Use coalesce to handle records that didn't match the dimension table
    enriched = enriched.withColumn(
        "mcc",
        F.coalesce(
            F.col("merchants.merchant_category_code"),
            F.col("transactions.merchant_category_code"),
        )
        if "merchants.merchant_category_code" in enriched.columns
        else F.col("merchant_category_code"),
    )

    mcc_agg = (
        raw.filter(F.col("transaction_type").isin("purchase", "p2p"))
        .groupBy("merchant_category_code", "currency")
        .agg(
            F.count("*").alias("txn_count"),
            F.sum(F.abs(F.col("amount").cast(DoubleType()))).alias("total_spend"),
            F.avg(F.abs(F.col("amount").cast(DoubleType()))).alias("avg_txn_amount"),
            F.max(F.abs(F.col("amount").cast(DoubleType()))).alias("max_txn_amount"),
            F.countDistinct("merchant_id").alias("active_merchant_count"),
            F.countDistinct("card_hash").alias("unique_cards"),
            # Approval rate: approved (response_code == '00') / total
            F.sum(
                F.when(F.col("response_code") == "00", 1).otherwise(0)
            ).alias("approved_count"),
        )
        .withColumn(
            "approval_rate",
            F.round(F.col("approved_count") / F.col("txn_count"), 4),
        )
        .withColumn("processing_date", F.lit(processing_date))
        .drop("approved_count")
    )

    return mcc_agg


def compute_merchant_summary(spark: SparkSession, processing_date: str) -> DataFrame:
    """Per-merchant daily summary — volume, approval rate, average ticket.

    Used by risk teams to monitor merchant health and detect sudden changes
    in transaction patterns (possible terminal compromise).
    """
    raw = read_json_landing(spark, "transactions", date=processing_date)

    return (
        raw.groupBy("merchant_id")
        .agg(
            F.count("*").alias("total_txns"),
            F.sum(
                F.when(F.col("transaction_type") == "purchase", 1).otherwise(0)
            ).alias("purchase_count"),
            F.sum(
                F.when(F.col("transaction_type") == "refund", 1).otherwise(0)
            ).alias("refund_count"),
            F.sum(
                F.when(F.col("transaction_type") == "chargeback", 1).otherwise(0)
            ).alias("chargeback_count"),
            F.sum(
                F.when(
                    F.col("transaction_type") == "purchase",
                    F.abs(F.col("amount").cast(DoubleType())),
                ).otherwise(0.0)
            ).alias("purchase_volume"),
            F.sum(
                F.when(F.col("response_code") == "00", 1).otherwise(0)
            ).alias("approved_count"),
            F.countDistinct("card_hash").alias("unique_cards"),
            F.first("merchant_category_code").alias("merchant_category_code"),
        )
        .withColumn(
            "approval_rate",
            F.round(F.col("approved_count") / F.col("total_txns"), 4),
        )
        .withColumn("processing_date", F.lit(processing_date))
        .drop("approved_count")
    )


def run(spark: SparkSession, processing_date: str) -> None:
    """Execute merchant analytics jobs."""
    logger.info("merchant_analytics_start", date=processing_date)

    mcc_df = compute_mcc_spending(spark, processing_date)
    write_parquet(
        mcc_df, "analytics", "mcc_spending",
        partition_by=["processing_date"],
    )

    merchant_df = compute_merchant_summary(spark, processing_date)
    write_parquet(
        merchant_df, "analytics", "merchant_summary",
        partition_by=["processing_date"],
    )

    logger.info(
        "merchant_analytics_complete",
        date=processing_date,
        mcc_rows=mcc_df.count(),
        merchant_rows=merchant_df.count(),
    )


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser(description="Merchant category spending analysis")
    parser.add_argument("--date", default=str(date.today()))
    args = parser.parse_args()
    spark = get_spark_session(app_name="merchant-analytics", mode="batch")
    try:
        run(spark, args.date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
