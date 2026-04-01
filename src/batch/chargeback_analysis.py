"""
Chargeback ratio analysis batch job.

Merchants whose chargeback rate exceeds the network threshold (default 2%)
are flagged for review.  Persistently high-ratio merchants risk losing their
acceptance privileges, so timely detection is a key risk management function.

spark-submit::

    spark-submit \\
      --master spark://spark-master:7077 \\
      src/batch/chargeback_analysis.py --date 2024-01-15 --threshold 0.02
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


def compute_chargeback_ratios(spark: SparkSession, processing_date: str) -> DataFrame:
    """Compute per-merchant chargeback ratios for ``processing_date``.

    Returns a DataFrame with one row per merchant that had at least one
    transaction on the processing date, including:
      - Raw purchase and chargeback counts and volumes
      - Chargeback ratio (by count and by value)
      - A boolean ``is_flagged`` column (ratio > threshold)

    Parameters
    ----------
    spark:
        Active SparkSession.
    processing_date:
        ISO date string.
    """
    raw = read_json_landing(spark, "transactions", date=processing_date)
    merchants = read_seed(spark, "merchants.csv")
    threshold = float(os.getenv("CHARGEBACK_RATIO_THRESHOLD", "0.02"))

    purchases = (
        raw.filter(F.col("transaction_type") == "purchase")
        .groupBy("merchant_id")
        .agg(
            F.count("*").alias("purchase_count"),
            F.sum(F.abs(F.col("amount").cast(DoubleType()))).alias("purchase_volume"),
        )
    )

    chargebacks = (
        raw.filter(F.col("transaction_type") == "chargeback")
        .groupBy("merchant_id")
        .agg(
            F.count("*").alias("chargeback_count"),
            F.sum(F.abs(F.col("amount").cast(DoubleType()))).alias("chargeback_volume"),
        )
    )

    # Left join so merchants with zero chargebacks still appear
    ratio_df = (
        purchases
        .join(chargebacks, "merchant_id", "left")
        .na.fill({"chargeback_count": 0, "chargeback_volume": 0.0})
        # Count-based ratio: industry standard for Visa/MC compliance monitoring
        .withColumn(
            "chargeback_ratio_count",
            F.round(F.col("chargeback_count") / F.col("purchase_count"), 6),
        )
        # Value-based ratio: used as a secondary indicator
        .withColumn(
            "chargeback_ratio_value",
            F.round(
                F.col("chargeback_volume")
                / (F.col("purchase_volume") + F.col("chargeback_volume")),
                6,
            ),
        )
        .withColumn(
            "is_flagged",
            F.col("chargeback_ratio_count") > threshold,
        )
        .withColumn("threshold_applied", F.lit(threshold))
        .withColumn("processing_date", F.lit(processing_date))
    )

    # Enrich with merchant name via broadcast join
    enriched = ratio_df.join(
        F.broadcast(merchants.select("merchant_id", "merchant_name", "merchant_category_code")),
        "merchant_id",
        "left",
    )

    return enriched


def run(spark: SparkSession, processing_date: str, threshold: float) -> None:
    """Execute chargeback analysis job."""
    logger.info("chargeback_analysis_start", date=processing_date, threshold=threshold)

    df = compute_chargeback_ratios(spark, processing_date)
    flagged_count = df.filter(F.col("is_flagged")).count()

    write_parquet(
        df, "analytics", "chargeback_ratios",
        partition_by=["processing_date"],
    )

    logger.info(
        "chargeback_analysis_complete",
        date=processing_date,
        total_merchants=df.count(),
        flagged_merchants=flagged_count,
    )


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser(description="Chargeback ratio analysis")
    parser.add_argument("--date", default=str(date.today()))
    parser.add_argument("--threshold", type=float, default=0.02)
    args = parser.parse_args()
    spark = get_spark_session(app_name="chargeback-analysis", mode="batch")
    try:
        run(spark, args.date, args.threshold)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
