"""
Chargeback ratio analysis batch job.

Merchants whose chargeback rate exceeds the network threshold (default 2%)
are flagged for review.  Persistently high-ratio merchants risk losing their
acceptance privileges, so timely detection is a key risk management function.

spark-submit::

    spark-submit \\
      --master spark://spark-master:7077 \\
      src/batch/chargeback_analysis.py --date 2024-01-15 --threshold 0.02

SQL implementation
------------------
The original two separate groupBy steps (purchases, chargebacks) followed by
a left join and then a second broadcast join for merchant enrichment are
replaced by a single CTE query (``CHARGEBACK_RATIOS_SQL``).  The conditional
aggregation, ratio calculations, ``is_flagged`` flag, and merchant broadcast
join are all expressed inside the SQL.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import DataFrame, SparkSession

from src.batch.batch_queries_sql import CHARGEBACK_RATIOS_SQL
from src.utils.io_helpers import read_json_landing, read_seed, write_parquet
from src.utils.logging_config import configure_logging, get_logger
from src.utils.spark_session import get_spark_session

logger = get_logger(__name__)


def compute_chargeback_ratios(spark: SparkSession, processing_date: str) -> DataFrame:
    """Compute per-merchant chargeback ratios for ``processing_date``.

    Returns a DataFrame with one row per merchant that had at least one
    purchase on the processing date, including:
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

    raw.createOrReplaceTempView("_chargeback_txns")
    merchants.select(
        "merchant_id", "merchant_name", "merchant_category_code"
    ).createOrReplaceTempView("_chargeback_merchants")

    return spark.sql(
        CHARGEBACK_RATIOS_SQL.format(
            view_name="_chargeback_txns",
            merchants_view="_chargeback_merchants",
            threshold=threshold,
            processing_date=processing_date,
        )
    )


def run(spark: SparkSession, processing_date: str, threshold: float) -> None:
    """Execute chargeback analysis job."""
    logger.info("chargeback_analysis_start", date=processing_date, threshold=threshold)

    df = compute_chargeback_ratios(spark, processing_date)
    flagged_count = df.filter(df["is_flagged"]).count()

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
