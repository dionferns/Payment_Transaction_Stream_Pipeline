"""
Merchant category (MCC) spending analysis batch job.

Produces per-merchant and per-category aggregations that power the
analytics team's merchant performance dashboards.

spark-submit::

    spark-submit \\
      --master spark://spark-master:7077 \\
      src/batch/merchant_analytics.py --date 2024-01-15

SQL implementation
------------------
Both aggregations are now expressed as Spark SQL queries (``MCC_SPENDING_SQL``
and ``MERCHANT_SUMMARY_SQL``).  Multiple ``.when()`` conditional expressions
are replaced with SQL ``CASE WHEN`` clauses, and the approval-rate computation
is inlined directly in the SELECT rather than added as a subsequent
``.withColumn()`` step.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import DataFrame, SparkSession

from src.batch.batch_queries_sql import MCC_SPENDING_SQL, MERCHANT_SUMMARY_SQL
from src.utils.io_helpers import read_json_landing, write_parquet
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
    raw.createOrReplaceTempView("_mcc_spending_txns")
    return spark.sql(
        MCC_SPENDING_SQL.format(
            view_name="_mcc_spending_txns",
            processing_date=processing_date,
        )
    )


def compute_merchant_summary(spark: SparkSession, processing_date: str) -> DataFrame:
    """Per-merchant daily summary — volume, approval rate, average ticket.

    Used by risk teams to monitor merchant health and detect sudden changes
    in transaction patterns (possible terminal compromise).
    """
    raw = read_json_landing(spark, "transactions", date=processing_date)
    raw.createOrReplaceTempView("_merchant_summary_txns")
    return spark.sql(
        MERCHANT_SUMMARY_SQL.format(
            view_name="_merchant_summary_txns",
            processing_date=processing_date,
        )
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
