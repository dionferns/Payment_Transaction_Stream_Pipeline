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

SQL implementation
------------------
The original three-way groupBy + full-outer-join approach is replaced by a
single conditional aggregation query (``SETTLEMENT_SQL``).  One GROUP BY pass
over the transactions computes purchase, refund, and chargeback subtotals
simultaneously; the final SELECT joins in the dimension views for acquirer and
issuer validation using SQL broadcast hints.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import DataFrame, SparkSession

from src.batch.batch_queries_sql import SETTLEMENT_SQL
from src.utils.io_helpers import (
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
    acquirers = read_seed(spark, "acquirers.csv")
    issuers = read_seed(spark, "issuers.csv")

    raw.createOrReplaceTempView("_settlement_txns")
    acquirers.createOrReplaceTempView("_settlement_acquirers")
    issuers.createOrReplaceTempView("_settlement_issuers")

    return spark.sql(
        SETTLEMENT_SQL.format(
            view_name="_settlement_txns",
            acquirers_view="_settlement_acquirers",
            issuers_view="_settlement_issuers",
            processing_date=processing_date,
        )
    )


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
