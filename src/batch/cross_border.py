"""
Cross-border transaction corridor analysis batch job.

A "corridor" is a (card_issuing_country, merchant_country) pair.  This job
measures volume and value flowing through each corridor, which drives:
  - FX conversion revenue reporting
  - Regulatory compliance reporting (country-level volume thresholds)
  - Network routing optimisation decisions

spark-submit::

    spark-submit \\
      --master spark://spark-master:7077 \\
      src/batch/cross_border.py --date 2024-01-15
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


def compute_corridor_volume(spark: SparkSession, processing_date: str) -> DataFrame:
    """Aggregate cross-border transaction volume by corridor for ``processing_date``.

    A corridor is identified by (issuer_country, merchant_country) and is only
    meaningful for transactions where ``is_cross_border = True``.

    We derive ``issuer_country`` from the issuer dimension table.  If the
    issuer is unknown we fall back to ``country_code`` (merchant country) as a
    conservative proxy rather than dropping the record.

    Parameters
    ----------
    spark:
        Active SparkSession.
    processing_date:
        ISO date string.

    Returns
    -------
    DataFrame with columns:
        issuer_country, merchant_country, corridor,
        txn_count, total_value_usd_equiv,
        avg_amount, processing_date
    """
    raw = read_json_landing(spark, "transactions", date=processing_date)
    issuers = read_seed(spark, "issuers.csv")
    countries = read_seed(spark, "country_codes.csv")

    # Only cross-border transactions are relevant for corridor analysis
    xborder = raw.filter(F.col("is_cross_border") == True)

    # Broadcast issuers (small dimension) — avoids shuffle on a large raw dataset
    issuer_enriched = xborder.join(
        F.broadcast(issuers.select("issuer_id", "country_code").withColumnRenamed(
            "country_code", "issuer_country"
        )),
        on="issuer_id",
        how="left",
    ).withColumn(
        "issuer_country",
        F.coalesce(F.col("issuer_country"), F.col("country_code")),
    )

    # Broadcast country names for readable output
    country_names = countries.select(
        F.col("country_code").alias("cc"),
        F.col("country_name"),
    )

    corridor_agg = (
        issuer_enriched
        .filter(F.col("transaction_type").isin("purchase", "p2p"))
        .groupBy(
            F.col("issuer_country"),
            F.col("country_code").alias("merchant_country"),
            F.col("currency"),
        )
        .agg(
            F.count("*").alias("txn_count"),
            F.sum(F.abs(F.col("amount").cast(DoubleType()))).alias("total_value"),
            F.avg(F.abs(F.col("amount").cast(DoubleType()))).alias("avg_amount"),
            F.countDistinct("card_hash").alias("unique_cards"),
            F.countDistinct("merchant_id").alias("unique_merchants"),
        )
        # Readable corridor label: "US→GB"
        .withColumn(
            "corridor",
            F.concat(F.col("issuer_country"), F.lit("→"), F.col("merchant_country")),
        )
        .withColumn("processing_date", F.lit(processing_date))
    )

    return corridor_agg


def run(spark: SparkSession, processing_date: str) -> None:
    """Execute cross-border corridor analysis."""
    logger.info("cross_border_analysis_start", date=processing_date)
    df = compute_corridor_volume(spark, processing_date)
    write_parquet(
        df, "analytics", "cross_border_corridors",
        partition_by=["processing_date"],
    )
    logger.info(
        "cross_border_analysis_complete",
        date=processing_date,
        corridor_rows=df.count(),
    )


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser(description="Cross-border corridor analysis")
    parser.add_argument("--date", default=str(date.today()))
    args = parser.parse_args()
    spark = get_spark_session(app_name="cross-border-analysis", mode="batch")
    try:
        run(spark, args.date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
