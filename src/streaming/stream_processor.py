"""
PySpark Structured Streaming job for real-time payment transaction processing.

Reads new transaction JSON files from the landing zone (or Kafka) as they
arrive, applies fraud detection rules in ``foreachBatch`` mode, computes
windowed aggregations, and writes results to:
  - ``analytics/fraud_flags``      — flagged transactions (append)
  - ``analytics/merchant_volume``  — rolling 5-min merchant aggregations
  - ``analytics/issuer_xborder``   — rolling 1-hr cross-border issuer metrics

spark-submit::

    spark-submit \\
      --master spark://spark-master:7077 \\
      --packages io.delta:delta-spark_2.12:3.1.0 \\
      src/streaming/stream_processor.py
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Allow running as spark-submit without installed package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from src.streaming.aggregations import (
    card_daily_running_total,
    issuer_cross_border_ratio_1h,
    merchant_volume_5min,
)
from src.streaming.fraud_rules import apply_all_fraud_rules
from src.utils.io_helpers import analytics_zone_path, landing_zone_path
from src.utils.logging_config import configure_logging, get_logger
from src.utils.spark_session import get_spark_session

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",         StringType(),      False),
    StructField("card_hash",              StringType(),      False),
    StructField("merchant_id",            StringType(),      False),
    StructField("merchant_category_code", StringType(),      True),
    StructField("amount",                 DecimalType(18, 2), False),
    StructField("currency",               StringType(),      True),
    StructField("timestamp",              TimestampType(),   False),
    StructField("transaction_type",       StringType(),      False),
    StructField("country_code",           StringType(),      True),
    StructField("acquirer_id",            StringType(),      True),
    StructField("issuer_id",              StringType(),      True),
    StructField("response_code",          StringType(),      True),
    StructField("is_cross_border",        BooleanType(),     True),
    StructField("merchant_name",          StringType(),      True),
    StructField("terminal_id",            StringType(),      True),
    StructField("auth_code",              StringType(),      True),
])


# ---------------------------------------------------------------------------
# Micro-batch handler
# ---------------------------------------------------------------------------

def process_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Process one micro-batch: apply fraud rules and write flagged records.

    This function is registered with ``foreachBatch`` so that we can apply
    arbitrary Spark operations (including non-streaming-compatible ones like
    window functions) on each micro-batch DataFrame.

    Parameters
    ----------
    batch_df:
        The micro-batch DataFrame — behaves like a regular static DataFrame.
    batch_id:
        Monotonically increasing batch identifier assigned by Spark.
    """
    if batch_df.isEmpty():
        logger.info("empty_micro_batch", batch_id=batch_id)
        return

    record_count = batch_df.count()
    logger.info("processing_micro_batch", batch_id=batch_id, records=record_count)

    fraud_window_secs = int(os.getenv("FRAUD_VELOCITY_WINDOW_SECONDS", "60"))
    fraud_max_txns = int(os.getenv("FRAUD_VELOCITY_MAX_TXN", "5"))
    fraud_multiplier = float(os.getenv("FRAUD_AMOUNT_MULTIPLIER", "3.0"))
    geo_window = int(os.getenv("FRAUD_GEO_WINDOW_MINUTES", "30"))

    flagged_df = apply_all_fraud_rules(
        batch_df,
        velocity_window_seconds=fraud_window_secs,
        velocity_max_txns=fraud_max_txns,
        amount_multiplier=fraud_multiplier,
        geo_window_minutes=geo_window,
    )

    fraud_path = f"{analytics_zone_path()}/fraud_flags"
    (
        flagged_df.filter(F.col("is_fraud_flagged"))
        .write.format("parquet")
        .mode("append")
        .partitionBy("transaction_type")
        .save(fraud_path)
    )

    flagged_count = flagged_df.filter(F.col("is_fraud_flagged")).count()
    logger.info(
        "micro_batch_complete",
        batch_id=batch_id,
        total_records=record_count,
        fraud_flagged=flagged_count,
    )


# ---------------------------------------------------------------------------
# Streaming queries
# ---------------------------------------------------------------------------

def start_fraud_stream(spark: SparkSession, source_df: DataFrame) -> object:
    """Start the fraud-detection foreachBatch stream.

    Returns the StreamingQuery handle so the caller can manage its lifecycle.
    """
    checkpoint = f"{os.getenv('LANDING_ZONE_PATH', './data')}/checkpoints/fraud"
    return (
        source_df.writeStream
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=f"{os.getenv('STREAMING_TRIGGER_SECONDS', '30')} seconds")
        .start()
    )


def start_merchant_volume_stream(spark: SparkSession, source_df: DataFrame) -> object:
    """Start the rolling 5-minute merchant volume aggregation stream."""
    out_path = f"{analytics_zone_path()}/merchant_volume"
    checkpoint = f"{os.getenv('LANDING_ZONE_PATH', './data')}/checkpoints/merchant_volume"
    return (
        merchant_volume_5min(source_df)
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", out_path)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="5 minutes")
        .start()
    )


def start_issuer_xborder_stream(spark: SparkSession, source_df: DataFrame) -> object:
    """Start the rolling 1-hour cross-border ratio aggregation stream."""
    out_path = f"{analytics_zone_path()}/issuer_xborder"
    checkpoint = f"{os.getenv('LANDING_ZONE_PATH', './data')}/checkpoints/issuer_xborder"
    return (
        issuer_cross_border_ratio_1h(source_df)
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", out_path)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="15 minutes")
        .start()
    )


def start_card_daily_stream(spark: SparkSession, source_df: DataFrame) -> object:
    """Start the running daily-spend-per-card stream."""
    out_path = f"{analytics_zone_path()}/card_daily_totals"
    checkpoint = f"{os.getenv('LANDING_ZONE_PATH', './data')}/checkpoints/card_daily"
    return (
        card_daily_running_total(source_df)
        .writeStream
        .outputMode("complete")
        .format("parquet")
        .option("path", out_path)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="10 minutes")
        .start()
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Launch all streaming queries and block until termination."""
    configure_logging()
    spark = get_spark_session(mode="streaming")

    landing = landing_zone_path()
    source_path = f"{landing}/transactions"
    use_kafka = bool(os.getenv("KAFKA_BOOTSTRAP_SERVERS"))

    logger.info(
        "stream_processor_start",
        source="kafka" if use_kafka else "file",
        path=source_path if not use_kafka else None,
    )

    if use_kafka:
        raw_stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
            .option("subscribe", os.getenv("KAFKA_TOPIC_TRANSACTIONS", "payment.transactions.raw"))
            .option("startingOffsets", "latest")
            .load()
            .select(
                F.from_json(F.col("value").cast("string"), TRANSACTION_SCHEMA).alias("data")
            )
            .select("data.*")
        )
    else:
        raw_stream = (
            spark.readStream.schema(TRANSACTION_SCHEMA)
            .option("maxFilesPerTrigger", 10)
            .json(source_path)
        )

    queries = [
        start_fraud_stream(spark, raw_stream),
        start_merchant_volume_stream(spark, raw_stream),
        start_issuer_xborder_stream(spark, raw_stream),
        start_card_daily_stream(spark, raw_stream),
    ]

    logger.info("streaming_queries_started", query_count=len(queries))

    # Block until all queries terminate or the process is interrupted
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
