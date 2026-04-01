"""
Real-time fraud flagging rules for the payment stream pipeline.

Rules operate on micro-batches in ``foreachBatch`` mode rather than
stream-to-stream aggregations, giving us access to arbitrary Spark SQL
expressions and broadcast joins without the output-mode constraints of
append-only streaming aggregations.

Each rule function takes a batch DataFrame and returns a DataFrame of
flagged transactions enriched with ``fraud_flag`` and ``fraud_reason``
columns.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType, StringType


# ---------------------------------------------------------------------------
# Rule 1 — Velocity check
# ---------------------------------------------------------------------------

def flag_velocity(
    df: DataFrame,
    window_seconds: int = 60,
    max_txns: int = 5,
) -> DataFrame:
    """Flag cards that exceed ``max_txns`` transactions within ``window_seconds``.

    Implementation uses a range-frame window ordered by epoch seconds so that
    we can express a time-range window without a full Spark Structured Streaming
    watermark context (we're inside foreachBatch).

    Parameters
    ----------
    df:
        Micro-batch DataFrame.  Must contain ``card_hash``, ``timestamp``.
    window_seconds:
        Lookback window in seconds (default 60).
    max_txns:
        Maximum allowed transactions per card in the window (default 5).
    """
    df = df.withColumn("epoch", F.unix_timestamp("timestamp"))

    w = (
        Window.partitionBy("card_hash")
        .orderBy("epoch")
        .rangeBetween(-window_seconds, 0)
    )

    flagged = (
        df.withColumn("velocity_count", F.count("*").over(w))
        .withColumn(
            "fraud_velocity",
            F.col("velocity_count") > max_txns,
        )
    )
    return flagged


# ---------------------------------------------------------------------------
# Rule 2 — Amount anomaly
# ---------------------------------------------------------------------------

def flag_amount_anomaly(
    df: DataFrame,
    multiplier: float = 3.0,
    min_samples: int = 3,
) -> DataFrame:
    """Flag transactions where the amount is >``multiplier``x the card's
    rolling average for the same micro-batch window.

    A minimum sample count prevents false positives on cards with very few
    transactions (the rolling average would be unreliable).

    Parameters
    ----------
    df:
        Micro-batch DataFrame.  Must contain ``card_hash``, ``amount``,
        ``timestamp``.
    multiplier:
        Anomaly threshold multiplier (default 3.0).
    min_samples:
        Minimum prior transactions required to activate the rule (default 3).
    """
    w = Window.partitionBy("card_hash").orderBy("timestamp").rowsBetween(
        Window.unboundedPreceding, -1
    )

    flagged = (
        df.withColumn("amount_dbl", F.col("amount").cast(DoubleType()))
        .withColumn("rolling_avg", F.avg("amount_dbl").over(w))
        .withColumn("prior_sample_count", F.count("*").over(w))
        .withColumn(
            "fraud_amount_anomaly",
            (F.col("prior_sample_count") >= min_samples)
            & (F.col("amount_dbl") > F.col("rolling_avg") * multiplier),
        )
    )
    return flagged


# ---------------------------------------------------------------------------
# Rule 3 — Geographic impossibility
# ---------------------------------------------------------------------------

def flag_geo_impossibility(
    df: DataFrame,
    window_minutes: int = 30,
) -> DataFrame:
    """Flag cards that appear in two different countries within ``window_minutes``.

    Two transactions are "geographically impossible" if the same card hash
    appears in distinct countries within the time window — a strong signal of
    card cloning or account takeover.

    Parameters
    ----------
    df:
        Micro-batch DataFrame.  Must contain ``card_hash``, ``country_code``,
        ``timestamp``.
    window_minutes:
        Detection window in minutes (default 30).
    """
    window_seconds = window_minutes * 60

    df_with_epoch = df.withColumn("epoch", F.unix_timestamp("timestamp"))

    w = (
        Window.partitionBy("card_hash")
        .orderBy("epoch")
        .rangeBetween(-window_seconds, 0)
    )

    flagged = (
        df_with_epoch
        .withColumn(
            "distinct_countries_in_window",
            F.size(F.collect_set("country_code").over(w)),
        )
        .withColumn(
            "fraud_geo_impossibility",
            F.col("distinct_countries_in_window") > 1,
        )
    )
    return flagged


# ---------------------------------------------------------------------------
# Composite flag application
# ---------------------------------------------------------------------------

def apply_all_fraud_rules(
    df: DataFrame,
    velocity_window_seconds: int = 60,
    velocity_max_txns: int = 5,
    amount_multiplier: float = 3.0,
    amount_min_samples: int = 3,
    geo_window_minutes: int = 30,
) -> DataFrame:
    """Apply all fraud detection rules and consolidate results.

    Returns the original DataFrame with two additional columns:
      - ``is_fraud_flagged`` (BooleanType): True if any rule fired.
      - ``fraud_reasons`` (StringType): pipe-delimited list of triggered rule names.

    Parameters
    ----------
    df:
        Micro-batch DataFrame with the full transaction schema.
    """
    df = flag_velocity(df, velocity_window_seconds, velocity_max_txns)
    df = flag_amount_anomaly(df, amount_multiplier, amount_min_samples)
    df = flag_geo_impossibility(df, geo_window_minutes)

    # Consolidate into a single flag column
    df = df.withColumn(
        "fraud_reasons",
        F.concat_ws(
            "|",
            F.when(F.col("fraud_velocity"), F.lit("VELOCITY")).otherwise(F.lit("")),
            F.when(F.col("fraud_amount_anomaly"), F.lit("AMOUNT_ANOMALY")).otherwise(F.lit("")),
            F.when(F.col("fraud_geo_impossibility"), F.lit("GEO_IMPOSSIBILITY")).otherwise(F.lit("")),
        ).cast(StringType()),
    ).withColumn(
        "is_fraud_flagged",
        F.col("fraud_velocity") | F.col("fraud_amount_anomaly") | F.col("fraud_geo_impossibility"),
    )

    # Drop intermediate columns before returning
    return df.drop(
        "epoch",
        "velocity_count",
        "fraud_velocity",
        "amount_dbl",
        "rolling_avg",
        "prior_sample_count",
        "fraud_amount_anomaly",
        "distinct_countries_in_window",
        "fraud_geo_impossibility",
    )
