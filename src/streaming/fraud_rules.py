"""
Real-time fraud flagging rules for the payment stream pipeline.

Rules operate on micro-batches in ``foreachBatch`` mode rather than
stream-to-stream aggregations, giving us access to arbitrary Spark SQL
expressions and broadcast joins without the output-mode constraints of
append-only streaming aggregations.

Each rule function takes a batch DataFrame and returns a DataFrame of
flagged transactions enriched with intermediate flag columns.
``apply_all_fraud_rules`` chains all three rules and consolidates the
results into the final ``is_fraud_flagged`` and ``fraud_reasons`` columns.

SQL implementation
------------------
All three detection rules and the final consolidation are now expressed as
Spark SQL queries (see ``fraud_rules_sql.py``).  Each function registers
its input DataFrame as a temporary view, executes the SQL template, and
returns the result DataFrame — preserving the original function signatures
so that callers and tests require no changes.
"""

from __future__ import annotations

from pyspark.sql import DataFrame

from src.streaming.fraud_rules_sql import (
    AMOUNT_ANOMALY_SQL,
    CONSOLIDATION_SQL,
    GEO_IMPOSSIBILITY_SQL,
    VELOCITY_SQL,
)


# ---------------------------------------------------------------------------
# Rule 1 — Velocity check
# ---------------------------------------------------------------------------

def flag_velocity(
    df: DataFrame,
    window_seconds: int = 60,
    max_txns: int = 5,
) -> DataFrame:
    """Flag cards that exceed ``max_txns`` transactions within ``window_seconds``.

    Uses a Spark SQL RANGE BETWEEN window ordered by UNIX epoch seconds to
    count transactions per card in the lookback window.  Adds two columns to
    the output:

    * ``velocity_count`` (long)  — number of transactions for this card in
      the window.
    * ``fraud_velocity`` (boolean) — True when velocity_count > max_txns.

    Parameters
    ----------
    df:
        Micro-batch DataFrame.  Must contain ``card_hash``, ``timestamp``.
    window_seconds:
        Lookback window in seconds (default 60).
    max_txns:
        Maximum allowed transactions per card in the window (default 5).
    """
    df.createOrReplaceTempView("_velocity_input")
    return df.sparkSession.sql(
        VELOCITY_SQL.format(
            view_name="_velocity_input",
            window_seconds=window_seconds,
            max_txns=max_txns,
        )
    )


# ---------------------------------------------------------------------------
# Rule 2 — Amount anomaly
# ---------------------------------------------------------------------------

def flag_amount_anomaly(
    df: DataFrame,
    multiplier: float = 3.0,
    min_samples: int = 3,
) -> DataFrame:
    """Flag transactions where the amount is >``multiplier``x the card's
    rolling average for prior transactions in this micro-batch.

    A minimum sample count prevents false positives on cards with very few
    transactions (the rolling average would be unreliable).  Adds three
    columns:

    * ``amount_dbl``          (double)  — amount cast to double for arithmetic.
    * ``rolling_avg``         (double)  — per-card average of all prior rows.
    * ``prior_sample_count``  (long)    — number of prior rows for this card.
    * ``fraud_amount_anomaly`` (boolean) — True when the anomaly condition fires.

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
    df.createOrReplaceTempView("_amount_anomaly_input")
    return df.sparkSession.sql(
        AMOUNT_ANOMALY_SQL.format(
            view_name="_amount_anomaly_input",
            multiplier=multiplier,
            min_samples=min_samples,
        )
    )


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
    card cloning or account takeover.  Adds one column:

    * ``fraud_geo_impossibility`` (boolean) — True when the card was seen in
      more than one country within the detection window.

    Parameters
    ----------
    df:
        Micro-batch DataFrame.  Must contain ``card_hash``, ``country_code``,
        ``timestamp``.
    window_minutes:
        Detection window in minutes (default 30).
    """
    window_seconds = window_minutes * 60
    df.createOrReplaceTempView("_geo_input")
    return df.sparkSession.sql(
        GEO_IMPOSSIBILITY_SQL.format(
            view_name="_geo_input",
            window_seconds=window_seconds,
        )
    )


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

    Chains ``flag_velocity``, ``flag_amount_anomaly``, and
    ``flag_geo_impossibility``, then uses Spark SQL to produce two final
    summary columns and drops all intermediate working columns.

    Returns the original DataFrame with two additional columns:
      - ``is_fraud_flagged`` (BooleanType): True if any rule fired.
      - ``fraud_reasons`` (StringType): pipe-delimited list of triggered
        rule names (e.g. ``"VELOCITY|GEO_IMPOSSIBILITY"``).

    Parameters
    ----------
    df:
        Micro-batch DataFrame with the full transaction schema.
    """
    df = flag_velocity(df, velocity_window_seconds, velocity_max_txns)
    df = flag_amount_anomaly(df, amount_multiplier, amount_min_samples)
    df = flag_geo_impossibility(df, geo_window_minutes)

    # Consolidate the three boolean flags into summary columns via SQL
    df.createOrReplaceTempView("_consolidation_input")
    consolidated = df.sparkSession.sql(
        CONSOLIDATION_SQL.format(view_name="_consolidation_input")
    )

    # Drop all intermediate working columns added by the three rules
    return consolidated.drop(
        "velocity_count",
        "fraud_velocity",
        "amount_dbl",
        "rolling_avg",
        "prior_sample_count",
        "fraud_amount_anomaly",
        "fraud_geo_impossibility",
    )
