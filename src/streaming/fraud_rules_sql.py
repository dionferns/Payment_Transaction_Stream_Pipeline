"""
SQL query templates for the three real-time fraud detection rules.

Each template is a parameterised Spark SQL string.  Numeric thresholds are
injected via Python ``.format()`` before the query is sent to the Spark
Catalyst engine.  The templates are consumed by ``fraud_rules.py``.

Design notes
------------
* ``UNIX_TIMESTAMP(timestamp)`` is used inline in window ``ORDER BY`` /
  ``RANGE BETWEEN`` clauses so that no ``epoch`` helper column needs to be
  added to (or removed from) the output schema.
* The geo-impossibility rule uses a self-join CTE because ``COLLECT_SET``
  is not supported as a window-function analytic in Spark SQL string syntax;
  the self-join produces identical results to the DataFrame API
  ``F.collect_set().over(window)`` approach.
* All templates produce the same output columns as the original DataFrame API
  implementations so that ``apply_all_fraud_rules`` and downstream tests
  require no changes.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Rule 1 — Velocity check
# ---------------------------------------------------------------------------
# Uses a RANGE BETWEEN window ordered by UNIX epoch seconds to count how many
# times the same card appears within the lookback window.  If the count
# exceeds max_txns the transaction is flagged.
#
# Placeholders:
#   {view_name}      — name of the registered Spark temp view
#   {window_seconds} — lookback in seconds (int, e.g. 60)
#   {max_txns}       — maximum allowed transactions in the window (int, e.g. 5)

VELOCITY_SQL = """
WITH with_velocity AS (
    SELECT
        *,
        COUNT(*) OVER (
            PARTITION BY card_hash
            ORDER BY     UNIX_TIMESTAMP(timestamp)
            RANGE BETWEEN {window_seconds} PRECEDING AND CURRENT ROW
        ) AS velocity_count
    FROM {view_name}
)
SELECT
    *,
    velocity_count > {max_txns} AS fraud_velocity
FROM with_velocity
"""

# ---------------------------------------------------------------------------
# Rule 2 — Amount anomaly
# ---------------------------------------------------------------------------
# Computes a per-card rolling average of all *prior* transactions (unbounded
# preceding, exclusive of the current row) and flags the current row when its
# amount exceeds multiplier × rolling_avg and the card has at least
# min_samples prior transactions (guards against unreliable averages on new
# cards).
#
# Placeholders:
#   {view_name}    — name of the registered Spark temp view
#   {multiplier}   — anomaly threshold multiplier (float, e.g. 3.0)
#   {min_samples}  — minimum prior transactions required (int, e.g. 3)

AMOUNT_ANOMALY_SQL = """
WITH with_amount_dbl AS (
    SELECT
        *,
        CAST(amount AS DOUBLE) AS amount_dbl
    FROM {view_name}
),
with_rolling AS (
    SELECT
        *,
        AVG(amount_dbl) OVER (
            PARTITION BY card_hash
            ORDER BY     timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS rolling_avg,
        COUNT(*) OVER (
            PARTITION BY card_hash
            ORDER BY     timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prior_sample_count
    FROM with_amount_dbl
)
SELECT
    *,
    (
        prior_sample_count >= {min_samples}
        AND amount_dbl > rolling_avg * {multiplier}
    ) AS fraud_amount_anomaly
FROM with_rolling
"""

# ---------------------------------------------------------------------------
# Rule 3 — Geographic impossibility
# ---------------------------------------------------------------------------
# Finds cards that appear in two or more distinct countries within
# window_seconds.  A self-join on the same temp view identifies card_hash
# values where at least one other transaction in the time window used a
# different country_code; all transactions on those cards are then flagged.
#
# Note: COLLECT_SET is not supported as a window-function analytic in Spark
# SQL string syntax, so we use a self-join CTE instead.  The self-join is
# evaluated inside a single micro-batch so the dataset is small.
#
# Placeholders:
#   {view_name}      — name of the registered Spark temp view
#   {window_seconds} — detection window in seconds (int, e.g. 1800 for 30 min)

GEO_IMPOSSIBILITY_SQL = """
WITH multi_country_cards AS (
    SELECT DISTINCT a.card_hash
    FROM   {view_name} a
    JOIN   {view_name} b
      ON   a.card_hash      = b.card_hash
      AND  a.transaction_id != b.transaction_id
      AND  UNIX_TIMESTAMP(b.timestamp) >= UNIX_TIMESTAMP(a.timestamp) - {window_seconds}
      AND  UNIX_TIMESTAMP(b.timestamp) <= UNIX_TIMESTAMP(a.timestamp)
      AND  a.country_code   != b.country_code
)
SELECT
    t.*,
    (m.card_hash IS NOT NULL) AS fraud_geo_impossibility
FROM   {view_name} t
LEFT   JOIN multi_country_cards m
  ON   t.card_hash = m.card_hash
"""

# ---------------------------------------------------------------------------
# Consolidation — combines the three rule flags into summary columns
# ---------------------------------------------------------------------------
# Applied after all three rules have added their intermediate columns.
# Produces fraud_reasons (pipe-delimited label string) and is_fraud_flagged
# (boolean OR of all three flags), then drops the per-rule intermediate
# columns so the output schema is clean.
#
# Placeholders: none — column names are fixed by the rule functions above.

CONSOLIDATION_SQL = """
SELECT
    *,
    CONCAT_WS(
        '|',
        CASE WHEN fraud_velocity          THEN 'VELOCITY'          END,
        CASE WHEN fraud_amount_anomaly    THEN 'AMOUNT_ANOMALY'    END,
        CASE WHEN fraud_geo_impossibility THEN 'GEO_IMPOSSIBILITY' END
    ) AS fraud_reasons,
    (
        fraud_velocity
        OR fraud_amount_anomaly
        OR fraud_geo_impossibility
    ) AS is_fraud_flagged
FROM {view_name}
"""
