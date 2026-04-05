"""
SQL query templates for the three streaming aggregation functions.

Each template is a parameterised Spark SQL string consumed by
``aggregations.py``.  Watermarks are applied to the DataFrame before the
view is registered so that Spark's late-data eviction still works correctly —
the SQL templates themselves express only the grouping and aggregation logic.

Design notes
------------
* ``window(timestamp, '5 minutes')`` is Spark SQL's built-in tumbling-window
  function; ``window(timestamp, '1 hour', '15 minutes')`` produces a sliding
  (hop) window.  Both return a struct with ``.start`` and ``.end`` fields.
* ``COUNT(DISTINCT ...)`` is supported in Spark SQL groupBy aggregations but
  NOT in window-function analytics — used here in a GROUP BY context so it
  works correctly.
* The daily-running-total query uses ``DATE(timestamp)`` rather than a window
  struct; the result matches the ``(card_hash, date)`` group key from the
  original DataFrame API implementation.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Aggregation 1 — Merchant 5-minute tumbling window
# ---------------------------------------------------------------------------
# Placeholders:
#   {view_name} — name of the registered Spark temp view (watermark already
#                 applied to the DataFrame before view creation)

MERCHANT_VOLUME_SQL = """
SELECT
    window(timestamp, '5 minutes').start  AS window_start,
    window(timestamp, '5 minutes').end    AS window_end,
    merchant_id,
    COUNT(*)                              AS txn_count,
    SUM(ABS(CAST(amount AS DOUBLE)))      AS total_value,
    COUNT(DISTINCT card_hash)             AS unique_cards
FROM {view_name}
WHERE transaction_type IN ('purchase', 'p2p')
GROUP BY
    window(timestamp, '5 minutes'),
    merchant_id
"""

# ---------------------------------------------------------------------------
# Aggregation 2 — Issuer cross-border ratio, 1-hour sliding window
# ---------------------------------------------------------------------------
# Sliding window: 1-hour duration, 15-minute slide (hop).
# Placeholders:
#   {view_name} — name of the registered Spark temp view

ISSUER_XBORDER_SQL = """
SELECT
    window(timestamp, '1 hour', '15 minutes').start  AS window_start,
    window(timestamp, '1 hour', '15 minutes').end    AS window_end,
    issuer_id,
    COUNT(*)                                         AS total_txns,
    SUM(CAST(is_cross_border AS INT))                AS cross_border_txns,
    ROUND(
        SUM(CAST(is_cross_border AS INT)) / COUNT(*),
        4
    )                                                AS cross_border_ratio
FROM {view_name}
GROUP BY
    window(timestamp, '1 hour', '15 minutes'),
    issuer_id
"""

# ---------------------------------------------------------------------------
# Aggregation 3 — Card daily running total
# ---------------------------------------------------------------------------
# Groups by card_hash and calendar date so downstream queries can detect
# cards exceeding daily spend limits.
# Placeholders:
#   {view_name} — name of the registered Spark temp view

CARD_DAILY_SQL = """
SELECT
    card_hash,
    DATE(timestamp)                  AS date,
    COUNT(*)                         AS daily_txn_count,
    SUM(ABS(CAST(amount AS DOUBLE))) AS daily_spend,
    MAX(timestamp)                   AS last_seen
FROM {view_name}
WHERE transaction_type IN ('purchase', 'p2p')
GROUP BY
    card_hash,
    DATE(timestamp)
"""
