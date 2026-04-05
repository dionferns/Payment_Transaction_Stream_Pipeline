"""
SQL query templates for the four batch processing jobs.

Each template is a parameterised Spark SQL string consumed by the
corresponding batch job module.  Dimension tables (acquirers, issuers,
merchants) are registered as temporary views by the calling Python code so
they can be referenced inside the SQL.  Broadcast hints are expressed via
Spark SQL's ``/*+ BROADCAST(...) */`` syntax so the optimiser avoids
shuffle-merge joins on small dimension tables.

Design notes
------------
* ``SETTLEMENT_SQL`` replaces three separate groupBy + full-outer-join chains
  with a single conditional aggregation pass — one GROUP BY computes
  purchase, refund, and chargeback subtotals simultaneously.
* ``MCC_SPENDING_SQL`` and ``MERCHANT_SUMMARY_SQL`` consolidate multiple
  ``.when()`` expressions into SQL ``CASE WHEN`` clauses for readability.
* ``CORRIDOR_VOLUME_SQL`` expresses the issuer broadcast join and the
  ``COALESCE`` fallback for unknown issuers in SQL using a broadcast hint.
* ``CHARGEBACK_RATIOS_SQL`` expresses both the conditional aggregation and the
  merchant broadcast join in a single query, replacing the two-step Python
  groupBy + join pattern.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Batch job 1 — Daily settlement
# ---------------------------------------------------------------------------
# Registered views required:
#   {view_name}      — raw transactions for the processing date
#   {acquirers_view} — acquirer dimension table (acquirer_id)
#   {issuers_view}   — issuer dimension table (issuer_id)
#
# Placeholders:
#   {view_name}        — transactions temp view name
#   {acquirers_view}   — acquirers temp view name
#   {issuers_view}     — issuers temp view name
#   {processing_date}  — ISO date string (e.g. '2024-01-15')

SETTLEMENT_SQL = """
WITH settlement_base AS (
    SELECT
        acquirer_id,
        issuer_id,
        currency,
        SUM(CASE WHEN transaction_type = 'purchase'
                 THEN 1 ELSE 0 END)                                    AS purchase_count,
        SUM(CASE WHEN transaction_type = 'purchase'
                 THEN ABS(CAST(amount AS DOUBLE)) ELSE 0.0 END)        AS purchase_volume,
        SUM(CASE WHEN transaction_type = 'refund'
                 THEN 1 ELSE 0 END)                                    AS refund_count,
        SUM(CASE WHEN transaction_type = 'refund'
                 THEN ABS(CAST(amount AS DOUBLE)) ELSE 0.0 END)        AS refund_volume,
        SUM(CASE WHEN transaction_type = 'chargeback'
                 THEN 1 ELSE 0 END)                                    AS chargeback_count,
        SUM(CASE WHEN transaction_type = 'chargeback'
                 THEN ABS(CAST(amount AS DOUBLE)) ELSE 0.0 END)        AS chargeback_volume
    FROM {view_name}
    WHERE transaction_type IN ('purchase', 'refund', 'chargeback')
    GROUP BY acquirer_id, issuer_id, currency
)
SELECT /*+ BROADCAST({acquirers_view}, {issuers_view}) */
    s.acquirer_id,
    s.issuer_id,
    s.currency,
    s.purchase_count,
    s.purchase_volume,
    s.refund_count,
    s.refund_volume,
    s.chargeback_count,
    s.chargeback_volume,
    s.purchase_volume - s.refund_volume - s.chargeback_volume AS net_settlement_amount,
    '{processing_date}'                                        AS processing_date
FROM       settlement_base s
LEFT JOIN  {acquirers_view} a ON s.acquirer_id = a.acquirer_id
LEFT JOIN  {issuers_view}   i ON s.issuer_id   = i.issuer_id
"""

# ---------------------------------------------------------------------------
# Batch job 2a — MCC spending summary
# ---------------------------------------------------------------------------
# Registered views required:
#   {view_name} — raw transactions for the processing date
#
# Placeholders:
#   {view_name}       — transactions temp view name
#   {processing_date} — ISO date string

MCC_SPENDING_SQL = """
SELECT
    merchant_category_code,
    currency,
    COUNT(*)                                                        AS txn_count,
    SUM(ABS(CAST(amount AS DOUBLE)))                               AS total_spend,
    AVG(ABS(CAST(amount AS DOUBLE)))                               AS avg_txn_amount,
    MAX(ABS(CAST(amount AS DOUBLE)))                               AS max_txn_amount,
    COUNT(DISTINCT merchant_id)                                    AS active_merchant_count,
    COUNT(DISTINCT card_hash)                                      AS unique_cards,
    ROUND(
        SUM(CASE WHEN response_code = '00' THEN 1 ELSE 0 END)
        / COUNT(*),
        4
    )                                                              AS approval_rate,
    '{processing_date}'                                            AS processing_date
FROM {view_name}
WHERE transaction_type IN ('purchase', 'p2p')
GROUP BY merchant_category_code, currency
"""

# ---------------------------------------------------------------------------
# Batch job 2b — Merchant daily summary
# ---------------------------------------------------------------------------
# Registered views required:
#   {view_name} — raw transactions for the processing date
#
# Placeholders:
#   {view_name}       — transactions temp view name
#   {processing_date} — ISO date string

MERCHANT_SUMMARY_SQL = """
SELECT
    merchant_id,
    COUNT(*)                                                        AS total_txns,
    SUM(CASE WHEN transaction_type = 'purchase'
             THEN 1 ELSE 0 END)                                    AS purchase_count,
    SUM(CASE WHEN transaction_type = 'refund'
             THEN 1 ELSE 0 END)                                    AS refund_count,
    SUM(CASE WHEN transaction_type = 'chargeback'
             THEN 1 ELSE 0 END)                                    AS chargeback_count,
    SUM(CASE WHEN transaction_type = 'purchase'
             THEN ABS(CAST(amount AS DOUBLE)) ELSE 0.0 END)        AS purchase_volume,
    ROUND(
        SUM(CASE WHEN response_code = '00' THEN 1 ELSE 0 END)
        / COUNT(*),
        4
    )                                                              AS approval_rate,
    COUNT(DISTINCT card_hash)                                      AS unique_cards,
    FIRST(merchant_category_code)                                  AS merchant_category_code,
    '{processing_date}'                                            AS processing_date
FROM {view_name}
GROUP BY merchant_id
"""

# ---------------------------------------------------------------------------
# Batch job 3 — Cross-border corridor volume
# ---------------------------------------------------------------------------
# Registered views required:
#   {view_name}    — raw transactions (already filtered for is_cross_border)
#   {issuers_view} — issuer dimension table (issuer_id, country_code)
#
# Placeholders:
#   {view_name}       — transactions temp view name
#   {issuers_view}    — issuers temp view name
#   {processing_date} — ISO date string

CORRIDOR_VOLUME_SQL = """
WITH issuer_enriched AS (
    SELECT /*+ BROADCAST({issuers_view}) */
        t.*,
        COALESCE(i.country_code, t.country_code) AS issuer_country
    FROM {view_name} t
    LEFT JOIN {issuers_view} i
      ON t.issuer_id = i.issuer_id
)
SELECT
    issuer_country,
    country_code                                              AS merchant_country,
    currency,
    CONCAT(issuer_country, '\u2192', country_code)           AS corridor,
    COUNT(*)                                                  AS txn_count,
    SUM(ABS(CAST(amount AS DOUBLE)))                         AS total_value,
    AVG(ABS(CAST(amount AS DOUBLE)))                         AS avg_amount,
    COUNT(DISTINCT card_hash)                                AS unique_cards,
    COUNT(DISTINCT merchant_id)                              AS unique_merchants,
    '{processing_date}'                                      AS processing_date
FROM issuer_enriched
WHERE transaction_type IN ('purchase', 'p2p')
GROUP BY issuer_country, country_code, currency
"""

# ---------------------------------------------------------------------------
# Batch job 4 — Chargeback ratio analysis
# ---------------------------------------------------------------------------
# Registered views required:
#   {view_name}       — raw transactions for the processing date
#   {merchants_view}  — merchant dimension table
#
# Placeholders:
#   {view_name}        — transactions temp view name
#   {merchants_view}   — merchants temp view name
#   {threshold}        — chargeback ratio threshold (float, e.g. 0.02)
#   {processing_date}  — ISO date string

CHARGEBACK_RATIOS_SQL = """
WITH purchase_agg AS (
    SELECT
        merchant_id,
        COUNT(*)                         AS purchase_count,
        SUM(ABS(CAST(amount AS DOUBLE))) AS purchase_volume
    FROM {view_name}
    WHERE transaction_type = 'purchase'
    GROUP BY merchant_id
),
chargeback_agg AS (
    SELECT
        merchant_id,
        COUNT(*)                         AS chargeback_count,
        SUM(ABS(CAST(amount AS DOUBLE))) AS chargeback_volume
    FROM {view_name}
    WHERE transaction_type = 'chargeback'
    GROUP BY merchant_id
)
SELECT /*+ BROADCAST({merchants_view}) */
    p.merchant_id,
    m.merchant_name,
    m.merchant_category_code,
    p.purchase_count,
    p.purchase_volume,
    COALESCE(c.chargeback_count,  0)     AS chargeback_count,
    COALESCE(c.chargeback_volume, 0.0)   AS chargeback_volume,
    ROUND(
        COALESCE(c.chargeback_count,  0) / p.purchase_count,
        6
    )                                    AS chargeback_ratio_count,
    ROUND(
        COALESCE(c.chargeback_volume, 0.0)
        / (p.purchase_volume + COALESCE(c.chargeback_volume, 0.0)),
        6
    )                                    AS chargeback_ratio_value,
    COALESCE(c.chargeback_count,  0) / p.purchase_count > {threshold}
                                         AS is_flagged,
    CAST({threshold} AS DOUBLE)          AS threshold_applied,
    '{processing_date}'                  AS processing_date
FROM       purchase_agg    p
LEFT JOIN  chargeback_agg  c ON p.merchant_id = c.merchant_id
LEFT JOIN  {merchants_view} m ON p.merchant_id = m.merchant_id
"""
