"""
SQL query templates for the data quality checks that benefit from SQL
consolidation.

Three checks gain the most from SQL:

* ``NULLS_CHECK_SQL`` — replaces a Python loop that issued one ``.count()``
  Spark action per required column.  A single SQL query scans the DataFrame
  once and returns all null counts in a single row, reducing the number of
  Spark jobs from N (one per column) to 1.

* ``DUPLICATES_CHECK_SQL`` — replaces two separate ``.count()`` calls
  (total and distinct) with one query that computes both values and their
  difference in a single scan.

* ``AMOUNT_RANGES_CHECK_SQL`` — replaces a Python loop that issued one
  ``.count()`` action per transaction type with a single conditional
  aggregation query over all four types simultaneously.

The three remaining checks (``check_schema``, ``check_freshness``,
``check_referential_integrity``) do not benefit from SQL because they
operate on schema metadata or already perform a single efficient action.

Placeholders
------------
All templates accept ``{view_name}`` — the name of the registered Spark
temporary view for the batch DataFrame under test.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Null check — one scan for all required columns
# ---------------------------------------------------------------------------
# Returns a single row with one column per checked field.  Column names are
# ``null_count_<field_name>`` so the Python check function can extract counts
# by iterating over the result schema.
#
# The column list is built dynamically in ``check_nulls()`` and injected as
# ``{null_count_exprs}`` so the query works for any subset of columns.
#
# Placeholders:
#   {view_name}         — registered temp view name
#   {null_count_exprs}  — comma-separated CASE WHEN expressions, e.g.:
#       COUNT(CASE WHEN transaction_id IS NULL THEN 1 END) AS null_count_transaction_id,
#       COUNT(CASE WHEN amount IS NULL THEN 1 END) AS null_count_amount

NULLS_CHECK_SQL = """
SELECT
    {null_count_exprs}
FROM {view_name}
"""

# ---------------------------------------------------------------------------
# Duplicate check — total and distinct count in one scan
# ---------------------------------------------------------------------------
# Returns a single row with three columns:
#   total_count    — total number of rows
#   unique_count   — number of distinct transaction_id values
#   duplicate_count — total_count - unique_count
#
# Placeholders:
#   {view_name} — registered temp view name

DUPLICATES_CHECK_SQL = """
SELECT
    COUNT(*)                         AS total_count,
    COUNT(DISTINCT transaction_id)   AS unique_count,
    COUNT(*) - COUNT(DISTINCT transaction_id) AS duplicate_count
FROM {view_name}
"""

# ---------------------------------------------------------------------------
# Amount range check — all four transaction types in one scan
# ---------------------------------------------------------------------------
# Returns a single row with one violation-count column per transaction type.
# A value of 0 means no violations for that type.
#
# Rules enforced:
#   purchase   : amount must be between  0.01 and  50,000 (positive)
#   refund     : amount must be between -50,000 and -0.01 (negative)
#   chargeback : same range as refund
#   p2p        : amount must be between  0.01 and  10,000 (positive)
#
# Placeholders:
#   {view_name} — registered temp view name

AMOUNT_RANGES_CHECK_SQL = """
SELECT
    SUM(CASE
        WHEN transaction_type = 'purchase'
         AND (CAST(amount AS DOUBLE) < 0.01 OR CAST(amount AS DOUBLE) > 50000.0)
        THEN 1 ELSE 0
    END) AS purchase_violations,
    SUM(CASE
        WHEN transaction_type = 'refund'
         AND (CAST(amount AS DOUBLE) >= -0.01 OR CAST(amount AS DOUBLE) < -50000.0)
        THEN 1 ELSE 0
    END) AS refund_violations,
    SUM(CASE
        WHEN transaction_type = 'chargeback'
         AND (CAST(amount AS DOUBLE) >= -0.01 OR CAST(amount AS DOUBLE) < -50000.0)
        THEN 1 ELSE 0
    END) AS chargeback_violations,
    SUM(CASE
        WHEN transaction_type = 'p2p'
         AND (CAST(amount AS DOUBLE) < 0.01 OR CAST(amount AS DOUBLE) > 10000.0)
        THEN 1 ELSE 0
    END) AS p2p_violations
FROM {view_name}
"""
