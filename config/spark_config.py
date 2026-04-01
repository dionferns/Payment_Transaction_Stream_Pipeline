"""
Spark session tuning parameters for the payment stream pipeline.

Centralising Spark config here means every job shares the same baseline
and overrides remain explicit and reviewable.
"""

from __future__ import annotations

import os
from typing import Any


# ---------------------------------------------------------------------------
# Baseline configuration shared across all jobs
# ---------------------------------------------------------------------------

BASE_SPARK_CONF: dict[str, Any] = {
    # ---- Serialisation --------------------------------------------------------
    # Kryo is faster and more compact than Java serialisation for Spark internals.
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.kryo.unsafe": "true",

    # ---- Adaptive Query Execution (AQE) ---------------------------------------
    # AQE re-optimises the physical plan at runtime using shuffle statistics,
    # automatically coalescing small partitions and converting sort-merge joins
    # to broadcast joins when one side becomes small after filtering.
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",

    # ---- Shuffle -------------------------------------------------------------
    # 200 default partitions is almost always wrong.  For a ~1 M row/day volume
    # 64 gives ~1–2 MB per partition after a shuffle, which is in the sweet spot.
    # Increase proportionally for larger datasets.
    "spark.sql.shuffle.partitions": "64",

    # ---- Broadcast joins for small dimension tables --------------------------
    # Dimension tables (merchants, issuers, acquirers) are small; broadcasting
    # them avoids a full shuffle-merge join.  10 MB is the default; 50 MB is
    # safe for our seed tables.
    "spark.sql.autoBroadcastJoinThreshold": str(50 * 1024 * 1024),

    # ---- Parquet / IO --------------------------------------------------------
    "spark.sql.parquet.compression.codec": os.getenv("PARQUET_COMPRESSION", "snappy"),
    "spark.sql.parquet.mergeSchema": "false",       # Schema is controlled by the pipeline
    "spark.sql.parquet.filterPushdown": "true",     # Predicate pushdown to skip row groups
    "spark.hadoop.parquet.enable.summary-metadata": "false",  # Skip metadata files on write

    # ---- Delta Lake (if enabled) ---------------------------------------------
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",

    # ---- Memory management ---------------------------------------------------
    # 60/40 split between execution and storage; default is 50/50.  Payment
    # aggregations are shuffle-heavy so more execution memory helps.
    "spark.memory.fraction": "0.8",
    "spark.memory.storageFraction": "0.3",

    # ---- Speculation -------------------------------------------------------------------------
    # Disabled by default in this pipeline — stragglers are more likely caused
    # by skew (handled by AQE) than by hardware faults in a controlled env.
    "spark.speculation": "false",
}


# ---------------------------------------------------------------------------
# Streaming-specific overrides
# ---------------------------------------------------------------------------

STREAMING_SPARK_CONF: dict[str, Any] = {
    **BASE_SPARK_CONF,
    # Checkpointing forces WAL writes; enable only what is needed.
    "spark.streaming.stopGracefullyOnShutdown": "true",
    # Micro-batch interval governed per-query via trigger, not globally here.
    "spark.sql.streaming.metricsEnabled": "true",
}


# ---------------------------------------------------------------------------
# Batch-specific overrides
# ---------------------------------------------------------------------------

BATCH_SPARK_CONF: dict[str, Any] = {
    **BASE_SPARK_CONF,
    # For large daily scans, DPP prunes partitions at the file-system level
    # before tasks are scheduled.
    "spark.sql.optimizer.dynamicPartitionPruning.enabled": "true",
    # Allow the planner to reuse the same broadcast hint across multiple joins.
    "spark.sql.broadcastTimeout": "300",
}
