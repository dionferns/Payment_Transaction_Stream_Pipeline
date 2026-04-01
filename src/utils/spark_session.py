"""
Spark session factory for the payment stream pipeline.

Centralising session creation here means all jobs pick up the same baseline
configuration (AQE, Kryo, broadcast thresholds, etc.) without repeating
boilerplate.  Job-specific overrides are passed via ``extra_conf``.
"""

from __future__ import annotations

import os
from typing import Optional

from pyspark.sql import SparkSession

from config.spark_config import BASE_SPARK_CONF, BATCH_SPARK_CONF, STREAMING_SPARK_CONF


def get_spark_session(
    app_name: Optional[str] = None,
    mode: str = "batch",
    extra_conf: Optional[dict[str, str]] = None,
) -> SparkSession:
    """Create or retrieve the active SparkSession.

    Parameters
    ----------
    app_name:
        Application name shown in the Spark UI.  Defaults to the
        ``SPARK_APP_NAME`` environment variable or ``payment-stream-pipeline``.
    mode:
        Configuration profile to apply — ``"batch"`` or ``"streaming"``.
        Batch mode adds dynamic partition pruning; streaming mode adds
        graceful shutdown and metrics settings.
    extra_conf:
        Additional Spark configuration key-value pairs that override the
        selected baseline profile.  Useful for job-specific tuning.

    Returns
    -------
    SparkSession
        An active SparkSession configured for the requested mode.
    """
    name = app_name or os.getenv("SPARK_APP_NAME", "payment-stream-pipeline")
    master = os.getenv("SPARK_MASTER", "local[*]")

    conf_map = STREAMING_SPARK_CONF if mode == "streaming" else BATCH_SPARK_CONF
    if extra_conf:
        conf_map = {**conf_map, **extra_conf}

    builder = SparkSession.builder.appName(name).master(master)

    for key, value in conf_map.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    return spark


def stop_spark_session(spark: SparkSession) -> None:
    """Gracefully stop a SparkSession."""
    spark.stop()
