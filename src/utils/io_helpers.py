"""
I/O helpers for reading from and writing to the landing, processed, and
analytics storage zones.

All paths are resolved from environment variables so the same code runs
locally (file-system), in Docker (bind-mounted volumes), and on cloud storage
(s3a://, gs://) without modification.
"""

from __future__ import annotations

import os
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Zone path resolution
# ---------------------------------------------------------------------------

def _zone_path(env_var: str, default: str) -> str:
    return os.getenv(env_var, default)


def landing_zone_path() -> str:
    return _zone_path("LANDING_ZONE_PATH", "./data/landing")


def processed_zone_path() -> str:
    return _zone_path("PROCESSED_ZONE_PATH", "./data/processed")


def analytics_zone_path() -> str:
    return _zone_path("ANALYTICS_ZONE_PATH", "./data/analytics")


def quality_results_path() -> str:
    return _zone_path("QUALITY_RESULTS_PATH", "./data/quality")


def seeds_path() -> str:
    return _zone_path("SEEDS_PATH", "./data/seeds")


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------

def read_json_landing(
    spark: SparkSession,
    dataset: str,
    date: Optional[str] = None,
    hour: Optional[int] = None,
) -> DataFrame:
    """Read raw JSON transaction files from the landing zone.

    The landing zone is partitioned as::

        landing/<dataset>/date=YYYY-MM-DD/hour=HH/*.json

    Partition-predicate pushdown is applied automatically when ``date`` and/or
    ``hour`` are provided, preventing a full scan of historical data.

    Parameters
    ----------
    spark:
        Active SparkSession.
    dataset:
        Sub-directory name under the landing zone root (e.g. ``"transactions"``).
    date:
        ISO-format date string (``"2024-01-15"``).  Limits scan to one day.
    hour:
        UTC hour (0–23).  Requires ``date`` to be set.
    """
    base = f"{landing_zone_path()}/{dataset}"
    if date:
        base = f"{base}/date={date}"
        if hour is not None:
            base = f"{base}/hour={hour:02d}"
    path = f"{base}/*.json"
    logger.info("reading_json_landing", path=path)
    return spark.read.json(path)


def read_parquet(
    spark: SparkSession,
    zone: str,
    dataset: str,
    filters: Optional[list[tuple[str, str, str]]] = None,
) -> DataFrame:
    """Read partitioned Parquet from a storage zone.

    Parameters
    ----------
    spark:
        Active SparkSession.
    zone:
        One of ``"processed"`` or ``"analytics"``.
    dataset:
        Dataset name (sub-directory under the zone root).
    filters:
        Optional list of ``(column, op, value)`` triples forwarded to
        ``spark.read.parquet`` for partition pruning — e.g.
        ``[("date", "=", "2024-01-15")]``.
    """
    zone_roots = {
        "processed": processed_zone_path(),
        "analytics": analytics_zone_path(),
    }
    path = f"{zone_roots[zone]}/{dataset}"
    logger.info("reading_parquet", zone=zone, dataset=dataset, path=path)
    df = spark.read.parquet(path)
    if filters:
        condition = None
        for col, op, val in filters:
            clause = F.col(col) == val if op == "=" else F.expr(f"{col} {op} '{val}'")
            condition = clause if condition is None else condition & clause
        df = df.filter(condition)
    return df


def read_seed(spark: SparkSession, filename: str) -> DataFrame:
    """Read a CSV dimension/seed table.

    Parameters
    ----------
    spark:
        Active SparkSession.
    filename:
        Filename inside the seeds directory (e.g. ``"merchants.csv"``).
    """
    path = f"{seeds_path()}/{filename}"
    logger.info("reading_seed", path=path)
    return spark.read.csv(path, header=True, inferSchema=True)


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------

def write_parquet(
    df: DataFrame,
    zone: str,
    dataset: str,
    partition_by: Optional[list[str]] = None,
    mode: str = "overwrite",
) -> None:
    """Write a DataFrame as partitioned Parquet to a storage zone.

    Parameters
    ----------
    df:
        DataFrame to persist.
    zone:
        Target zone: ``"processed"``, ``"analytics"``, or ``"quality"``.
    dataset:
        Dataset name (becomes the sub-directory).
    partition_by:
        Columns to partition the output by.  Omit for unpartitioned writes.
    mode:
        Spark write mode — ``"overwrite"`` or ``"append"``.
    """
    zone_roots = {
        "processed": processed_zone_path(),
        "analytics": analytics_zone_path(),
        "quality": quality_results_path(),
    }
    path = f"{zone_roots[zone]}/{dataset}"
    writer = df.write.format("parquet").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    logger.info("writing_parquet", zone=zone, dataset=dataset, path=path, mode=mode)
    writer.save(path)


def write_json_landing(
    df: DataFrame,
    dataset: str,
    date: str,
    hour: int,
    mode: str = "append",
) -> None:
    """Write a DataFrame as JSON to the landing zone.

    Used by the generator and by tests that seed the landing zone.
    """
    path = f"{landing_zone_path()}/{dataset}/date={date}/hour={hour:02d}"
    logger.info("writing_json_landing", path=path, mode=mode)
    df.write.json(path, mode=mode)
