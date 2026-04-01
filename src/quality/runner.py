"""
Data quality check runner.

Executes all registered checks against a DataFrame, collects results, and
writes a structured Parquet report to the quality results zone.  If any
FAIL results are present and alerting is configured, a Slack notification
is sent.

spark-submit::

    spark-submit \\
      --master spark://spark-master:7077 \\
      src/quality/runner.py --date 2024-01-15
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timezone
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from src.quality.checks import (
    ALL_CHECKS,
    CheckResult,
    check_referential_integrity,
)
from src.utils.io_helpers import (
    quality_results_path,
    read_json_landing,
    read_seed,
    write_parquet,
)
from src.utils.logging_config import configure_logging, get_logger
from src.utils.spark_session import get_spark_session

logger = get_logger(__name__)

RESULTS_SCHEMA = StructType([
    StructField("check_name",    StringType(),  False),
    StructField("status",        StringType(),  False),
    StructField("message",       StringType(),  True),
    StructField("metric_value",  FloatType(),   True),
    StructField("threshold",     FloatType(),   True),
    StructField("run_timestamp", StringType(),  False),
    StructField("passed",        BooleanType(), False),
    StructField("processing_date", StringType(), False),
])


class QualityRunner:
    """Orchestrates data quality checks and persists results.

    Parameters
    ----------
    spark:
        Active SparkSession.
    processing_date:
        The date partition to validate.
    """

    def __init__(self, spark: SparkSession, processing_date: str) -> None:
        self.spark = spark
        self.processing_date = processing_date
        self._results: list[CheckResult] = []

    def run(self) -> list[CheckResult]:
        """Execute all checks and return results."""
        df = read_json_landing(self.spark, "transactions", date=self.processing_date)

        if df.rdd.isEmpty():
            logger.warning("no_data_for_date", date=self.processing_date)
            return []

        # Persist in memory so multiple checks don't re-read from disk
        df.cache()
        record_count = df.count()
        logger.info("quality_run_start", date=self.processing_date, records=record_count)

        for check_fn in ALL_CHECKS:
            try:
                result = check_fn(df)
                self._results.append(result)
                logger.info(
                    "check_result",
                    check=result.check_name,
                    status=result.status,
                    message=result.message,
                )
            except Exception as exc:  # noqa: BLE001
                self._results.append(
                    CheckResult(
                        check_name=getattr(check_fn, "__name__", "unknown"),
                        status="FAIL",
                        message=f"Check raised an exception: {exc}",
                    )
                )
                logger.exception("check_raised_exception", check=check_fn.__name__)

        # Referential integrity check (requires dimension table)
        try:
            merchants = read_seed(self.spark, "merchants.csv")
            ri_result = check_referential_integrity(df, dimension_df=merchants)
            self._results.append(ri_result)
        except Exception as exc:  # noqa: BLE001
            logger.warning("referential_integrity_skipped", reason=str(exc))

        df.unpersist()

        fail_count = sum(1 for r in self._results if r.status == "FAIL")
        logger.info(
            "quality_run_complete",
            date=self.processing_date,
            total_checks=len(self._results),
            failed_checks=fail_count,
        )

        return self._results

    def persist_results(self) -> None:
        """Write check results as a Parquet file to the quality zone."""
        if not self._results:
            logger.warning("no_results_to_persist")
            return

        rows = [
            Row(
                check_name=r.check_name,
                status=r.status,
                message=r.message,
                metric_value=float(r.metric_value) if r.metric_value is not None else None,
                threshold=float(r.threshold) if r.threshold is not None else None,
                run_timestamp=r.run_timestamp,
                passed=r.passed,
                processing_date=self.processing_date,
            )
            for r in self._results
        ]

        results_df = self.spark.createDataFrame(rows, schema=RESULTS_SCHEMA)
        write_parquet(
            results_df,
            zone="quality",
            dataset="data_quality_results",
            partition_by=["processing_date"],
        )
        logger.info("quality_results_persisted", rows=len(rows))

    def alert_on_failures(self) -> None:
        """Post a Slack alert if any checks failed."""
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        if not webhook_url:
            return

        failures = [r for r in self._results if r.status == "FAIL"]
        if not failures:
            return

        try:
            import urllib.request
            import json

            lines = [f"*Data Quality Alert* — `{self.processing_date}`"]
            lines.append(f"{len(failures)} check(s) FAILED:\n")
            for r in failures:
                lines.append(f"• *{r.check_name}*: {r.message}")

            payload = json.dumps({"text": "\n".join(lines)}).encode()
            req = urllib.request.Request(
                webhook_url,
                data=payload,
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=5)
            logger.info("slack_alert_sent", failure_count=len(failures))
        except Exception as exc:  # noqa: BLE001
            logger.warning("slack_alert_failed", reason=str(exc))


def run(spark: SparkSession, processing_date: str) -> None:
    """Execute quality checks, persist results, and alert on failures."""
    runner = QualityRunner(spark, processing_date)
    runner.run()
    runner.persist_results()
    runner.alert_on_failures()


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser(description="Data quality check runner")
    parser.add_argument("--date", default=str(date.today()))
    args = parser.parse_args()
    spark = get_spark_session(app_name="data-quality-runner", mode="batch")
    try:
        run(spark, args.date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
