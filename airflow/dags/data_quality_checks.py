"""
Airflow DAG: data_quality_checks

Runs data quality checks after the daily batch completes.  This DAG is
triggered by the ``daily_batch_processing`` DAG via ExternalTaskSensor, which
ensures quality checks never run against incomplete data.

Task flow:
    wait_for_batch ──► run_quality_checks ──► evaluate_results ──► (alert on failure)
"""

from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

from plugins.callbacks import on_failure_callback, sla_miss_callback

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": on_failure_callback,
}

SPARK_CONN_ID = "spark_default"
SPARK_MASTER = "spark://spark-master:7077"


def _evaluate_quality_results(**context: object) -> str:
    """Read the quality results Parquet for the run date and branch based on outcome.

    Returns the task_id of the next task to execute:
      - ``"quality_passed"``  if all checks passed
      - ``"quality_failed"``  if any check failed
    """
    import os

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.master("local[2]").getOrCreate()
    quality_path = os.getenv("QUALITY_RESULTS_PATH", "./data/quality")
    run_date: str = context["ds"]  # type: ignore[assignment]

    try:
        df = spark.read.parquet(
            f"{quality_path}/data_quality_results/processing_date={run_date}"
        )
        any_failures = df.filter(F.col("status") == "FAIL").count() > 0
        return "quality_failed" if any_failures else "quality_passed"
    except Exception:  # noqa: BLE001
        return "quality_failed"
    finally:
        spark.stop()


def _quality_passed(**context: object) -> None:
    """No-op success branch — logged for observability."""
    import logging
    logging.getLogger(__name__).info("Quality checks passed for %s", context["ds"])


def _quality_failed(**context: object) -> None:
    """Failure branch — raises an exception to mark the task as failed."""
    raise RuntimeError(
        f"Data quality checks FAILED for {context['ds']}. "
        "Check the quality results Parquet for details."
    )


with DAG(
    dag_id="data_quality_checks",
    description="Post-batch data quality validation",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 4 * * *",   # 04:00 UTC — after batch window
    start_date=days_ago(1),
    catchup=False,
    tags=["payments", "quality", "spark"],
    sla_miss_callback=sla_miss_callback,
    doc_md=__doc__,
) as dag:

    wait_for_batch = ExternalTaskSensor(
        task_id="wait_for_batch",
        external_dag_id="daily_batch_processing",
        external_task_id="daily_settlement",
        timeout=7200,          # wait up to 2 hours
        poke_interval=60,
        mode="reschedule",     # releases the worker slot while waiting
    )

    run_quality_checks = SparkSubmitOperator(
        task_id="run_quality_checks",
        application="src/quality/runner.py",
        name="quality-checks-{{ ds }}",
        conn_id=SPARK_CONN_ID,
        application_args=["--date", "{{ ds }}"],
        conf={
            "spark.master": SPARK_MASTER,
            "spark.executor.memory": "1g",
            "spark.driver.memory": "512m",
        },
        sla=timedelta(hours=1),
    )

    evaluate_results = BranchPythonOperator(
        task_id="evaluate_results",
        python_callable=_evaluate_quality_results,
    )

    quality_passed = PythonOperator(
        task_id="quality_passed",
        python_callable=_quality_passed,
    )

    quality_failed = PythonOperator(
        task_id="quality_failed",
        python_callable=_quality_failed,
        on_failure_callback=on_failure_callback,
    )

    wait_for_batch >> run_quality_checks >> evaluate_results
    evaluate_results >> [quality_passed, quality_failed]
