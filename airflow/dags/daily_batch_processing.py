"""
Airflow DAG: daily_batch_processing

Orchestrates the full daily batch pipeline in dependency order:

    merchant_analytics ─┐
    cross_border         ├──► settlement ──► write_processed_zone
    chargeback_analysis ─┘

All Spark jobs are submitted to the standalone Spark cluster via SparkSubmitOperator.
Retries are configured at the task level; the DAG itself runs once per day at 01:00 UTC
(after midnight data cut-off).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from plugins.callbacks import on_failure_callback, on_success_callback, sla_miss_callback

SPARK_CONN_ID = "spark_default"
SPARK_MASTER = "spark://spark-master:7077"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}

SPARK_SUBMIT_DEFAULTS = {
    "conn_id": SPARK_CONN_ID,
    "application_args": ["--date", "{{ ds }}"],
    "conf": {
        "spark.master": SPARK_MASTER,
        "spark.executor.memory": "2g",
        "spark.driver.memory": "1g",
        "spark.sql.adaptive.enabled": "true",
    },
}

with DAG(
    dag_id="daily_batch_processing",
    description="Daily payment transaction batch processing pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 1 * * *",   # 01:00 UTC daily
    start_date=days_ago(1),
    catchup=False,
    tags=["payments", "batch", "spark"],
    sla_miss_callback=sla_miss_callback,
    doc_md=__doc__,
) as dag:

    merchant_analytics = SparkSubmitOperator(
        task_id="merchant_analytics",
        application="src/batch/merchant_analytics.py",
        name="merchant-analytics-{{ ds }}",
        sla=timedelta(hours=2),
        on_success_callback=on_success_callback,
        **SPARK_SUBMIT_DEFAULTS,
    )

    cross_border = SparkSubmitOperator(
        task_id="cross_border_analysis",
        application="src/batch/cross_border.py",
        name="cross-border-{{ ds }}",
        sla=timedelta(hours=2),
        **SPARK_SUBMIT_DEFAULTS,
    )

    chargeback_analysis = SparkSubmitOperator(
        task_id="chargeback_analysis",
        application="src/batch/chargeback_analysis.py",
        name="chargeback-analysis-{{ ds }}",
        sla=timedelta(hours=2),
        **SPARK_SUBMIT_DEFAULTS,
    )

    daily_settlement = SparkSubmitOperator(
        task_id="daily_settlement",
        application="src/batch/daily_settlement.py",
        name="daily-settlement-{{ ds }}",
        sla=timedelta(hours=3),
        on_success_callback=on_success_callback,
        **SPARK_SUBMIT_DEFAULTS,
    )

    # Merchant analytics, cross-border analysis, and chargeback analysis are
    # independent and can run in parallel.  Settlement depends on all three
    # because it consumes their output for the final position calculation.
    [merchant_analytics, cross_border, chargeback_analysis] >> daily_settlement
