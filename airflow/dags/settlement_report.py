"""
Airflow DAG: settlement_report

Generates the daily settlement report after quality checks pass.  The report
is a summary Parquet file partitioned by date, consumed by the finance team's
BI tooling.

Task flow:
    wait_for_quality ──► generate_report ──► notify_finance
"""

from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

from plugins.callbacks import on_failure_callback, on_success_callback

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": True,    # Settlement is sequential — prior day must close
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}

SPARK_CONN_ID = "spark_default"
SPARK_MASTER = "spark://spark-master:7077"


def _notify_finance(**context: object) -> None:
    """Post settlement completion notification to the finance Slack channel."""
    import json
    import os
    import urllib.request

    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook:
        return

    run_date: str = context["ds"]  # type: ignore[assignment]
    analytics_path = os.getenv("ANALYTICS_ZONE_PATH", "./data/analytics")

    message = (
        f":bank: *Daily Settlement Report Ready*\n"
        f"Processing date: `{run_date}`\n"
        f"Output path: `{analytics_path}/daily_settlement/processing_date={run_date}`"
    )
    payload = json.dumps({"text": message}).encode()
    req = urllib.request.Request(
        webhook,
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req, timeout=5)
    except Exception:  # noqa: BLE001
        pass


with DAG(
    dag_id="settlement_report",
    description="Daily settlement report generation",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 5 * * *",   # 05:00 UTC — after quality checks
    start_date=days_ago(1),
    catchup=False,
    tags=["payments", "settlement", "finance"],
    doc_md=__doc__,
) as dag:

    wait_for_quality = ExternalTaskSensor(
        task_id="wait_for_quality",
        external_dag_id="data_quality_checks",
        external_task_id="quality_passed",
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    generate_report = SparkSubmitOperator(
        task_id="generate_settlement_report",
        application="src/batch/daily_settlement.py",
        name="settlement-report-{{ ds }}",
        conn_id=SPARK_CONN_ID,
        application_args=["--date", "{{ ds }}"],
        conf={
            "spark.master": SPARK_MASTER,
            "spark.executor.memory": "2g",
            "spark.driver.memory": "1g",
            "spark.sql.adaptive.enabled": "true",
        },
        sla=timedelta(hours=1),
        on_success_callback=on_success_callback,
    )

    notify_finance = PythonOperator(
        task_id="notify_finance",
        python_callable=_notify_finance,
    )

    wait_for_quality >> generate_report >> notify_finance
