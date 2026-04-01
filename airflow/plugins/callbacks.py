"""
Airflow callback functions shared across all pipeline DAGs.

Callbacks are invoked by Airflow on task success, failure, retry, and SLA
miss events.  They emit structured log lines and, when configured, post to
Slack.
"""

from __future__ import annotations

import json
import os
import urllib.request
from datetime import datetime
from typing import Any

from airflow.models import TaskInstance


def _post_slack(message: str) -> None:
    """Fire-and-forget Slack webhook notification."""
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook:
        return
    try:
        payload = json.dumps({"text": message}).encode()
        req = urllib.request.Request(
            webhook,
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception:  # noqa: BLE001
        pass  # Alerting must never break the pipeline itself


def on_failure_callback(context: dict[str, Any]) -> None:
    """Called when any task fails.  Sends a Slack alert with task context."""
    ti: TaskInstance = context["task_instance"]
    execution_date = context.get("execution_date", "unknown")
    exception = context.get("exception", "unknown error")

    message = (
        f":red_circle: *Pipeline Task Failed*\n"
        f"DAG: `{ti.dag_id}`\n"
        f"Task: `{ti.task_id}`\n"
        f"Execution: `{execution_date}`\n"
        f"Error: `{exception}`"
    )
    _post_slack(message)


def on_success_callback(context: dict[str, Any]) -> None:
    """Called on task success — only posts for milestone tasks (SLA-tracked)."""
    ti: TaskInstance = context["task_instance"]
    if not getattr(ti.task, "sla", None):
        return  # Only alert on SLA-bearing tasks to avoid noise

    execution_date = context.get("execution_date", "unknown")
    _post_slack(
        f":white_check_mark: `{ti.dag_id}.{ti.task_id}` completed — {execution_date}"
    )


def sla_miss_callback(
    dag: Any,
    task_list: str,
    blocking_task_list: str,
    slas: Any,
    blocking_tis: Any,
) -> None:
    """Called when a task misses its SLA window."""
    _post_slack(
        f":warning: *SLA Miss* in DAG `{dag.dag_id}`\n"
        f"Missed tasks: `{task_list}`\n"
        f"Blocking tasks: `{blocking_task_list}`"
    )
