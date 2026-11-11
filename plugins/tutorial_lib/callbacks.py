"""
Callback functions used by the error-handling examples.

A *callback* is a plain function Airflow calls when something happens:

* ``on_success_callback`` – task/DAG run finished successfully
* ``on_failure_callback`` – task/DAG run failed (after all retries)
* ``on_retry_callback``   – a task failed but will be retried
* ``on_skipped_callback`` – a task was skipped (raised AirflowSkipException)

Task callbacks receive the task *context* (the same dict you get with
``get_current_context()``). In real projects you would post to Slack, Teams,
PagerDuty or e-mail here — we only log so the examples work offline.
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def task_failure_alert(context) -> None:
    ti = context["ti"]
    exception = context.get("exception")
    log.error(
        "ALERT: task %s in DAG %s failed on try %s. Error: %r",
        ti.task_id,
        ti.dag_id,
        ti.try_number,
        exception,
    )


def task_retry_alert(context) -> None:
    ti = context["ti"]
    log.warning("Task %s failed on try %s and will be retried.", ti.task_id, ti.try_number)


def task_success_log(context) -> None:
    ti = context["ti"]
    log.info("Task %s succeeded (try %s).", ti.task_id, ti.try_number)


def dag_failure_alert(context) -> None:
    """DAG-level callbacks get a context too; `dag_run` tells you which run failed."""
    dag_run = context.get("dag_run")
    log.error("ALERT: DAG run %s failed. Reason: %s", dag_run, context.get("reason"))


def dag_success_log(context) -> None:
    log.info("DAG run %s finished successfully.", context.get("dag_run"))


async def deadline_missed_alert(context: dict, **kwargs) -> None:
    """
    Called by the *triggerer* when a DeadlineAlert is missed.

    Deadline callbacks wrapped in ``AsyncCallback`` must be ``async`` functions that
    live in an importable module (like this one) because the triggerer imports them.
    """
    log.warning("DEADLINE MISSED for dag_run=%s extra=%s", context.get("dag_run"), kwargs)
