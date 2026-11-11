"""
# 12 · Error handling: retries, timeouts, callbacks, notifiers, deadlines

**Concepts:** `retries`, `retry_delay`, `retry_exponential_backoff`, `max_retry_delay`,
`execution_timeout`, `dagrun_timeout`, success/failure/retry callbacks (task and DAG
level), Notifiers, `AirflowSkipException`, `AirflowFailException`, retry policies
(Airflow 3.4), `DeadlineAlert` (replacement for Airflow 2 SLAs).

| Task | What happens |
|------|--------------|
| `flaky_api_call`        | fails on try 1 and 2, succeeds on try 3 → see *retry* callbacks |
| `skip_when_no_data`     | raises AirflowSkipException → SKIPPED |
| `invalid_config`        | raises AirflowFailException → FAILED immediately, **no retries** (only if `fail_hard`) |
| `slow_task`             | has an `execution_timeout`; set `sleep_seconds` > 10 to see a timeout |
| `smart_retries`         | a retry *policy* decides per exception type whether to retry |

Trigger with config to flip the behaviour (`fail_hard`, `sleep_seconds`).

> **Known Airflow 3.4.0 issue:** `airflow dags test` / `dag.test()` on a DAG with a
> `deadline=` prints an `AttributeError: 'dict' object has no attribute 'hex'` traceback
> *after* the run has already finished successfully (the in-process test mode passes the
> deadline objects where stored IDs are expected). Runs started by the scheduler use the
> serialized DAG and don't go through this path. Remove `deadline=` while testing locally
> if the traceback bothers you.

📖 docs/16-error-handling-retries-alerts.md
"""

from __future__ import annotations

import time
from datetime import timedelta

import pendulum
from airflow.sdk import (
    AsyncCallback,
    DeadlineAlert,
    DeadlineReference,
    ExceptionRetryPolicy,
    Param,
    RetryAction,
    RetryRule,
    TriggerRule,
    dag,
    task,
)
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException

from tutorial_lib.callbacks import (
    dag_failure_alert,
    dag_success_log,
    deadline_missed_alert,
    task_failure_alert,
    task_retry_alert,
    task_success_log,
)
from tutorial_lib.notifiers import LogNotifier


class TemporaryNetworkError(Exception):
    """Pretend 'the network blipped' — worth retrying quickly."""


class BadCredentialsError(Exception):
    """Pretend 'the password is wrong' — retrying won't help."""


# A retry policy looks at the actual exception and decides what to do.
SMART_RETRY_POLICY = ExceptionRetryPolicy(
    rules=[
        RetryRule(exception=TemporaryNetworkError, action=RetryAction.RETRY, retry_delay=timedelta(seconds=5)),
        RetryRule(exception=BadCredentialsError, action=RetryAction.FAIL, reason="Credentials are wrong"),
    ]
)

default_args = {
    "owner": "tutorial",
    "retries": 3,
    "retry_delay": timedelta(seconds=5),
    "retry_exponential_backoff": 2.0,  # wait 5s, 10s, 20s… (multiplier)
    "max_retry_delay": timedelta(minutes=1),  # …but never more than 1 minute
    "execution_timeout": timedelta(minutes=10),  # kill any task running longer than this
    "on_failure_callback": task_failure_alert,
    "on_retry_callback": task_retry_alert,
}


@dag(
    dag_id="12_error_handling_and_alerts",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=30),  # whole run fails if it takes longer
    on_success_callback=dag_success_log,
    on_failure_callback=dag_failure_alert,
    # Deadline alert: call deadline_missed_alert if the run isn't finished within
    # 10 minutes of being queued. (Airflow 3 replacement for SLAs.)
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(minutes=10),
        callback=AsyncCallback(deadline_missed_alert),
        name="finish_within_10_minutes",
    ),
    params={
        "fail_hard": Param(False, type="boolean", description="Make invalid_config fail without retries"),
        "sleep_seconds": Param(3, type="integer", minimum=0, maximum=60, description="> 10 triggers a timeout"),
    },
    tags=["tutorial", "09-reliability"],
    doc_md=__doc__,
)
def error_handling_and_alerts():
    @task(on_success_callback=task_success_log)
    def flaky_api_call(ti=None) -> str:
        # try_number starts at 1 and increases on every retry.
        if ti.try_number < 3:
            raise ConnectionError(f"Simulated network glitch on try {ti.try_number}")
        return "Got data on try 3"

    @task
    def skip_when_no_data() -> None:
        new_rows = 0
        if new_rows == 0:
            raise AirflowSkipException("No new rows today, skipping.")

    @task(
        # Notifier: templated, reusable callback (see tutorial_lib/notifiers.py)
        on_failure_callback=LogNotifier("{{ ti.task_id }} in {{ dag.dag_id }} failed hard!", level="error"),
    )
    def invalid_config(params=None) -> None:
        if params["fail_hard"]:
            raise AirflowFailException("Config is invalid. Retrying will not help -> failing now.")
        print("Config OK")

    @task(execution_timeout=timedelta(seconds=10), retries=0)
    def slow_task(params=None) -> None:
        print(f"Sleeping {params['sleep_seconds']}s (timeout is 10s)")
        time.sleep(params["sleep_seconds"])

    @task(retries=3, retry_policy=SMART_RETRY_POLICY)
    def smart_retries(ti=None) -> str:
        if ti.try_number == 1:
            raise TemporaryNetworkError("blip")  # policy: retry after 5s
        return "Recovered after a temporary network error"

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def summary() -> None:
        print("Always runs: check each task's state in the Grid view.")

    [flaky_api_call(), skip_when_no_data(), invalid_config(), slow_task(), smart_retries()] >> summary()


error_handling_and_alerts()
