"""
# 01 · Hello Airflow — your first DAG

**Concepts:** DAG, task, operator, dependencies (`>>`), schedule, start_date, `default_args`.

This DAG uses the *classic* style (`with DAG(...)` + operator classes) so you can see
every moving part. Example 02 shows the more modern TaskFlow style.

```
start ──► say_hello (Bash) ──► print_context (Python) ──► end
```

**Try it:** unpause the DAG (toggle), press ▶ *Trigger*, then click each task
square in the Grid view and open **Logs**.

📖 docs/05-first-dag.md
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

# default_args are passed to EVERY task in the DAG (a task can still override them).
default_args = {
    "owner": "tutorial",
    "retries": 1,  # re-try a failed task once...
    "retry_delay": timedelta(seconds=30),  # ...after waiting 30 seconds
}


def _print_context(**context) -> str:
    """A normal Python function. `context` holds run information (dates, run_id, task instance...)."""
    print(f"DAG id:       {context['dag'].dag_id}")
    print(f"Run id:       {context['run_id']}")
    print(f"Logical date: {context['logical_date']}")
    print(f"Task:         {context['ti'].task_id}, try number {context['ti'].try_number}")
    # Whatever a python_callable returns is stored as an XCom (see example 04).
    return "Hello from Python!"


with DAG(
    dag_id="01_hello_airflow",  # unique name shown in the UI
    description="The smallest useful DAG: Bash + Python tasks in a line",
    schedule="@daily",  # run once a day at midnight UTC (None = only manual runs)
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),  # first date the DAG may run for
    catchup=False,  # don't create runs for every missed day since start_date
    default_args=default_args,
    tags=["tutorial", "01-basics"],
    doc_md=__doc__,  # this file's docstring is shown on the DAG page in the UI
) as dag:
    start = EmptyOperator(task_id="start")  # does nothing, useful as a visual marker

    say_hello = BashOperator(
        task_id="say_hello",
        # {{ ds }} is a Jinja template: replaced by the run's date (YYYY-MM-DD) at runtime.
        bash_command='echo "Hello Airflow! This run is for {{ ds }}" && sleep 2',
    )

    print_context = PythonOperator(
        task_id="print_context",
        python_callable=_print_context,
    )

    end = EmptyOperator(task_id="end")

    # Dependencies: ">>" means "runs before". This builds the graph shown in the UI.
    start >> say_hello >> print_context >> end
