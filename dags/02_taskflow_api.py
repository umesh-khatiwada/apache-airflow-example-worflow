"""
# 02 · TaskFlow API — Python functions as tasks

**Concepts:** `@dag`, `@task`, passing data by return values, `multiple_outputs`,
`@task.bash`, mixing TaskFlow with classic operators.

With TaskFlow you write plain Python functions. Calling one task with the output of
another **creates the dependency and passes the data automatically** (using XCom under
the hood). No `>>` needed.

```
extract ──► transform ──► load ──► report (bash)
                 └──► (multiple_outputs) total / count used separately
```

📖 docs/06-taskflow-api.md
"""

from __future__ import annotations

import json

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task


@dag(
    dag_id="02_taskflow_api",
    schedule=None,  # manual trigger only
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    tags=["tutorial", "01-basics"],
    doc_md=__doc__,
)
def taskflow_api():
    @task
    def extract() -> dict:
        """Pretend to download orders. The returned dict is saved as an XCom."""
        data = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
        return json.loads(data)

    # multiple_outputs=True stores each dict key as its own XCom, so downstream tasks
    # can use summary["total"] and summary["count"] independently.
    @task(multiple_outputs=True)
    def transform(orders: dict) -> dict:
        total = sum(orders.values())
        return {"total": round(total, 2), "count": len(orders)}

    @task
    def load(total: float, count: int) -> str:
        message = f"{count} orders, total value {total}"
        print(message)
        return message

    # @task.bash: the function RETURNS the bash command to run.
    @task.bash
    def report(message: str) -> str:
        return f'echo "REPORT: {message}"'

    # Calling the functions wires everything together:
    orders = extract()
    summary = transform(orders)
    message = load(summary["total"], summary["count"])
    report(message)

    # Classic operators mix fine with TaskFlow. Here we read the XCom of `load`
    # with Jinja, and set the dependency with >>.
    classic_bash_reads_xcom = BashOperator(
        task_id="classic_bash_reads_xcom",
        bash_command="echo 'load returned: {{ ti.xcom_pull(task_ids=\"load\") }}'",
    )
    message >> classic_bash_reads_xcom


# Calling the @dag function registers the DAG. Don't forget this line!
taskflow_api()
