"""
# 04 · XCom — passing small data between tasks

**Concepts:** return values, explicit `xcom_push`/`xcom_pull`, custom keys,
pulling from several tasks, XCom in Jinja templates, what NOT to put in XCom.

XCom ("cross-communication") stores small values (JSON-serialisable: numbers,
strings, dicts, lists, dates…) in the metadata database so later tasks can read them.

⚠️ Keep XComs **small** (KBs, not MBs). For large data, write to a file / bucket /
table and pass the *path* via XCom.

Open a task → **XCom** tab in the UI to see the stored values.

📖 docs/08-xcom.md
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task


@dag(
    dag_id="04_xcom",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    tags=["tutorial", "02-data-passing"],
    doc_md=__doc__,
)
def xcom_examples():
    # 1) The return value is pushed automatically with key "return_value".
    @task
    def produce_number() -> int:
        return 42

    # 2) Push extra values with your own keys using the TaskInstance (`ti`).
    #    Airflow injects `ti` because the parameter has that exact name.
    @task
    def push_with_keys(ti=None) -> None:
        ti.xcom_push(key="file_path", value="/opt/airflow/data/report.csv")
        ti.xcom_push(key="row_count", value=1234)

    # 3) Pull explicitly by task_id and key.
    @task
    def pull_explicitly(ti=None) -> dict:
        number = ti.xcom_pull(task_ids="produce_number")  # key defaults to return_value
        path = ti.xcom_pull(task_ids="push_with_keys", key="file_path")
        rows = ti.xcom_pull(task_ids="push_with_keys", key="row_count")
        print(f"number={number}, path={path}, rows={rows}")
        return {"number": number, "path": path, "rows": rows}

    # 4) Pull from several tasks at once -> returns a list in the same order.
    @task
    def pull_many(ti=None) -> list:
        values = ti.xcom_pull(task_ids=["produce_number", "pull_explicitly"])
        print(f"Got: {values}")
        return values

    # 5) The TaskFlow way: just pass the return value as an argument.
    @task
    def double(value: int) -> int:
        return value * 2

    number = produce_number()
    keys = push_with_keys()
    explicit = pull_explicitly()

    # Explicit pulls don't create dependencies automatically, so declare them:
    [number, keys] >> explicit >> pull_many()

    doubled = double(number)

    # 6) Use XComs inside Jinja templates of classic operators.
    BashOperator(
        task_id="xcom_in_template",
        bash_command=(
            "echo 'doubled = {{ ti.xcom_pull(task_ids=\"double\") }}' && "
            "echo 'file = {{ ti.xcom_pull(task_ids=\"push_with_keys\", key=\"file_path\") }}'"
        ),
    ).set_upstream([doubled, keys])


xcom_examples()
