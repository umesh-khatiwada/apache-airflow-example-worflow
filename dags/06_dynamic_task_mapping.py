"""
# 06 · Dynamic Task Mapping — create tasks at runtime

**Concepts:** `.expand()`, `.partial()`, mapping over an upstream result,
`.expand_kwargs()`, mapping classic operators, `.map()` to transform results,
"reduce" step, `map_index_template` for readable labels, `max_active_tis_per_dag`.

You often don't know *how many* things to process until the DAG runs (files in a
folder, rows in a table…). Mapping creates **one task instance per item** at runtime.

In the Grid view a mapped task shows `[ ]` — click it to see each mapped instance.

📖 docs/10-dynamic-task-mapping.md
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, get_current_context, task


@dag(
    dag_id="06_dynamic_task_mapping",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    tags=["tutorial", "03-control-flow"],
    doc_md=__doc__,
)
def dynamic_task_mapping():
    # ---- 1) Map over a list known when the DAG is written --------------------------
    @task
    def square(x: int) -> int:
        return x * x

    squares = square.expand(x=[1, 2, 3, 4])  # creates 4 task instances

    # ---- 2) Reduce: a normal task receives ALL mapped results as a list ------------
    @task
    def total(values: list[int]) -> int:
        result = sum(values)
        print(f"Sum of squares = {result}")
        return result

    total(squares)

    # ---- 3) Map over the result of an upstream task (count unknown in advance) -----
    @task
    def list_files() -> list[str]:
        # In real life: list an S3 prefix, query a table, call an API…
        return ["orders_north.csv", "orders_south.csv", "orders_east.csv"]

    # partial() = arguments shared by every instance; expand() = one instance per value.
    # map_index_template shows the file name instead of 0,1,2 in the UI.
    @task(map_index_template="{{ file_name }}")
    def process_file(file_name: str, bucket: str) -> dict:
        context = get_current_context()
        context["file_name"] = file_name  # used by map_index_template above
        rows = len(file_name) * 10  # pretend work
        print(f"Processing s3://{bucket}/{file_name} -> {rows} rows")
        return {"file": file_name, "rows": rows}

    processed = process_file.partial(bucket="raw-data").expand(file_name=list_files())

    # ---- 4) .map(): transform each upstream result before mapping again ------------
    def to_message(result: dict) -> str:  # plain function, runs lazily per item
        return f"{result['file']} had {result['rows']} rows"

    @task
    def notify(message: str) -> None:
        print(message)

    notify.expand(message=processed.map(to_message))

    # ---- 5) expand_kwargs(): map over a list of dicts (several args per instance) ---
    @task
    def copy_table(source: str, target: str) -> str:
        print(f"Copying {source} -> {target}")
        return target

    copy_table.expand_kwargs(
        [
            {"source": "raw.customers", "target": "staging.customers"},
            {"source": "raw.orders", "target": "staging.orders"},
        ]
    )

    # ---- 6) Mapping a classic operator, with a concurrency limit --------------------
    BashOperator.partial(
        task_id="mapped_bash",
        max_active_tis_per_dag=2,  # at most 2 of these instances run at the same time
    ).expand(bash_command=[f"echo 'chunk {i}' && sleep 3" for i in range(5)])

    # ---- 7) Cross product: expand over two lists => len(a) * len(b) instances -------
    @task
    def deploy(region: str, env: str) -> None:
        print(f"Deploying to {env} in {region}")

    deploy.expand(region=["eu", "us"], env=["staging", "prod"])  # 4 instances


dynamic_task_mapping()
