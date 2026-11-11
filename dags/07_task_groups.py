"""
# 07 · Task Groups — organise big DAGs

**Concepts:** `@task_group` decorator, `TaskGroup` context manager, nested groups,
group-level `default_args`, reusing a group several times, mapped task groups,
`tooltip`, `ui_color`.

Task groups are **purely visual/organisational**: they collapse into one box in the
Graph view and prefix task ids (`extract.download`). They don't change how tasks run.

📖 docs/11-task-groups.md
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskGroup, dag, task, task_group


@dag(
    dag_id="07_task_groups",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    tags=["tutorial", "04-organisation"],
    doc_md=__doc__,
)
def task_groups():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # ---- 1) Decorator style: a reusable "function that builds tasks" ---------------
    @task_group(tooltip="Download + validate one source", ui_color="#fff4d6")
    def ingest(source: str):
        @task
        def download(src: str) -> str:
            print(f"Downloading {src}")
            return f"/tmp/{src}.json"

        @task
        def validate(path: str) -> str:
            print(f"Validating {path}")
            return path

        # No `return` here: the call then gives back the TaskGroup itself, so
        # `start >> group` connects to the group's FIRST task (download).
        # Returning validate(...) would make `start >> group` point at validate only.
        validate(download(source))

    # Reuse the same group twice. group_id must be unique -> override() it.
    customers = ingest.override(group_id="ingest_customers")("customers")
    orders = ingest.override(group_id="ingest_orders")("orders")

    # ---- 2) Context-manager style + nesting + group default_args -------------------
    with TaskGroup(
        group_id="transform",
        tooltip="Clean then aggregate",
        default_args={"retries": 2},  # applies to every task inside this group
    ) as transform:
        with TaskGroup(group_id="clean") as clean:
            EmptyOperator(task_id="dedupe") >> EmptyOperator(task_id="fix_types")

        with TaskGroup(group_id="aggregate"):
            daily = EmptyOperator(task_id="daily")
            weekly = EmptyOperator(task_id="weekly")

        clean >> [daily, weekly]

    # ---- 3) Mapped task group: run the whole group once per item -------------------
    @task_group
    def publish(destination: str):
        @task
        def render(dest: str) -> str:
            return f"report for {dest}"

        @task
        def upload(report: str, dest: str) -> None:
            print(f"Uploading '{report}' to {dest}")

        upload(render(destination), destination)

    published = publish.expand(destination=["s3", "email", "slack"])

    # Groups are used in dependencies exactly like tasks.
    start >> [customers, orders] >> transform >> published >> end


task_groups()
