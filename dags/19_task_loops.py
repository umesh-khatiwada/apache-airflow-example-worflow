"""
# 19 · Task loops — repeat a task group until a condition is met (Airflow 3.4+)

**Concepts:** `@task_group(...).loop()`, `max_iterations`, `until=` condition,
`loop.index`, `loop.previous`, `loop.result`, `LoopDecision`.

Airflow DAGs are *acyclic*, so before 3.4 "repeat until done" needed sensors or
re-triggering the DAG. Task loops let a task group run again and again, each iteration
seeing the previous iteration's result.

Two DAGs:

* `19_task_loop_fixed`    — run a group exactly 3 times (accumulate a counter).
* `19_task_loop_until`    — poll a pretend "export job" until it reports `done`,
                            with a safety cap of 10 iterations.

📖 docs/23-task-loops.md
"""

from __future__ import annotations

import pendulum
from airflow.sdk import LoopDecision, dag, task, task_group

START = pendulum.datetime(2026, 1, 1, tz="UTC")


@dag(dag_id="19_task_loop_fixed", schedule=None, start_date=START, tags=["tutorial", "14-advanced"])
def task_loop_fixed():
    @task_group
    def accumulate():
        @task
        def add_ten(*, loop):
            # loop.index starts at 1. loop.previous = this group's result last iteration.
            previous = 0 if loop.index == 1 else loop.previous
            print(f"Iteration {loop.index}: {previous} + 10")
            return previous + 10

        add_ten()

    @task
    def report():
        print("Loop finished after exactly 3 iterations (total = 30)")

    accumulate.loop(max_iterations=3) >> report()


task_loop_fixed()


@dag(dag_id="19_task_loop_until", schedule=None, start_date=START, tags=["tutorial", "14-advanced"])
def task_loop_until():
    @task
    def start_export() -> str:
        return "export-job-42"

    @task_group
    def poll_export():
        @task
        def check_status(*, loop) -> dict:
            # Pretend the job needs 4 checks to finish: progress grows each iteration.
            progress = min(loop.index * 25, 100)
            status = "done" if progress == 100 else "running"
            print(f"Check #{loop.index}: {progress}% ({status})")
            return {"status": status, "progress": progress}

        check_status()

    # The condition runs after each iteration. Return a bool, or a LoopDecision with a reason.
    def export_finished(*, loop) -> LoopDecision:
        result = loop.result
        if result["status"] == "done":
            return LoopDecision(stop=True, reason=f"finished after {loop.index} checks")
        return LoopDecision(stop=False, reason=f"{result['progress']}% done, checking again")

    @task
    def download_export():
        print("Export ready -> downloading")

    start_export() >> poll_export.loop(max_iterations=10, until=export_finished) >> download_export()


task_loop_until()
