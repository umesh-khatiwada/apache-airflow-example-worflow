"""
# 05 · Branching, short-circuiting & trigger rules

**Concepts:** `@task.branch`, `@task.short_circuit`, trigger rules
(`all_success`, `none_failed_min_one_success`, `all_done`, `one_failed`, `always`),
edge labels, params that change behaviour.

```
choose_plan ─┬─(small)─► process_small ─┐
             └─(large)─► process_large ─┼─► join ─► is_business_day? ─► send_report
                                        │
           maybe_fail ──────────────────┴─► on_any_failure (one_failed)
                                             cleanup (all_done)
```

Trigger the DAG with **Trigger ▸ with config** and change `order_count` or
`make_it_fail` to see different paths.

📖 docs/09-branching-and-trigger-rules.md
"""

from __future__ import annotations

import pendulum
from airflow.sdk import Label, Param, TriggerRule, dag, task


@dag(
    dag_id="05_branching_and_trigger_rules",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    tags=["tutorial", "03-control-flow"],
    doc_md=__doc__,
    params={
        "order_count": Param(150, type="integer", minimum=0, description="Orders to process"),
        "make_it_fail": Param(False, type="boolean", description="Force a failure to see trigger rules"),
    },
)
def branching_and_trigger_rules():
    # ---- Branching -----------------------------------------------------------------
    # A branch task returns the task_id (or list of ids) to follow. Others are SKIPPED.
    @task.branch
    def choose_plan(params=None) -> str:
        if params["order_count"] > 100:
            return "process_large"
        return "process_small"

    @task
    def process_small(params=None):
        print(f"Processing {params['order_count']} orders on a single worker")

    @task
    def process_large(params=None):
        print(f"Processing {params['order_count']} orders in big batches")

    # After a branch one path is always skipped. The default rule (all_success) would
    # skip `join` too, so we use NONE_FAILED_MIN_ONE_SUCCESS.
    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def join():
        print("One of the branches finished")

    # ---- Short circuit ---------------------------------------------------------------
    # Returns True -> continue. Returns False -> skip EVERYTHING downstream.
    @task.short_circuit
    def is_business_day(logical_date=None) -> bool:
        if logical_date is None:  # manual runs may have no logical date
            return True
        return logical_date.weekday() < 5

    @task
    def send_report():
        print("Report sent!")

    # ---- Failure handling with trigger rules -------------------------------------------
    @task(retries=0)
    def maybe_fail(params=None):
        if params["make_it_fail"]:
            raise ValueError("Failing on purpose (make_it_fail=true)")
        print("All good")

    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def on_any_failure():
        print("Something upstream failed -> alert the team!")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def cleanup():
        print("Runs no matter what happened upstream (success, failed or skipped)")

    # ---- Wiring --------------------------------------------------------------------------
    branch = choose_plan()
    small, large = process_small(), process_large()
    joined = join()
    branch >> Label("≤ 100 orders") >> small >> joined
    branch >> Label("> 100 orders") >> large >> joined
    joined >> is_business_day() >> send_report()

    failing = maybe_fail()
    [failing, joined] >> on_any_failure()
    [failing, joined] >> cleanup()


branching_and_trigger_rules()
