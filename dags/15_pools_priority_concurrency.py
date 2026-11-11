"""
# 15 · Pools, priority & concurrency limits

**Concepts:** pools (`api_pool` = 2 slots, created from config/pools.json),
`pool_slots`, `priority_weight`, `weight_rule`, DAG-level `max_active_tasks` and
`max_active_runs`, task-level `max_active_tis_per_dag`.

What to watch in the Grid view while it runs:

* `call_api` × 6 — only **2** run at once because `api_pool` has 2 slots.
* `urgent_report` has a high `priority_weight`, so it jumps ahead in the queue.
* `heavy_query` uses `pool_slots=2` of `database_pool` (4 slots) → max 2 at once.
* `max_active_tasks=6` caps how many tasks of this DAG run in parallel overall.

Check **Admin ▸ Pools** to see running / queued slots live.

📖 docs/19-pools-priority-concurrency.md
"""

from __future__ import annotations

import time

import pendulum
from airflow.sdk import WeightRule, dag, task


@dag(
    dag_id="15_pools_priority_concurrency",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    max_active_runs=1,  # only one run of this DAG at a time
    max_active_tasks=6,  # at most 6 running tasks across the whole run
    tags=["tutorial", "12-scaling"],
    doc_md=__doc__,
)
def pools_priority_concurrency():
    @task(pool="api_pool", priority_weight=1)
    def call_api(endpoint: str) -> str:
        print(f"Calling {endpoint}")
        time.sleep(8)
        return endpoint

    @task(
        pool="api_pool",
        priority_weight=100,  # higher = scheduled first when slots are scarce
        weight_rule=WeightRule.ABSOLUTE,  # use exactly this weight (don't add upstream/downstream)
    )
    def urgent_report() -> None:
        print("I was scheduled before the normal API calls")
        time.sleep(3)

    @task(pool="database_pool", pool_slots=2)  # takes 2 of the 4 slots
    def heavy_query(table: str) -> None:
        print(f"Running a heavy query on {table}")
        time.sleep(6)

    @task(max_active_tis_per_dag=1)  # never two of these at once, even across runs
    def write_to_single_file(part: int) -> None:
        print(f"Appending part {part} to a file that doesn't like concurrent writers")
        time.sleep(2)

    call_api.expand(endpoint=[f"/v1/items?page={i}" for i in range(1, 7)])
    urgent_report()
    heavy_query.expand(table=["orders", "customers", "products"])
    write_to_single_file.expand(part=[1, 2, 3])


pools_priority_concurrency()
