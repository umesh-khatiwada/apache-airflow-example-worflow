"""
# 09 · Assets — data-aware (event-driven) scheduling

**Concepts:** `Asset`, `outlets`, schedule on assets, `Metadata` / `outlet_events`
extras, `triggering_asset_events`, AND / OR conditions, `AssetOrTimeSchedule`,
the `@asset` decorator.

An **Asset** is a logical name for a piece of data (a table, a file, a model…).
A task that *updates* it lists it in `outlets`. Another DAG can use the asset as its
`schedule` and runs **automatically every time the asset is updated** — no cron needed.

This file defines several DAGs:

| DAG | What it shows |
|-----|---------------|
| `09_assets_producer_orders`    | updates `raw_orders` and attaches extra info |
| `09_assets_producer_customers` | updates `raw_customers` |
| `09_assets_consumer`           | runs whenever `raw_orders` is updated |
| `09_assets_consumer_and_or`    | runs when **both** are updated, **or** `manual_override` |
| `09_assets_or_time`            | runs daily **or** whenever `raw_orders` updates |
| `tutorial_daily_kpis`          | an `@asset` — a DAG + task + asset in one function |

**Try it:** unpause all of them, then trigger the producers and watch the consumers
start on their own. Browse **Assets** in the top menu to see the lineage graph.

⚠️ Airflow 3.4+: an asset-triggered DAG creates **one run per asset event** by default.

📖 docs/13-assets-data-aware-scheduling.md
"""

from __future__ import annotations

import json

import pendulum
from airflow.sdk import Asset, AssetOrTimeSchedule, CronTriggerTimetable, Metadata, asset, dag, task

from tutorial_lib.paths import DATA_DIR

START = pendulum.datetime(2026, 1, 1, tz="UTC")

# Asset definitions are usually shared in one module. `name` is what you see in the UI;
# `uri` identifies where the data lives (it is only an identifier — Airflow never reads it).
raw_orders = Asset(name="raw_orders", uri="file:///opt/airflow/data/assets/raw_orders.json")
raw_customers = Asset(name="raw_customers", uri="file:///opt/airflow/data/assets/raw_customers.json")
manual_override = Asset(name="manual_override")


# ------------------------------------------------------------------------------------
# Producers
# ------------------------------------------------------------------------------------
@dag(dag_id="09_assets_producer_orders", schedule=None, start_date=START, tags=["tutorial", "06-assets"])
def producer_orders():
    @task(outlets=[raw_orders])  # when this task SUCCEEDS, an asset event is created
    def write_orders(*, outlet_events=None):
        orders = [{"id": i, "amount": i * 10} for i in range(1, 6)]
        path = DATA_DIR / "assets" / "raw_orders.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(orders))
        # Attach extra metadata to the asset event (visible in the UI and to consumers).
        outlet_events[raw_orders].extra = {"row_count": len(orders), "path": str(path)}

    write_orders()


producer_orders()


@dag(dag_id="09_assets_producer_customers", schedule=None, start_date=START, tags=["tutorial", "06-assets"])
def producer_customers():
    @task(outlets=[raw_customers])
    def write_customers():
        customers = [{"id": "C001", "name": "Ada"}, {"id": "C002", "name": "Linus"}]
        path = DATA_DIR / "assets" / "raw_customers.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(customers))
        # Alternative way to attach extras: yield Metadata(...)
        yield Metadata(raw_customers, {"row_count": len(customers)})

    write_customers()


producer_customers()


# ------------------------------------------------------------------------------------
# Consumers
# ------------------------------------------------------------------------------------
@dag(
    dag_id="09_assets_consumer",
    schedule=[raw_orders],  # run every time raw_orders gets a new event
    start_date=START,
    tags=["tutorial", "06-assets"],
)
def consumer():
    @task
    def read_orders(*, triggering_asset_events=None):
        # Which events caused this run, and their extra info.
        for asset_ref, events in triggering_asset_events.items():
            for event in events:
                print(f"Triggered by {asset_ref} with extra={event.extra}")
        orders = json.loads((DATA_DIR / "assets" / "raw_orders.json").read_text())
        print(f"Total order amount: {sum(o['amount'] for o in orders)}")

    read_orders()


consumer()


@dag(
    dag_id="09_assets_consumer_and_or",
    # & = AND (wait until BOTH have new events), | = OR (any one is enough)
    schedule=((raw_orders & raw_customers) | manual_override),
    start_date=START,
    tags=["tutorial", "06-assets"],
)
def consumer_and_or():
    @task
    def join_orders_and_customers():
        print("Both orders and customers were refreshed -> building the joined table")

    join_orders_and_customers()


consumer_and_or()


@dag(
    dag_id="09_assets_or_time",
    # Run at 06:00 every day AND also whenever raw_orders is updated.
    schedule=AssetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 6 * * *", timezone="UTC"),
        assets=[raw_orders],
    ),
    start_date=START,
    tags=["tutorial", "06-assets"],
)
def asset_or_time():
    @task
    def refresh_dashboard(dag_run=None):
        print(f"Refreshing dashboard, run type = {dag_run.run_type}")

    refresh_dashboard()


asset_or_time()


# ------------------------------------------------------------------------------------
# @asset: one function = one DAG (id = function name) + one task + one asset it produces.
# Parameters named like other assets become *inlets* (dependencies).
# ------------------------------------------------------------------------------------
@asset(schedule=[raw_orders], tags=["tutorial", "06-assets"])
def tutorial_daily_kpis(self, context, raw_orders):
    """`self` is the asset being produced; `raw_orders` is the upstream asset."""
    events = context["inlet_events"][raw_orders]
    latest = events[-1] if events else None
    print(f"Building KPIs ({self.name}) from latest raw_orders event: {latest}")
    return {"kpi": "orders_total", "status": "ok"}
