"""
# 20 · Capstone — a production-style sales ETL pipeline

Everything from the previous examples, combined into one realistic pipeline.

```
┌─ process_region (task group, mapped: one copy per region) ─────────────────────┐
│  locate_file ─► validate_raw_file (custom operator) ─► read_csv ─► transform   │
└──────────────────────────────────────────────────────────────────────────────────┘
     │  (all regions' results collected into one list)
     ▼                          create_tables (runs in parallel)
     │ ◄────────────────────────────────┘
     ▼
load_to_postgres (idempotent: delete batch, then insert)
     │
     ▼
build_summary (SQL file)
     │
     ▼
┌─ quality_checks (task group) ──────┐
│  column_checks   table_checks      │
└────────────────────────────────────┘
     │
     ▼
publish (updates Asset `sales_summary`) ═══► 20_capstone_sales_report (asset-scheduled)
```

**Features used:** TaskFlow, mapped task groups, custom operator, SQL files with
parameters, PostgresHook, idempotent loads, SQL data-quality checks, retries +
callbacks, pools, Asset with extra metadata, an asset-triggered downstream DAG.

**Requires** the `demo` Postgres database (docker compose). Inspect results with
`make psql` → `SELECT * FROM sales_daily_summary ORDER BY batch_id DESC LIMIT 10;`

📖 docs/24-capstone-sales-etl.md
"""

from __future__ import annotations

import csv
import re
from datetime import timedelta
from decimal import Decimal

import pendulum
from airflow.providers.common.sql.operators.sql import (
    SQLColumnCheckOperator,
    SQLExecuteQueryOperator,
    SQLTableCheckOperator,
)
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Asset, dag, task, task_group

from tutorial_lib.callbacks import task_failure_alert
from tutorial_lib.operators.csv_quality import CsvQualityCheckOperator
from tutorial_lib.paths import DATA_DIR, SAMPLE_DATA_DIR, SQL_DIR

CONN_ID = "demo_postgres"
REGIONS = ["north", "south", "west"]
REQUIRED_COLUMNS = ["order_id", "order_date", "region", "product", "quantity", "unit_price", "customer_id"]

sales_summary = Asset(name="sales_summary", uri="postgres://postgres:5432/demo/public/sales_daily_summary")

def clean_rows(rows: list[dict]) -> dict:
    """
    Business logic as a plain function (no Airflow imports) so it's easy to unit test.
    Drops rows with quantity <= 0 and computes revenue = quantity * unit_price.
    """
    clean, rejected = [], []
    for row in rows:
        quantity = int(row["quantity"])
        if quantity <= 0:
            rejected.append(row["order_id"])
            continue
        unit_price = Decimal(row["unit_price"])
        clean.append(
            {
                "order_id": int(row["order_id"]),
                "order_date": row["order_date"],
                "region": row["region"].strip().lower(),
                "product": row["product"].strip(),
                "quantity": quantity,
                "unit_price": str(unit_price),
                "revenue": str(unit_price * quantity),
                "customer_id": row["customer_id"] or None,
            }
        )
    region = clean[0]["region"] if clean else "unknown"
    return {"region": region, "rows": clean, "rejected": rejected}


default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(seconds=20),
    "on_failure_callback": task_failure_alert,
}


@dag(
    dag_id="20_capstone_sales_etl",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=30),
    template_searchpath=[str(SQL_DIR)],
    tags=["tutorial", "15-capstone"],
    doc_md=__doc__,
)
def capstone_sales_etl():
    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id=CONN_ID,
        sql="create_sales_tables.sql",
        split_statements=True,
    )

    # ---- Extract + validate + transform: one copy of the group per region ----------
    # Everything for one region lives in ONE mapped task group. Downstream tasks then
    # receive the group's result for all regions as a list.
    @task_group(tooltip="Locate, validate, read and clean one regional CSV")
    def process_region(region: str):
        @task
        def locate_file(region: str) -> str:
            # In real life: find today's file in S3/SFTP. Here: the bundled sample CSVs.
            return str(SAMPLE_DATA_DIR / f"sales_{region}.csv")

        path = locate_file(region)

        # A task's output (XComArg) can be passed straight into a classic operator.
        validate = CsvQualityCheckOperator(
            task_id="validate_raw_file",
            file_path=path,
            required_columns=REQUIRED_COLUMNS,
            not_null_columns=["order_id", "product"],
            min_rows=1,
        )

        @task
        def read_csv(path: str) -> list[dict]:
            with open(path, newline="") as f:
                rows = list(csv.DictReader(f))
            print(f"Read {len(rows)} rows from {path}")
            return rows  # small data -> fine for XCom. Big data -> write a file, return its path.

        @task
        def transform(rows: list[dict]) -> dict:
            result = clean_rows(rows)
            print(f"{result['region']}: kept {len(result['rows'])} rows, rejected {result['rejected']}")
            return result

        rows = read_csv(path)
        validate >> rows
        return transform(rows)

    # ---- Load: idempotent (safe to re-run) ---------------------------------------------
    @task(pool="database_pool")
    def load_to_postgres(results: list[dict], run_id=None) -> int:
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        rows = [
            (
                r["order_id"], r["order_date"], r["region"], r["product"], r["quantity"],
                r["unit_price"], r["revenue"], r["customer_id"], run_id,
            )
            for result in results
            for r in result["rows"]
        ]
        # Delete anything this run loaded before (e.g. on a retry), then insert.
        hook.run("DELETE FROM sales_clean WHERE batch_id = %(batch_id)s", parameters={"batch_id": run_id})
        hook.insert_rows(
            table="sales_clean",
            rows=rows,
            target_fields=[
                "order_id", "order_date", "region", "product", "quantity",
                "unit_price", "revenue", "customer_id", "batch_id",
            ],
        )
        print(f"Loaded {len(rows)} rows for batch {run_id}")
        return len(rows)

    build_summary = SQLExecuteQueryOperator(
        task_id="build_summary",
        conn_id=CONN_ID,
        sql="build_sales_summary.sql",
        parameters={"batch_id": "{{ run_id }}"},
        split_statements=True,
    )

    # ---- Data quality: fail the run if the data looks wrong --------------------------
    @task_group(tooltip="SQL data quality checks on this batch")
    def quality_checks():
        batch_filter = "batch_id = '{{ run_id }}'"
        SQLColumnCheckOperator(
            task_id="column_checks",
            conn_id=CONN_ID,
            table="sales_clean",
            partition_clause=batch_filter,
            column_mapping={
                "order_id": {"null_check": {"equal_to": 0}, "unique_check": {"equal_to": 0}},
                "quantity": {"min": {"greater_than": 0}},
                "revenue": {"min": {"geq_to": 0}},
            },
        )
        SQLTableCheckOperator(
            task_id="table_checks",
            conn_id=CONN_ID,
            table="sales_clean",
            partition_clause=batch_filter,
            checks={
                "has_rows": {"check_statement": "COUNT(*) > 0"},
                "revenue_matches": {"check_statement": "SUM(revenue) = SUM(quantity * unit_price)"},
            },
        )

    # ---- Publish: emit an asset event so downstream DAGs start --------------------------
    @task(outlets=[sales_summary])
    def publish(loaded_rows: int, transformed: list[dict], run_id=None, *, outlet_events=None) -> None:
        rejected = sum(len(t["rejected"]) for t in transformed)
        outlet_events[sales_summary].extra = {"batch_id": run_id, "rows": loaded_rows, "rejected": rejected}
        print(f"Published batch {run_id}: {loaded_rows} rows loaded, {rejected} rejected")

    # ---- Wiring ---------------------------------------------------------------------------
    transformed = process_region.expand(region=REGIONS)  # list of 3 results downstream
    loaded = load_to_postgres(transformed)
    checks = quality_checks()
    create_tables >> loaded  # tables must exist before loading; extraction can run in parallel
    loaded >> build_summary >> checks >> publish(loaded, transformed)


capstone_sales_etl()


# ======================================================================================
# Downstream DAG: runs automatically whenever `sales_summary` is updated.
# ======================================================================================
@dag(
    dag_id="20_capstone_sales_report",
    schedule=[sales_summary],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    tags=["tutorial", "15-capstone"],
    doc_md="Builds a Markdown report every time `20_capstone_sales_etl` publishes `sales_summary`.",
)
def capstone_sales_report():
    @task
    def get_batch_id(*, triggering_asset_events=None) -> str:
        events = triggering_asset_events[sales_summary]
        extra = events[-1].extra
        print(f"Triggered by batch {extra['batch_id']} ({extra['rows']} rows)")
        return extra["batch_id"]

    @task
    def write_report(batch_id: str) -> str:
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        records = hook.get_records(
            "SELECT region, orders, units, revenue FROM sales_daily_summary "
            "WHERE batch_id = %(batch_id)s ORDER BY revenue DESC",
            parameters={"batch_id": batch_id},
        )
        lines = [
            f"# Sales report — batch `{batch_id}`",
            "",
            "| Region | Orders | Units | Revenue |",
            "|---|---|---|---|",
        ]
        lines += [f"| {r} | {o} | {u} | {rev:,.2f} |" for r, o, u, rev in records]
        lines += ["", f"**Total revenue:** {sum(rec[3] for rec in records):,.2f}"]
        report = "\n".join(lines)

        safe_name = re.sub(r"[^A-Za-z0-9_-]", "_", batch_id)
        path = DATA_DIR / "reports" / f"sales_report_{safe_name}.md"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(report)
        print(report)
        return str(path)

    write_report(get_batch_id())


capstone_sales_report()
