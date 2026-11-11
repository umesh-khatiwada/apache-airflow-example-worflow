"""
# 17 · Dynamic DAG generation — many similar DAGs from one file

**Concepts:** generating DAGs in a loop from configuration, unique `dag_id`s,
avoiding expensive work at parse time.

One Python file can create many DAGs. Here a small config dict produces three
"ingest" DAGs, each with its own schedule and table list.

⚠️ Rules for generated DAGs:
1. Every `dag_id` must be unique.
2. The config must be **cheap to read** (a dict, a small YAML/JSON file in the repo).
   Do NOT call databases or APIs at the top level: the file is re-parsed every ~30 s.
3. Prefer *Dynamic Task Mapping* (example 06) when only the **number of tasks** varies;
   generate separate DAGs when schedules/owners/alerts differ.

📖 docs/21-dynamic-dag-generation.md
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

# In real projects this often comes from a YAML/JSON file next to the DAG.
SOURCES = {
    "crm": {"schedule": "0 1 * * *", "tables": ["contacts", "deals"], "owner": "sales-eng"},
    "erp": {"schedule": "0 2 * * *", "tables": ["invoices", "products", "suppliers"], "owner": "finance-eng"},
    "web": {"schedule": "@hourly", "tables": ["pageviews"], "owner": "growth-eng"},
}


def build_ingest_dag(source: str, config: dict):
    @dag(
        dag_id=f"17_generated_ingest_{source}",
        schedule=config["schedule"],
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        default_args={"owner": config["owner"]},
        tags=["tutorial", "13-patterns", f"source:{source}"],
        doc_md=f"Generated ingest DAG for **{source}** — see `17_dynamic_dag_generation.py`.",
    )
    def ingest():
        @task
        def extract(table: str) -> dict:
            print(f"[{source}] extracting {table}")
            return {"table": table, "rows": len(table) * 100}

        @task
        def load(results: list[dict]) -> None:
            for r in results:
                print(f"[{source}] loaded {r['rows']} rows into raw_{source}.{r['table']}")

        load(extract.expand(table=config["tables"]))

    return ingest()


for source_name, source_config in SOURCES.items():
    build_ingest_dag(source_name, source_config)
