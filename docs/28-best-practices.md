# Best practices for writing DAGs

**What you'll learn:** the rules experienced Airflow users follow so their pipelines are safe to re-run, fast to parse, easy to read and cheap to operate. Each rule explains *why* it matters and shows a short "bad" and "good" example, taken from or consistent with the DAGs in this project. The chapter ends with a checklist you can use when reviewing a new DAG.

> **Run it**
> There is no single DAG for this chapter. The best "good" examples live in:
> - [../dags/20_capstone_sales_etl.py](../dags/20_capstone_sales_etl.py) (`20_capstone_sales_etl`): atomic tasks, idempotent loads, data-quality gates
> - [../dags/10_params_variables_templating.py](../dags/10_params_variables_templating.py) (`10_params_variables_templating`): Variables read at runtime, static Param defaults
> - [../dags/17_dynamic_dag_generation.py](../dags/17_dynamic_dag_generation.py) (`17_generated_ingest_*`): cheap top-level code
> - [../tests/](../tests/): integrity and structure tests that enforce conventions

---

## The big picture

An Airflow DAG file is read in two very different situations:

1. **At parse time.** The [dag-processor](32-glossary.md#dag-processor) imports every file in `dags/` again and again (every 30 seconds in this project, see `AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL` in `docker-compose.yaml`). It only wants to know the *shape* of the DAG.
2. **At run time.** A worker runs one task: one function or one operator's `execute()`.

Most best practices come from keeping these two apart: **top-level code must be cheap and predictable; real work belongs inside tasks.** The rest are about making each task safe to run more than once.

```mermaid
flowchart LR
    subgraph parse["Parse time (every ~30 s)"]
        A["import dag file"] --> B["build DAG object<br/>(tasks + dependencies)"]
    end
    subgraph run["Run time (once per task try)"]
        C["worker runs task code<br/>(DB, API, files...)"]
    end
    B -- "serialized DAG in metadata DB" --> C
```

---

## 1. DAG design

### 1.1 Keep tasks atomic

**Rule:** one task = one logical step that either fully succeeds or fully fails. Prefer *extract → transform → load* as separate tasks over one function that does everything.

**Why:** when a step fails, Airflow retries only that task. If a single task downloads, cleans, loads and emails, a failure in the email step reruns the download too. Smaller tasks also give clearer logs and a readable Grid view.

```python
# Bad: one task does everything. A failure while loading re-downloads all files.
@task
def do_everything():
    rows = download_all_regions()
    clean = [fix(r) for r in rows]
    load_into_postgres(clean)
    send_email()
```

```python
# Good (from 20_capstone_sales_etl): separate, retryable steps
transformed = process_region.expand(region=REGIONS)  # locate/validate/read/clean, per region
loaded = load_to_postgres(transformed)                # one DB write
create_tables >> loaded
loaded >> build_summary >> checks >> publish(loaded, transformed)
```

Don't go too far the other way: a task per line of code adds scheduling overhead (each task is a separate process). "One step you'd want to retry on its own" is a good size.

### 1.2 Make every task idempotent

**Rule:** running a task twice for the same run must leave the world in the same state as running it once. This property is called [idempotency](32-glossary.md#idempotent).

**Why:** Airflow *will* run your task more than once: retries, a user clicking **Clear**, a backfill. A plain `INSERT` run twice gives duplicate rows.

```python
# Bad: a retry or a cleared task inserts the same rows again
hook.insert_rows(table="sales_clean", rows=rows, target_fields=[...])
```

```python
# Good (from 20_capstone_sales_etl, load_to_postgres): delete this batch, then insert
hook.run("DELETE FROM sales_clean WHERE batch_id = %(batch_id)s", parameters={"batch_id": run_id})
hook.insert_rows(table="sales_clean", rows=rows, target_fields=[..., "batch_id"])
```

The same pattern appears in [../dags/include/sql/build_sales_summary.sql](../dags/include/sql/build_sales_summary.sql) (`DELETE ... WHERE batch_id = ...` then `INSERT ... SELECT`). Other idempotent patterns:

| Pattern | Example |
|---|---|
| Delete-then-insert a partition | `DELETE WHERE batch_id = X; INSERT ...` (the capstone) |
| Upsert / merge | `INSERT ... ON CONFLICT (order_id) DO UPDATE ...` |
| Overwrite a whole file or partition | write `s3://bucket/sales/date=2026-11-01/part.parquet`, replacing it |
| Create-if-missing | `CREATE TABLE IF NOT EXISTS` ([../dags/include/sql/create_sales_tables.sql](../dags/include/sql/create_sales_tables.sql)) |

> **Choose the key carefully.** The capstone uses `run_id` as `batch_id`, so *re-running the same run* is safe. A *new* run (a different `run_id`) loads the rows again, which is fine for this demo because the sample CSVs never change. For a real daily pipeline, key the partition on the data you process, for example `{{ ds }}` or `data_interval_start`, so two runs for the same day replace each other instead of adding up.

Not every example in this repo is idempotent on purpose: `save_to_postgres` in [../dags/11_connections_and_hooks.py](../dags/11_connections_and_hooks.py) only inserts, so a retry of that task adds duplicate rows for the same `run_id`. Fixing it is an exercise in [33-exercises.md](33-exercises.md). The full walk-through of the capstone's loads is in [24-capstone-sales-etl.md](24-capstone-sales-etl.md).

### 1.3 Make tasks deterministic

**Rule:** the output of a task should depend only on its inputs and the run's dates, not on *when* it happened to run.

**Why:** if `transform` uses `datetime.now()` to decide which rows to process, a retry tomorrow processes different rows. A backfill for last month would process today's data.

```python
# Bad: "yesterday" depends on the wall clock at run time
@task
def extract():
    day = (datetime.now() - timedelta(days=1)).date()
    return query(f"SELECT * FROM orders WHERE order_date = '{day}'")
```

```python
# Good: use the run's data interval (stable across retries and backfills)
@dag(schedule=CronDataIntervalTimetable("@daily", timezone="UTC"), ...)  # a data-interval timetable
def orders_daily():
    @task
    def extract(data_interval_start=None, data_interval_end=None):
        hook = PostgresHook(postgres_conn_id="demo_postgres")
        return hook.get_records(
            "SELECT order_id, amount FROM orders WHERE created_at >= %(start)s AND created_at < %(end)s",
            parameters={"start": data_interval_start, "end": data_interval_end},
        )
```

Watch out: in Airflow 3 a plain cron string like `schedule="@daily"` uses `CronTriggerTimetable`, whose data interval has **zero length** (start == end). The query above would then return nothing. Either use a data-interval timetable as shown (or `CronTriggerTimetable(..., interval=timedelta(days=1))`), or compute the window from `logical_date`, for example `logical_date.subtract(days=1)`. [18-scheduling-and-timetables.md](18-scheduling-and-timetables.md) explains the difference.

Even randomness can be deterministic. The offline fallback in [../plugins/tutorial_lib/hooks/open_meteo.py](../plugins/tutorial_lib/hooks/open_meteo.py) seeds its random generator with the coordinates, so the same city always gets the same fake numbers (and `tests/test_custom_components.py` checks this):

```python
rng = random.Random(f"{latitude},{longitude}")  # same city -> same numbers
```

> Manually triggered and asset-triggered runs may have **no** data interval (`logical_date` is `None`). Decide what the task should do in that case, as `is_business_day` does in [../dags/05_branching_and_trigger_rules.py](../dags/05_branching_and_trigger_rules.py). See [30-troubleshooting-faq.md](30-troubleshooting-faq.md).

### 1.4 Keep XComs small

**Rule:** pass *references* (paths, table names, IDs, counts) between tasks, not datasets.

**Why:** [XComs](32-glossary.md#xcom) are stored in the metadata database. Megabytes of JSON per task slow down the database, the API and the UI for everyone.

```python
# Bad: returns a whole DataFrame's worth of rows
@task
def extract() -> list[dict]:
    return hook.get_records("SELECT * FROM events")  # millions of rows
```

```python
# Good: write the data somewhere, return where it is
@task
def extract(ds=None) -> str:
    path = DATA_DIR / "events" / f"events_{ds}.parquet"
    write_parquet(query_events(ds), path)
    return str(path)
```

The capstone's `read_csv` returns rows directly, with a comment explaining why that's acceptable there: `# small data -> fine for XCom. Big data -> write a file, return its path.` The sample CSVs have about a dozen rows each.

Two more things to know:

- A `@task` whose return annotation is `dict` (for example `def transform(rows) -> dict:`) automatically gets `multiple_outputs=True`, so each key is **also** stored as its own XCom. Be explicit (`@task(multiple_outputs=False)`) for big dicts you only pass as a whole.
- If you really must pass larger objects, configure an object-storage XCom backend (the `common.io` provider's `XComObjectStorageBackend`, settings under `[common.io] xcom_objectstorage_*`). Values above a size threshold go to S3/GCS/local storage and only a reference lands in the database.

More in [08-xcom.md](08-xcom.md).

---

## 2. Fast, safe parsing

### 2.1 No heavy work at the top level

**Rule:** module-level code (outside tasks) must be fast: imports, constants, the DAG definition. No network calls, database queries, big file reads or `time.sleep`.

**Why:** the dag-processor re-imports the file every 30 seconds here (5 minutes by default). Slow files delay every DAG, and if importing takes longer than `[core] dagbag_import_timeout` (30 s by default) the file fails with an import error and its DAGs disappear.

```python
# Bad: an HTTP call at import time, repeated every 30 s, forever
import requests
CITIES = requests.get("https://example.com/cities").json()

@dag(...)
def weather():
    ...
```

```python
# Good (from 11_connections_and_hooks): a small static list at the top...
CITIES = [
    {"city": "Kathmandu", "latitude": 27.71, "longitude": 85.32},
    {"city": "London", "latitude": 51.51, "longitude": -0.13},
    {"city": "New York", "latitude": 40.71, "longitude": -74.01},
]
# ...and if the list must be dynamic, fetch it INSIDE a task and use .expand()
```

The same applies to custom operators. [../plugins/tutorial_lib/operators/weather.py](../plugins/tutorial_lib/operators/weather.py) says it in its docstring: `__init__` should only store arguments, because it runs every time the DAG file is parsed; the network call lives in `execute()`.

Heavy *imports* count too. If only one task needs `pandas` or a big SDK, import it inside that task, as the capstone does with `from airflow.sdk import get_current_context` inside `transform`.

### 2.2 No `Variable.get()`, DB or API calls at parse time

**Rule:** read [Variables](32-glossary.md#variable) and [Connections](32-glossary.md#connection) inside tasks or through Jinja templates, never at the top of the file.

**Why:** each top-level `Variable.get()` is a lookup on *every parse*, not once per run: with a 30-second refresh that's thousands of lookups a day per call, against the database or a paid secrets backend. If the lookup is slow or the backend is unreachable, the whole file fails to import and its DAGs disappear. It also bakes the value into the DAG structure at parse time, so a change to the Variable only takes effect after the next parse.

```python
# Bad: runs at parse time
from airflow.sdk import Variable
ENVIRONMENT = Variable.get("environment")
```

```python
# Good (from 10_params_variables_templating): read at RUN time inside a task...
@task
def use_params_and_variables(params=None) -> dict:
    environment = Variable.get("environment", default="unknown")
    settings = Variable.get("tutorial_settings", default={}, deserialize_json=True)
    ...

# ...or in a template field, rendered just before the task runs
BashOperator(task_id="show_env", bash_command="echo {{ var.value.environment }}")
```

Ruff can catch this: rule `AIR003` (`airflow-variable-get-outside-task`, a preview rule at the time of writing) flags `Variable.get` outside a task.

### 2.3 Use a static `start_date`

**Rule:** `start_date` is a fixed, timezone-aware date.

**Why:** the scheduler computes run dates from `start_date`. If it changes on every parse (`datetime.now()`), the first interval keeps moving and runs may never be created, or are created at odd times.

```python
# Bad
from datetime import datetime
@dag(start_date=datetime.now(), schedule="@daily")
```

```python
# Good (every DAG in this repo)
@dag(start_date=pendulum.datetime(2026, 1, 1, tz="UTC"), schedule="@daily", catchup=False)
```

`airflow.utils.dates.days_ago()` was the old shortcut for "dynamic start date"; it no longer exists in Airflow 3. See [31-airflow-2-vs-3.md](31-airflow-2-vs-3.md).

### 2.4 Avoid dynamic defaults

**Rule:** the same goes for any value baked into the DAG at parse time: Param defaults, `default_args`, task ids, generated DAG ids.

**Why:** a default computed with `today()` is frozen at the moment of the last parse, not the moment of the run. It also makes the DAG look "changed" on every parse, which creates a new DAG version over and over.

```python
# Bad: the default is whatever day the dag-processor last parsed the file
"report_date": Param(str(date.today()), type="string", format="date")
```

```python
# Good (from 10_params_variables_templating)
# Defaults must be static: never compute them with today()/now() at parse time.
"report_date": Param("2026-01-01", type="string", format="date", title="Report date"),
```

If you want "today" as a fallback, compute it at run time inside the task (`params["report_date"] or ds`). Ruff rule `AIR304` (`airflow3-dag-dynamic-value`, preview) catches some of these cases.

---

## 3. Naming, tagging and layout

### 3.1 Naming conventions

| Thing | Convention | Examples |
|---|---|---|
| `dag_id` | lowercase `snake_case`, unique, says *what* and *how often* | `sales_daily_etl`, `crm_contacts_hourly_ingest` |
| `task_id` | a verb phrase: what the task does | `extract_orders`, `load_to_postgres`, `wait_for_file` |
| Task group id | a noun for the stage | `extract`, `quality_checks` |
| Connection id | system + purpose | `demo_postgres`, `open_meteo_api` |
| Pool | resource being protected | `api_pool`, `database_pool` |

This project adds one rule for learners: **each `dag_id` starts with the number of its file** (`01_hello_airflow` in `01_hello_airflow.py`). `test_dag_ids_match_their_file_number` in [../tests/test_dag_integrity.py](../tests/test_dag_integrity.py) enforces it. In real projects, prefer a domain prefix (`sales_`, `finance_`) instead of numbers.

Avoid renaming a `dag_id` once it has history: Airflow treats the new id as a brand-new DAG and the old runs stay under the old name.

### 3.2 Tags and owners

[Tags](32-glossary.md#tag) are filters in the UI's DAG list. Use a small, agreed vocabulary: team, domain, layer, criticality. Every DAG here has `"tutorial"` plus a topic tag, and a test checks it:

```python
tags=["tutorial", "15-capstone"],                # 20_capstone_sales_etl
tags=["tutorial", "13-patterns", f"source:{source}"],  # 17_generated_ingest_*
```

Set `owner` in `default_args` (`"owner": "data-platform"` in the capstone) so people know who to ask when a DAG breaks.

### 3.3 Folder layout

Keep DAG files thin, and put reusable code in an importable package. This project's layout is a good template:

```text
dags/
  01_hello_airflow.py ... 20_capstone_sales_etl.py   # DAG files: wiring only
  include/
    sql/        # .sql files used through template_searchpath
    data/       # small sample files shipped with the repo
  .airflowignore  # stops the dag-processor scanning include/, data, sql
plugins/
  tutorial_lib/   # hooks/, operators/, sensors/, callbacks.py, notifiers.py, paths.py
  tutorial_plugin.py  # only what Airflow itself must register (macros, extra links)
config/
  pools.json      # pools as code, imported by airflow-init
tests/            # integrity, structure, unit and integration tests
```

Rules of thumb:

- One DAG file per pipeline (or per generated family, like `17_*`).
- SQL in `.sql` files, not long strings in Python. They get syntax highlighting and are easy to diff.
- Shared Python in a package (`plugins/tutorial_lib`), imported with a normal `import`. You only need an [Airflow plugin](32-glossary.md#plugin) for things Airflow itself must know about (macros, UI links, listeners, timetables). See [17-custom-operators-hooks-plugins.md](17-custom-operators-hooks-plugins.md).
- List non-DAG folders in `.airflowignore` so the dag-processor doesn't waste time on them.

---

## 4. Secrets and configuration

### 4.1 Keep secrets in connections or a secrets backend

**Rule:** no passwords, tokens or hostnames in DAG code. Reference a [connection](32-glossary.md#connection) id.

**Why:** code ends up in Git, in logs, in screenshots. Connections can be changed without a deploy, and Airflow masks their passwords in task logs.

```python
# Bad
conn = psycopg.connect("postgresql://airflow:airflow@postgres:5432/demo")
```

```python
# Good (from 11_connections_and_hooks)
hook = PostgresHook(postgres_conn_id="demo_postgres")
```

Where the connection is *defined* depends on the environment:

| Environment | Where connections live |
|---|---|
| This project (local) | env vars `AIRFLOW_CONN_DEMO_POSTGRES=...` in `docker-compose.yaml` / `scripts/local_env.sh` |
| Small team server | the metadata DB (Admin ▸ Connections), encrypted with a [Fernet key](32-glossary.md#fernet-key) |
| Production | a **secrets backend** (HashiCorp Vault, AWS Secrets Manager, GCP Secret Manager, Azure Key Vault) configured under `[secrets] backend` |

The `.env` file is in `.gitignore` for the same reason. See [15-connections-and-hooks.md](15-connections-and-hooks.md) and the security section of [29-executors-and-production.md](29-executors-and-production.md).

### 4.2 Variables are for configuration, not data

Use Variables for small settings (an environment name, a feature flag, a row limit), like `environment` and `tutorial_settings` here. Don't use them as a cache or to pass data between tasks (that's what XCom and storage are for), and don't put secrets in them unless they come from a secrets backend.

---

## 5. Dependencies and environments

### 5.1 Pin dependencies with Airflow's constraints file

**Rule:** install Airflow and its providers with the official **constraints file** for your Airflow and Python version.

**Why:** Airflow has hundreds of transitive dependencies. Without constraints, `pip` may pick a newer library that Airflow wasn't tested with, and things break on the next rebuild even though you changed nothing.

```bash
# Bad: whatever versions pip picks today
pip install apache-airflow apache-airflow-providers-postgres
```

```bash
# Good (what `make venv` does)
uv pip install -r requirements-dev.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.12.txt
```

In Docker, extend the official image and repeat the Airflow version so pip can't change it, as the project's [../Dockerfile](../Dockerfile) does:

```dockerfile
FROM apache/airflow:3.4.0
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
```

`_PIP_ADDITIONAL_REQUIREMENTS` in `docker-compose.yaml` installs packages at *every container start*. It's handy for a quick experiment, but not something to rely on.

### 5.2 Isolate heavy or conflicting dependencies

**Rule:** if a task needs libraries that conflict with Airflow's (an old `numpy`, a huge ML stack, a different Python), run it in its own environment instead of installing it into the Airflow image.

| Option | Where the code runs | Needs | Use when |
|---|---|---|---|
| `@task.virtualenv(requirements=[...])` | a new virtualenv created on the worker (can be cached with `venv_cache_path`) | `standard` provider (installed) | a few extra pure-Python packages |
| `@task.external_python(python="/path/bin/python")` | a virtualenv you pre-built in the image | `standard` provider | big or slow-to-install stacks; no install at runtime |
| `@task.docker` / `DockerOperator` | a Docker container | `docker` provider + Docker daemon access | you already ship the code as an image |
| `KubernetesPodOperator` / `@task.kubernetes` | a pod on Kubernetes | `cncf.kubernetes` provider + a cluster | production-grade isolation, per-task CPU/memory |

```python
# Bad: adding scikit-learn==1.3 (and its pinned numpy) to the Airflow image for one task
```

```python
# Good: isolate it. Only arguments and the return value cross the boundary.
@task.virtualenv(requirements=["scikit-learn==1.5.2"], system_site_packages=False)
def train(rows: list[dict]) -> float:
    from sklearn.linear_model import LinearRegression  # import inside the function
    ...
    return score
```

Functions run this way can't use closures or globals from the DAG file, and their arguments/return values must be serializable.

---

## 6. Running reliably

### 6.1 Use retries sensibly

**Rule:** set a small number of retries with a delay for tasks that touch the network or other systems. Fail fast when retrying can't help.

**Why:** most failures in data pipelines are temporary (a timeout, a locked table). Retrying immediately and forever hammers a struggling system; retrying a typo in a config wastes time.

```python
# Bad: retries every 0 seconds, 50 times, even for a wrong password
@task(retries=50, retry_delay=timedelta(seconds=0))
```

```python
# Good (from 12_error_handling_and_alerts default_args)
"retries": 3,
"retry_delay": timedelta(seconds=5),
"retry_exponential_backoff": 2.0,          # 5 s, 10 s, 20 s...
"max_retry_delay": timedelta(minutes=1),   # ...capped
"execution_timeout": timedelta(minutes=10),
```

And for errors that will never fix themselves, either raise `AirflowFailException` (as `CsvQualityCheckOperator` does when the file is missing) or use a retry policy that decides per exception type:

```python
SMART_RETRY_POLICY = ExceptionRetryPolicy(
    rules=[
        RetryRule(exception=TemporaryNetworkError, action=RetryAction.RETRY, retry_delay=timedelta(seconds=5)),
        RetryRule(exception=BadCredentialsError, action=RetryAction.FAIL, reason="Credentials are wrong"),
    ]
)
```

Always set `execution_timeout` (or `dagrun_timeout` on the DAG) so a hung task doesn't hold a slot forever. Details in [16-error-handling-retries-alerts.md](16-error-handling-retries-alerts.md).

### 6.2 Use pools to protect shared resources

**Rule:** when many tasks hit the same external system, put them in a [pool](32-glossary.md#pool) sized to what that system can handle.

**Why:** `parallelism` and `max_active_tasks` limit Airflow; they don't know that your API allows 2 concurrent calls or your database falls over at 4 heavy queries.

```python
# Good (from 11 and 15): at most 2 API calls at once, across ALL DAGs
@task(pool="api_pool", retries=2)
def fetch_weather(city: str, latitude: float, longitude: float) -> dict: ...
```

Keep pools in code (`config/pools.json`, imported by `airflow-init`) so every environment has the same ones. Use `max_active_tis_per_dag` for "never two of these at once" and `priority_weight` for "this one first". See [19-pools-priority-concurrency.md](19-pools-priority-concurrency.md).

### 6.3 Don't let sensors hog worker slots

A sensor in `mode="poke"` holds a worker slot for its whole wait. For waits longer than a few minutes use `mode="reschedule"` or, better, `deferrable=True`, which hands the wait to the [triggerer](32-glossary.md#triggerer). Always set a `timeout`. See [12-sensors-and-deferrable.md](12-sensors-and-deferrable.md).

### 6.4 Add data-quality gates before you publish

**Rule:** check the data *before* downstream consumers see it, and fail the run if it's wrong.

**Why:** a pipeline that "succeeds" with empty or duplicated data is worse than one that fails loudly. Bad data spreads quietly into dashboards and models.

```python
# Bad: load, then immediately tell everyone the data is ready
loaded >> publish()
```

```python
# Good (from 20_capstone_sales_etl): checks sit between load and publish
loaded >> build_summary >> checks >> publish(loaded, transformed)

SQLColumnCheckOperator(
    task_id="column_checks", conn_id=CONN_ID, table="sales_clean",
    partition_clause="batch_id = '{{ run_id }}'",
    column_mapping={
        "order_id": {"null_check": {"equal_to": 0}, "unique_check": {"equal_to": 0}},
        "quantity": {"min": {"greater_than": 0}},
    },
)
```

Check at two levels: the raw input (`CsvQualityCheckOperator` validates columns and row counts before reading) and the loaded output (SQL checks). Because `publish` updates the `sales_summary` [asset](32-glossary.md#asset), a failed check also stops `20_capstone_sales_report` from running on bad data. `test_capstone_has_quality_gate_before_publish` in `tests/test_dag_structure.py` makes sure nobody removes the gate by accident.

---

## 7. Testing and CI

**Rule:** every change to `dags/` or `plugins/` runs automated tests before it reaches the Airflow that runs your pipelines.

**Why:** a syntax error in one DAG file makes that file's DAGs vanish from the UI. You want to find out in CI, not from a missed report.

The test pyramid in this repo ([27-testing-dags.md](27-testing-dags.md) explains each file):

| Level | File | Catches |
|---|---|---|
| DAG integrity | `tests/test_dag_integrity.py` | import errors, missing DAGs, missing tags, naming conventions |
| Structure | `tests/test_dag_structure.py` | changed dependencies, trigger rules, retries, pools |
| Unit | `tests/test_custom_components.py` | logic bugs in operators, sensors, hooks, macros |
| Integration | `tests/test_dag_runs.py` (`pytest -m integration`) | DAGs that parse but fail when run |

A minimal CI job:

```bash
uv pip install -r requirements-dev.txt --constraint "$CONSTRAINTS_URL"
ruff check dags plugins tests          # pyproject.toml selects the AIR rules too
pytest -q tests -m "not integration"   # fast checks on every push
```

Keep task logic in plain functions or operator `execute()` methods so you can unit-test them without a scheduler, as `test_taskflow_transform_logic` does by calling `transform`'s `python_callable` directly.

---

## 8. Code review checklist for a new DAG

Copy this into your pull-request template.

| # | Check | Why | Where to look in this repo |
|---|---|---|---|
| 1 | `dag_id` is unique, descriptive and follows the naming convention | renames lose history; duplicates hide each other | `tests/test_dag_integrity.py` |
| 2 | `start_date` is static and timezone-aware; `catchup` set on purpose | moving start dates break scheduling | every DAG |
| 3 | `schedule` is right (cron / preset / timetable / asset / `None`) and documented | wrong cadence = wrong data | [18-scheduling-and-timetables.md](18-scheduling-and-timetables.md) |
| 4 | No network/DB calls, `Variable.get()` or heavy imports at module level | slow parsing, import timeouts | `17_dynamic_dag_generation.py` |
| 5 | Tasks are atomic and idempotent (re-run safe) | retries and clears happen | `load_to_postgres` in `20_*` |
| 6 | Tasks use the run's dates, not `now()` | backfills and retries | §1.3 |
| 7 | XComs are small (paths, ids, counts) | metadata DB health | `04_xcom.py` |
| 8 | Credentials come from connections; no secrets in code or logs | security | `11_connections_and_hooks.py` |
| 9 | Retries, `retry_delay` and `execution_timeout`/`dagrun_timeout` are set | transient failures, hung tasks | `12_error_handling_and_alerts.py` |
| 10 | Failure alerting is configured (callback / notifier / deadline alert) | someone must notice | `tutorial_lib/callbacks.py` |
| 11 | Shared resources are protected by pools; sensors are deferrable or reschedule | slot starvation | `15_*`, `08_*` |
| 12 | Data-quality checks run before data is published | bad data spreads | `quality_checks` in `20_*` |
| 13 | `owner`, `tags` and `doc_md` are filled in | discoverability, on-call | every DAG |
| 14 | New Python packages are added to `requirements.txt` and installed with constraints (or isolated) | reproducible images | `Dockerfile` |
| 15 | Tests updated/added and CI is green, including `ruff check` | catches breakage early | `tests/` |
| 16 | The DAG was run end to end (`airflow dags test` or in a dev environment) | parse ≠ works | [27-testing-dags.md](27-testing-dags.md) |

---

## Common mistakes

- **Treating the DAG file like a script.** Code at the top level runs at every parse, not once per run. Move work into tasks.
- **Using `datetime.now()` for "which data".** Use `data_interval_start`/`data_interval_end` or `ds` so retries and backfills process the same slice.
- **Plain `INSERT` loads.** They duplicate rows on retry. Delete-then-insert, upsert or overwrite instead.
- **Pushing big data through XCom.** Write it to storage and pass the path.
- **Installing packages without constraints**, then wondering why a rebuild broke Airflow.
- **Hardcoding credentials** "just for now". They end up in Git history.

## Try it yourself

1. Make `save_to_postgres` in [../dags/11_connections_and_hooks.py](../dags/11_connections_and_hooks.py) idempotent: delete rows for the current `run_id` before inserting. Clear the task twice and confirm the row count doesn't grow (`make psql`, then `SELECT run_id, COUNT(*) FROM weather_observations GROUP BY run_id;`).
2. Add a deliberate top-level `time.sleep(40)` to a copy of `01_hello_airflow.py` (in a new file). Watch the import error appear in the UI and with `airflow dags list-import-errors`. Then delete the file.
3. Add a third SQL check to `quality_checks` in the capstone, for example `"no_future_dates": {"check_statement": "MAX(order_date) <= CURRENT_DATE"}`.
4. Run `uvx ruff check --select AIR --preview dags plugins` and read what each finding means.

---

**Next →** [29-executors-and-production.md](29-executors-and-production.md) · [Back to the index](README.md)
