# Core concepts

**What you'll learn:** the vocabulary of Airflow — about twenty words you will meet everywhere —
each explained in a few lines, with a pointer to the chapter and the example DAG that goes deeper.
Read it once now; come back to it whenever a word is unclear.

> **Run it**
> Every concept below has an example. The [map of the examples](#map-of-the-examples) at the end
> tells you which DAG file to open. A good first pair: `01_hello_airflow`
> ([../dags/01_hello_airflow.py](../dags/01_hello_airflow.py), classic style) and `02_taskflow_api`
> ([../dags/02_taskflow_api.py](../dags/02_taskflow_api.py), TaskFlow style).

---

## The building blocks

### DAG

A **DAG** (Directed Acyclic Graph) is one workflow: a set of tasks plus the arrows (dependencies)
between them, with no cycles. It also carries settings such as its `schedule`, `start_date`, tags and
default arguments for its tasks. A DAG is defined in a Python file in `dags/` with
`with DAG(...)` or the `@dag` decorator, both imported from `airflow.sdk`. Each DAG has a unique
`dag_id`.
→ [05 · Your first DAG](05-first-dag.md)

### Task

A **task** is one node of the DAG — one unit of work, such as "run this SQL" or "call this
function". Each task has a `task_id` that is unique inside its DAG. Tasks are connected with `>>`
("runs before") and `<<`, or automatically by passing one TaskFlow task's output into another.
→ [07 · Operators & dependencies](07-operators-and-dependencies.md)

### Operator

An **operator** is a *template* for a kind of task; you create a task by instantiating it with
arguments. There are three broad families:

| Kind | Does what | Examples in this project |
|---|---|---|
| **Action** operators | Do something | `BashOperator`, `PythonOperator`, `EmptyOperator`, `SQLExecuteQueryOperator`, `TriggerDagRunOperator` |
| **Transfer** operators | Move data from system A to system B | Not used here; providers ship many (e.g. "S3 to Redshift", "GCS to BigQuery") |
| **Sensors** | Wait until something is true | `FileSensor`, `TimeDeltaSensor`, the custom `CsvHasRowsSensor` |

Most operators come from **providers** (see below). Standard ones live in
`airflow.providers.standard.operators.*` and `airflow.providers.standard.sensors.*`.
→ [07 · Operators & dependencies](07-operators-and-dependencies.md),
[12 · Sensors & deferrable](12-sensors-and-deferrable.md),
[17 · Custom operators](17-custom-operators-hooks-plugins.md)

### TaskFlow vs classic style

Two ways to write the same thing. **Classic**: instantiate operator classes and wire them with `>>`.
**TaskFlow**: decorate Python functions with `@task`; calling one with another's return value
creates the dependency *and* passes the data (via XCom). They mix freely in one DAG.

```python
# Classic — from 01_hello_airflow.py
print_context = PythonOperator(task_id="print_context", python_callable=_print_context)
start >> say_hello >> print_context >> end

# TaskFlow — from 02_taskflow_api.py
orders = extract()
summary = transform(orders)
message = load(summary["total"], summary["count"])
```

→ [06 · TaskFlow API](06-taskflow-api.md)

## Runs and time

### DAG run

A **DAG run** is one execution of a DAG. It is created by the scheduler (a *scheduled* run), by you
(a *manual* run: Trigger button, CLI, REST API), by an asset update (*asset_triggered*), or by a
backfill. Each has a `run_id`, a state (`queued`, `running`, `success`, `failed`) and an optional
`conf` dictionary.
→ [18 · Scheduling](18-scheduling-and-timetables.md)

### Task instance

A **task instance** is one task inside one DAG run — for example `say_hello` in the run for
2026-11-11. It has a state (`scheduled`, `running`, `success`, `failed`, `up_for_retry`, …), a
`try_number`, and its own log. In the UI each square of the Grid view is a task instance.
→ [02 · Architecture — task states](02-architecture.md#task-instance-states)

### Logical date, data interval and `run_after`

These three timestamps are the most confusing part of Airflow, so here they are side by side.

- **`run_after`** — the earliest moment the run may start. Always set.
- **`logical_date`** — the date the run is "about". It fills templates such as `{{ ds }}`
  (`YYYY-MM-DD`). In Airflow 3 it **can be `None`** for runs triggered without one (CLI, REST API with
  `"logical_date": null`, asset-triggered runs); the UI's Trigger form pre-fills the current time.
- **data interval** (`data_interval_start` / `data_interval_end`) — the period of data the run
  should process.

How they relate depends on the **timetable** (the object behind `schedule`). In Airflow 3 a cron
string such as `"@daily"` gives a **trigger** timetable (`CronTriggerTimetable`) by default: the
run happens *at* the cron time and covers no period. A **data-interval** timetable
(`CronDataIntervalTimetable`, the Airflow 2 behaviour) runs *after* the period it covers has ended.

```text
Trigger timetable — schedule="@daily"  (01_hello_airflow, 14_schedule_cron_preset)

  Nov 10 00:00            Nov 11 00:00            Nov 12 00:00
  ─────┼───────────────────────┼───────────────────────┼──────▶ time
                               ▲
                               run starts here
                               run_after          = Nov 11 00:00
                               logical_date       = Nov 11 00:00   ({{ ds }} = 2026-11-11)
                               data interval      = [Nov 11 00:00, Nov 11 00:00]  (zero length)

Data-interval timetable — CronDataIntervalTimetable("@daily")  (14_schedule_catchup_backfill)

  Nov 10 00:00            Nov 11 00:00            Nov 12 00:00
  ─────┼───────────────────────┼───────────────────────┼──────▶ time
       ├── data interval ──────┤
       ▲                       ▲
       logical_date            run starts here
       = Nov 10 00:00          run_after = data_interval_end = Nov 11 00:00
       ({{ ds }} = 2026-11-10)
```

`14_scheduling_and_timetables.py` has a task `show_timing` that prints all of these, so you can
compare them across schedule types.
→ [18 · Scheduling](18-scheduling-and-timetables.md)

### `schedule`

The DAG argument that says *when* runs are created: a cron preset (`"@daily"`, `"@hourly"`,
`"@once"`, `"@continuous"`), a cron expression (`"30 6 * * 1-5"`), a `timedelta`, a timetable object,
a list/condition of **Assets**, or `None` for manual-only DAGs. (Airflow 2's `schedule_interval` no
longer exists.)
→ [18 · Scheduling](18-scheduling-and-timetables.md), [13 · Assets](13-assets-data-aware-scheduling.md)

### `start_date` (and `end_date`)

The first moment a DAG may be scheduled. It is **not** "when the DAG starts running after you deploy
it". Use a fixed date with a timezone, e.g. `pendulum.datetime(2026, 1, 1, tz="UTC")` — never
`datetime.now()`, which changes on every parse. `end_date` optionally stops scheduling.
→ [05 · Your first DAG](05-first-dag.md)

### `catchup`

If a DAG's `start_date` is in the past, should the scheduler create runs for every missed interval?
`catchup=True` says yes; `catchup=False` (the Airflow 3 default) only schedules from now on. To
re-run past dates on purpose, use a **backfill** (`airflow backfill create` or the UI).
→ [18 · Scheduling](18-scheduling-and-timetables.md), [26 · CLI](26-cli-and-rest-api.md)

## Passing data and configuration

### XCom

**XCom** ("cross-communication") lets tasks share *small* values (numbers, strings, small
dicts/lists) through Airflow. A task's return value is stored automatically under the key
`return_value`; others pull it. Keep XComs small — for large data, write a file or table and pass
the path.
→ [08 · XCom](08-xcom.md)

### Params

**Params** are typed inputs to a DAG run, declared on the DAG with `Param(...)`. When you click
Trigger, Airflow renders a form from them and validates the values (JSON Schema). Tasks read them
via `params`.
→ [14 · Params, variables & templating](14-params-variables-templating.md)

### Variables

**Variables** are global key/value settings stored in Airflow (*Admin → Variables*) or provided as
environment variables `AIRFLOW_VAR_<KEY>`. This project defines `environment` and
`tutorial_settings` (JSON) that way. Read them with `Variable.get(...)` at runtime or
`{{ var.value.<key> }}` in templates.
→ [14 · Params, variables & templating](14-params-variables-templating.md)

### Jinja templating

Many operator arguments accept `{{ ... }}` placeholders that are filled in right before the task
runs: `{{ ds }}`, `{{ run_id }}`, `{{ params.x }}`, `{{ var.value.x }}`, `{{ ti.xcom_pull(...) }}`.
The task's *Rendered Templates* tab in the UI shows the result.
→ [14 · Params, variables & templating](14-params-variables-templating.md)

## Talking to other systems

### Connections

A **connection** stores how to reach an external system: type, host, port, login, password, extras.
DAGs refer to it only by its `conn_id`, so credentials stay out of code. This project defines
`demo_postgres`, `fs_default` and `open_meteo_api` as `AIRFLOW_CONN_*` environment variables (they
work but are not listed in the UI).
→ [15 · Connections & hooks](15-connections-and-hooks.md)

### Hooks

A **hook** is a Python class that knows how to talk to one kind of system using a connection —
e.g. `PostgresHook(postgres_conn_id="demo_postgres")` gives you `get_records`, `insert_rows` and so
on. Operators use hooks internally; you can also use them inside `@task` functions.
→ [15 · Connections & hooks](15-connections-and-hooks.md),
[17 · Custom operators & hooks](17-custom-operators-hooks-plugins.md)

### Providers

**Providers** are separately installed packages (`apache-airflow-providers-<name>`) that add
operators, hooks, sensors, connection types and more for one technology: `postgres`, `http`,
`common-sql`, `amazon`, `google`, `slack`… Even the basic operators come from the `standard`
provider. List them with `airflow providers list` or *Admin → Providers*.
→ [03 · Adding Python packages](03-installation-and-setup.md#adding-python-packages)

## Scaling and waiting

### Pools

A **pool** is a named number of slots that limits how many tasks using it run at the same time,
across all DAGs — for example "at most 2 calls to this API". Tasks use `pool="api_pool"`; every
task is in `default_pool` otherwise. This project creates `api_pool` (2) and `database_pool` (4).
→ [19 · Pools, priority & concurrency](19-pools-priority-concurrency.md)

### Assets

An **Asset** is a logical name for a piece of data (a table, a file). A task that updates it lists it
in `outlets`; another DAG can use it as its `schedule` and run automatically after each update.
Assets were called *Datasets* in Airflow 2.
→ [13 · Assets](13-assets-data-aware-scheduling.md)

### Executor

The **executor** decides where task processes run: as local subprocesses (LocalExecutor, used here),
on Celery workers, in Kubernetes pods, and so on. Your DAG code does not change between executors.
→ [02 · Architecture](02-architecture.md#executors-in-one-minute),
[29 · Executors & production](29-executors-and-production.md)

### Triggerer and deferrable operators

A **deferrable** operator or sensor can pause itself and hand the waiting to the **triggerer**, an
`asyncio` process that can watch thousands of conditions cheaply. While deferred, the task uses no
worker slot. Sensors that are not deferrable instead "poke" in a loop (`mode="poke"`) or give the
slot back between checks (`mode="reschedule"`).
→ [12 · Sensors & deferrable](12-sensors-and-deferrable.md)

## Map of the examples

Each concept, and the DAG file that demonstrates it best. All files are in [`dags/`](../dags/).

| Concept | Example file | DAG id(s) | Chapter |
|---|---|---|---|
| DAG, task, classic operators, `>>`, `default_args`, `schedule`, `start_date`, `catchup` | [01_hello_airflow.py](../dags/01_hello_airflow.py) | `01_hello_airflow` | [05](05-first-dag.md) |
| TaskFlow API (`@dag`, `@task`, `multiple_outputs`, `@task.bash`) | [02_taskflow_api.py](../dags/02_taskflow_api.py) | `02_taskflow_api` | [06](06-taskflow-api.md) |
| Operators tour, `chain`, `cross_downstream`, `Label`, `TriggerDagRunOperator` | [03_operators_and_dependencies.py](../dags/03_operators_and_dependencies.py) | `03_operators_and_dependencies` | [07](07-operators-and-dependencies.md) |
| XCom push/pull, XCom in templates | [04_xcom.py](../dags/04_xcom.py) | `04_xcom` | [08](08-xcom.md) |
| Branching, short-circuit, trigger rules | [05_branching_and_trigger_rules.py](../dags/05_branching_and_trigger_rules.py) | `05_branching_and_trigger_rules` | [09](09-branching-and-trigger-rules.md) |
| Dynamic task mapping (`.expand()`, `.partial()`) | [06_dynamic_task_mapping.py](../dags/06_dynamic_task_mapping.py) | `06_dynamic_task_mapping` | [10](10-dynamic-task-mapping.md) |
| Task groups | [07_task_groups.py](../dags/07_task_groups.py) | `07_task_groups` | [11](11-task-groups.md) |
| Sensors, poke vs reschedule, deferrable, triggerer | [08_sensors_and_deferrable.py](../dags/08_sensors_and_deferrable.py) | `08_sensors_and_deferrable` | [12](12-sensors-and-deferrable.md) |
| Assets, asset schedules, `@asset` | [09_assets_data_aware_scheduling.py](../dags/09_assets_data_aware_scheduling.py) | `09_assets_producer_orders`, `09_assets_producer_customers`, `09_assets_consumer`, `09_assets_consumer_and_or`, `09_assets_or_time`, `tutorial_daily_kpis` | [13](13-assets-data-aware-scheduling.md) |
| Params, Variables, Jinja templating, macros | [10_params_variables_templating.py](../dags/10_params_variables_templating.py) | `10_params_variables_templating` | [14](14-params-variables-templating.md) |
| Connections, hooks, SQL operator | [11_connections_and_hooks.py](../dags/11_connections_and_hooks.py) | `11_connections_and_hooks` | [15](15-connections-and-hooks.md) |
| Retries, timeouts, callbacks, notifiers, retry policies, deadline alerts | [12_error_handling_and_alerts.py](../dags/12_error_handling_and_alerts.py) | `12_error_handling_and_alerts` | [16](16-error-handling-retries-alerts.md) |
| Custom operators, hooks, sensors, plugins | [13_custom_operators_hooks_sensors.py](../dags/13_custom_operators_hooks_sensors.py) + [plugins/](../plugins/) | `13_custom_operators_hooks_sensors` | [17](17-custom-operators-hooks-plugins.md) |
| Schedules, timetables, logical date / data interval / `run_after`, catchup | [14_scheduling_and_timetables.py](../dags/14_scheduling_and_timetables.py) | `14_schedule_*` (9 DAGs) | [18](18-scheduling-and-timetables.md) |
| Pools, priority weights, concurrency limits | [15_pools_priority_concurrency.py](../dags/15_pools_priority_concurrency.py) | `15_pools_priority_concurrency` | [19](19-pools-priority-concurrency.md) |
| Setup and teardown tasks | [16_setup_teardown.py](../dags/16_setup_teardown.py) | `16_setup_teardown` | [20](20-setup-teardown.md) |
| Generating DAGs in a loop | [17_dynamic_dag_generation.py](../dags/17_dynamic_dag_generation.py) | `17_generated_ingest_crm`, `_erp`, `_web` | [21](21-dynamic-dag-generation.md) |
| Human-in-the-loop operators | [18_human_in_the_loop.py](../dags/18_human_in_the_loop.py) | `18_human_in_the_loop` | [22](22-human-in-the-loop.md) |
| Task loops (Airflow 3.4) | [19_task_loops.py](../dags/19_task_loops.py) | `19_task_loop_fixed`, `19_task_loop_until` | [23](23-task-loops.md) |
| Everything together: ETL with mapped groups, SQL checks, asset-triggered report | [20_capstone_sales_etl.py](../dags/20_capstone_sales_etl.py) | `20_capstone_sales_etl`, `20_capstone_sales_report` | [24](24-capstone-sales-etl.md) |

## Common mistakes

- **Confusing a DAG with a DAG run**, or a task with a task instance. The DAG/task is the
  definition; the run/instance is one execution of it.
- **Expecting a DAG with `start_date` = today and `schedule="@daily"` to run immediately.** The
  scheduler waits for the next `run_after`; trigger it manually to test.
- **Using `datetime.now()` as `start_date`.** It moves on every parse; use a fixed date.
- **Assuming `logical_date` / `{{ ds }}` always exists.** In Airflow 3, runs created without a
  logical date (CLI without `-l`, REST API with `null`, asset-triggered) do not have one.
- **Putting large data in XCom** instead of a file or table.
- **Hard-coding passwords** in DAG files instead of using Connections.

## Try it yourself

1. In [`01_hello_airflow.py`](../dags/01_hello_airflow.py), find: the DAG, each task, each operator,
   the dependencies, and the `schedule`, `start_date` and `catchup` settings.
2. Rewrite `01_hello_airflow` on paper in the TaskFlow style, using
   [`02_taskflow_api.py`](../dags/02_taskflow_api.py) as a model.
3. Using the timeline above, work out `logical_date` and `run_after` for the 2026-11-05 run of
   `14_schedule_catchup_backfill`. Then check your answer in that task's log.
4. Pick three rows of the map table, open the files and find the exact line that demonstrates the
   concept.

---

**Next →** [05 · Your first DAG](05-first-dag.md) · [Back to the index](README.md)

Official docs: [Core concepts](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/index.html) ·
[Glossary of this project](32-glossary.md)
