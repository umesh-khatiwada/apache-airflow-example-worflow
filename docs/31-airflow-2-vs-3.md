# Airflow 2 vs Airflow 3: translating old tutorials

**What you'll learn:** what changed between Airflow 2 and Airflow 3, so you can read older blog posts, courses and Stack Overflow answers and translate their code to the Airflow 3.4 used in this project. It closes with a summary of what's new in 3.1–3.4 and a migration checklist.

> **Run it**
> Every DAG in [../dags/](../dags/) is written in Airflow 3 style, so any of them shows the "new" column of the tables below. A good one to compare with an Airflow 2 tutorial is [../dags/01_hello_airflow.py](../dags/01_hello_airflow.py) (`01_hello_airflow`).
> To check your own code for Airflow 2 leftovers: `uvx ruff check --select AIR3 dags plugins` (this project already passes).

Airflow 3.0 was released in April 2025, the first major version since Airflow 2.0 in December 2020. Most tutorials, answers and AI-generated snippets on the internet are still written for Airflow 2. If an example uses `schedule_interval`, `execution_date`, `from airflow.operators.bash import ...` or `/api/v1/`, it's Airflow 2.

---

## 1. Quick translation table

### DAG and task code

| Airflow 2 (old) | Airflow 3 (new) | Notes |
|---|---|---|
| `DAG(..., schedule_interval="@daily")` | `DAG(..., schedule="@daily")` | `schedule_interval` and `timetable=` are gone: `TypeError: ... unexpected keyword argument 'schedule_interval'`. Timetables go into `schedule=` too |
| No `schedule` given → runs daily | No `schedule` given → **`None`** (manual only) | always set `schedule` explicitly |
| `catchup=True` by default | **`catchup=False` by default** (`[scheduler] catchup_by_default = False`) | set `catchup=True` when you want historical runs |
| `"@daily"` = data-interval schedule | `"@daily"` = **`CronTriggerTimetable`** (zero-length data interval) | `[scheduler] create_cron_data_intervals = False`; see [18-scheduling-and-timetables.md](18-scheduling-and-timetables.md) |
| `from airflow import DAG`, `from airflow.models import DAG` | `from airflow.sdk import DAG` | the old paths still import in 3.4 (some with a deprecation warning), but new code should use `airflow.sdk` |
| `from airflow.decorators import dag, task, task_group, setup, teardown` | `from airflow.sdk import dag, task, task_group, setup, teardown` | `airflow.decorators` logs a `DeprecatedImportWarning` |
| `from airflow.models import Variable, Param, BaseOperator` | `from airflow.sdk import Variable, Param, BaseOperator` | |
| `from airflow.hooks.base import BaseHook` | `from airflow.sdk import BaseHook` | |
| `from airflow.sensors.base import BaseSensorOperator, PokeReturnValue` | `from airflow.sdk import BaseSensorOperator, PokeReturnValue` | |
| `from airflow.utils.task_group import TaskGroup` | `from airflow.sdk import TaskGroup` | |
| `from airflow.models.baseoperator import chain, cross_downstream` | `from airflow.sdk import chain, cross_downstream` | |
| `from airflow.exceptions import AirflowSkipException, AirflowFailException` | `from airflow.sdk.exceptions import AirflowSkipException, AirflowFailException` | old path warns |
| `from airflow.operators.bash import BashOperator` | `from airflow.providers.standard.operators.bash import BashOperator` | core operators moved to the **standard provider** |
| `from airflow.operators.python import PythonOperator, BranchPythonOperator` | `from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator` | or use `@task` / `@task.branch` |
| `from airflow.operators.empty import EmptyOperator` / `DummyOperator` | `from airflow.providers.standard.operators.empty import EmptyOperator` | `DummyOperator` no longer exists |
| `from airflow.operators.trigger_dagrun import TriggerDagRunOperator` | `from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator` | |
| `from airflow.sensors.filesystem import FileSensor`, `...time_delta import TimeDeltaSensor` | `from airflow.providers.standard.sensors.filesystem import FileSensor` (etc.) | |
| `from airflow.datasets import Dataset` / `Dataset(...)` | `from airflow.sdk import Asset` / `Asset(name=..., uri=...)` | renamed in 3.0; the `Dataset` alias is removed in 3.4 |
| `DatasetOrTimeSchedule`, `DatasetAlias`, `triggering_dataset_events` | `AssetOrTimeSchedule`, `AssetAlias`, `triggering_asset_events` | see [13-assets-data-aware-scheduling.md](13-assets-data-aware-scheduling.md) |
| `SubDagOperator` | **removed**: use [task groups](11-task-groups.md) (or separate DAGs + assets) | |
| `sla=timedelta(...)`, `sla_miss_callback=` | **removed** (ignored with a warning): use `DeadlineAlert` (3.1+) | see [16-error-handling-retries-alerts.md](16-error-handling-retries-alerts.md) |
| `days_ago(1)` (`airflow.utils.dates`) | a fixed `pendulum.datetime(2026, 1, 1, tz="UTC")` | `days_ago` is gone |
| `concurrency=` on the DAG | `max_active_tasks=` | `concurrency` is rejected |
| `task_concurrency=` on a task | `max_active_tis_per_dag=` | |
| `provide_context=True` | not needed | context is passed automatically |
| `from airflow.settings import Session` / `@provide_session` inside a task | **not allowed**: use the REST API, the Task SDK (`Variable`, `Connection`, XCom), or a hook to *your* database | tasks no longer get a metadata DB connection |

### Run dates and the context

| Airflow 2 | Airflow 3 | Notes |
|---|---|---|
| `execution_date` (context key, `{{ execution_date }}`, `dag_run.execution_date`) | `logical_date` | renamed in 2.2, removed in 3.0 |
| `next_ds`, `next_ds_nodash`, `next_execution_date` | `data_interval_end` (formatted with `strftime`/macros) | removed |
| `prev_ds`, `prev_ds_nodash`, `prev_execution_date` | `data_interval_start`, or `prev_data_interval_start_success` / `prev_start_date_success` | removed |
| `prev_execution_date_success` | `prev_data_interval_start_success` | removed |
| `tomorrow_ds`, `tomorrow_ds_nodash`, `yesterday_ds`, `yesterday_ds_nodash` | `{{ macros.ds_add(ds, 1) }}` / `{{ macros.ds_add(ds, -1) }}` | removed |
| `conf` (the whole Airflow config in the context) | removed | read settings with Variables instead |
| Missing template variables | still an error | DAGs use Jinja's `StrictUndefined` by default, so a removed key like `{{ next_ds }}` fails the task with `UndefinedError` |
| `dag_run.external_trigger` | `dag_run.run_type` (`manual`, `scheduled`, `backfill`, `asset_triggered`, ...) | removed |
| Every run has an `execution_date` | `logical_date` **may be `None`** for manual (without a date) and asset-triggered runs | then `ds`, `ts`, `data_interval_*` are missing too; see [30-troubleshooting-faq.md](30-troubleshooting-faq.md) |
| — | new: `dag_run.run_after` | the earliest time the run may start; always set |

The removed keys were checked against the `Context` type in the installed `airflow.sdk` 3.4.0. Keys that still exist include `ds`, `ds_nodash`, `ts`, `ts_nodash`, `logical_date`, `data_interval_start/end`, `run_id`, `dag_run`, `ti`, `params`, `var`, `conn`, `macros`, `triggering_asset_events`, `outlet_events`, `inlet_events`.

### Running and operating Airflow

| Airflow 2 | Airflow 3 | Notes |
|---|---|---|
| `airflow webserver` (Flask) | `airflow api-server` (FastAPI) | serves the UI, the REST API and the Execution API |
| DAG parsing inside the scheduler (optional standalone processor) | **always a separate `airflow dag-processor`** | the `[scheduler] standalone_dag_processor` option is gone |
| Tasks open a connection to the metadata DB | tasks talk to the **Execution API** through the **Task SDK** | workers don't need DB credentials; see [02-architecture.md](02-architecture.md) |
| Flask-AppBuilder UI (Jinja templates) | **new React UI** | Grid, Graph, Assets, Required Actions, dark mode |
| Flask-AppBuilder security always on | pluggable **auth managers**: `SimpleAuthManager` (default), FAB (provider), Keycloak, AWS | this project uses FAB in Docker, Simple locally |
| REST API `/api/v1/...` with basic auth/session | REST API **`/api/v2/...`** with **JWT** tokens (`POST /auth/token`) | see [26-cli-and-rest-api.md](26-cli-and-rest-api.md) |
| `airflow db init`, `airflow db upgrade` | `airflow db migrate` | `init` is gone |
| `airflow dags backfill <dag_id> -s ... -e ...` | `airflow backfill create --dag-id <id> --from-date ... --to-date ...` | backfills are now run **by the scheduler** and are visible/controllable in the UI |
| `airflow tasks run` | removed from the CLI | use `airflow tasks test` (one task) or `airflow dags test` (whole DAG) for local runs |
| `airflow users create` | still exists **only with the FAB auth manager** | `airflow-init` in the compose file creates the `airflow` user through the image's entrypoint |
| DAG files in `dags_folder`, no history of code | **DAG bundles** and **DAG versioning** | a run can stick to the code version it started with (versioned bundles such as Git); the UI shows versions. See [29-executors-and-production.md](29-executors-and-production.md) |
| `CeleryKubernetesExecutor`, `LocalKubernetesExecutor` | **multiple executors** (`[core] executor = "LocalExecutor,CeleryExecutor"`, `executor=` per task) | |
| — | new **EdgeExecutor** (`edge3` provider) | workers in remote sites pull work over HTTP |
| `[scheduler] scheduler_zombie_task_threshold` ("zombies") | `[scheduler] task_instance_heartbeat_timeout` | same idea, clearer name |
| `airflow.cfg` options renamed/removed | `airflow config lint` / `airflow config update` | helpers built for the 2 → 3 move |

---

## 2. Before and after: one small DAG

An Airflow 2 style DAG you might find in an old tutorial:

```python
# Airflow 2 style: does NOT work in Airflow 3
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

with DAG(
    "hello",
    schedule_interval="@daily",
    start_date=days_ago(1),
) as dag:
    start = DummyOperator(task_id="start")
    say = BashOperator(task_id="say", bash_command="echo {{ execution_date }} {{ next_ds }}")
    start >> say
```

The same DAG in Airflow 3 style, matching [../dags/01_hello_airflow.py](../dags/01_hello_airflow.py):

```python
import pendulum

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

with DAG(
    dag_id="hello",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")
    say = BashOperator(task_id="say", bash_command="echo {{ logical_date }} {{ macros.ds_add(ds, 1) }}")
    start >> say
```

Four things changed: the imports, `schedule_interval` → `schedule`, the dynamic `days_ago` → a fixed date, and the removed template variables.

---

## 3. Concept changes worth understanding

### The Task SDK and the Execution API

In Airflow 2, every running task had full access to the metadata database. That was convenient, and also a security and scaling problem. In Airflow 3, tasks run against the **Task SDK** (`airflow.sdk`): reading Variables and Connections, pushing XComs and reporting state all go through the api-server's **Execution API** with a short-lived token. Practical effects:

- DAG code imports from `airflow.sdk`.
- Code that queried Airflow's own tables (`session.query(DagRun)...`) from inside a task must be rewritten to use the public REST API.
- Workers can live in other networks (Edge executor) and don't need database credentials.

### Assets instead of datasets

Datasets were renamed **assets** and gained a `name` (what you see in the UI) next to the `uri`, plus the `@asset` decorator, asset aliases, asset watchers (event-driven scheduling from external queues) and, in later 3.x releases, partitions. The scheduling idea is unchanged. [13-assets-data-aware-scheduling.md](13-assets-data-aware-scheduling.md) has examples.

### Backfills run in the scheduler

In Airflow 2, `airflow dags backfill` ran the backfill *in your terminal*: if you closed it, the backfill stopped. In Airflow 3 a backfill is an object the scheduler executes, visible in the UI, which you can pause or cancel. Start one from the UI (**Trigger ▸ Backfill**) or the CLI:

```bash
airflow backfill create --dag-id 14_schedule_catchup_backfill \
  --from-date 2026-11-01 --to-date 2026-11-03 --reprocess-behavior failed
```

### No more SLAs

Airflow 2 SLAs were known to be unreliable and were removed in 3.0. **Deadline alerts** (3.1+) replace them: "call this callback if the run isn't finished N minutes after it was queued / after its logical date". `12_error_handling_and_alerts` uses one:

```python
deadline=DeadlineAlert(
    reference=DeadlineReference.DAGRUN_QUEUED_AT,
    interval=timedelta(minutes=10),
    callback=AsyncCallback(deadline_missed_alert),
    name="finish_within_10_minutes",
),
```

---

## 4. What's new in Airflow 3.1 to 3.4

| Feature | Since | In this project |
|---|---|---|
| **Human-in-the-loop (HITL) operators**: `HITLOperator`, `ApprovalOperator`, `HITLEntryOperator`, `HITLBranchOperator` in `airflow.providers.standard.operators.hitl`; a person answers in the UI | 3.1 | [../dags/18_human_in_the_loop.py](../dags/18_human_in_the_loop.py), [22-human-in-the-loop.md](22-human-in-the-loop.md) |
| **Deadline alerts** (`DeadlineAlert`, `DeadlineReference`, `AsyncCallback`), the replacement for SLAs | 3.1 | [../dags/12_error_handling_and_alerts.py](../dags/12_error_handling_and_alerts.py) |
| HITL tasks wait in a dedicated **awaiting input** state, without the triggerer | 3.3 | `18_human_in_the_loop` |
| **Retry policies**: `ExceptionRetryPolicy` with `RetryRule`s decide per exception type whether to retry, fail, or change the delay; `ChainRetryPolicy` combines policies | available in 3.4 | `smart_retries` in `12_error_handling_and_alerts` |
| **Task loops**: `@task_group` + `.loop(max_iterations=..., until=...)` repeats a group, with `loop.index`, `loop.previous`, `loop.result`, `LoopDecision` | 3.4 | [../dags/19_task_loops.py](../dags/19_task_loops.py), [23-task-loops.md](23-task-loops.md) |
| **One run per asset event** by default for asset-scheduled DAGs (`batch_asset_events=True` on the timetable restores batching) | 3.4 | `09_assets_consumer`, `20_capstone_sales_report` |
| **Asset partitions**: assets and runs can carry a `partition_key` (for example a date), with `PartitionedAssetTimetable` and partition-aware backfills | 3.x | not covered; see the official docs |
| Python 3.10 dropped; **Python ≥ 3.11** required | 3.4 | `pyproject.toml`, `make venv` uses 3.12 |
| `Dataset` compatibility alias removed | 3.4 | use `Asset` |

Check the [release notes](https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html) for the full list; each minor release also brings UI improvements and many smaller options.

---

## 5. Migration helpers and checklist

### Ruff's Airflow rules

[Ruff](https://docs.astral.sh/ruff/) ships rules that find Airflow 2 code which breaks or is deprecated in Airflow 3, many with automatic fixes:

| Rule | Name | Finds |
|---|---|---|
| `AIR301` | airflow3-removal | APIs removed in Airflow 3 (`schedule_interval`, `days_ago`, `execution_date`, removed context keys, ...) |
| `AIR302` | airflow3-moved-to-provider | things moved to providers that no longer import from core (e.g. `airflow.operators.dummy.DummyOperator`) |
| `AIR303` | airflow3-incompatible-function-signature | changed function signatures |
| `AIR311` | airflow3-suggested-update | deprecated but still working names (e.g. `airflow.DAG`, `airflow.decorators.task`) |
| `AIR312` | airflow3-suggested-to-move-to-provider | deprecated core imports that now live in providers (e.g. `airflow.operators.bash.BashOperator`) |

```bash
uvx ruff check --select AIR3 dags plugins           # report
uvx ruff check --select AIR3 --fix dags plugins     # apply fixes (some are 'unsafe' and need --unsafe-fixes)
uvx ruff check --select AIR --preview dags plugins  # all Airflow rules, including preview ones
```

This project's `pyproject.toml` already selects the `AIR` rule family. Rule names were checked with ruff 0.17.4 against the Airflow 2 DAG from section 2: it flagged `schedule_interval`, `days_ago`, `DummyOperator`, the old imports and `execution_date`/`next_ds` used as Python names. Ruff does **not** look inside Jinja strings, so also search for old template variables yourself:

```bash
grep -rnE "execution_date|next_ds|prev_ds|yesterday_ds|tomorrow_ds" dags/
```

Run `uvx ruff rule AIR301` to read the details of any rule.

### Checklist for moving a DAG from 2 to 3

1. Run `ruff check --select AIR3 --fix` and fix what remains by hand.
2. Change imports to `airflow.sdk` and `airflow.providers.standard...`.
3. `schedule_interval` / `timetable` → `schedule`; decide `catchup` explicitly; check whether you relied on data intervals with a cron schedule.
4. `execution_date`, `next_ds`, `prev_ds`, `yesterday_ds`, ... → `logical_date`, `data_interval_*`, `macros.ds_add`.
5. Handle `logical_date is None` in tasks that can be triggered manually or by assets.
6. Remove `sla` / `sla_miss_callback`; add `DeadlineAlert` where you need it.
7. Replace SubDAGs with task groups.
8. Remove direct metadata-DB access from tasks.
9. `Dataset` → `Asset`.
10. Run the DAG integrity tests (`make test`) and `airflow config lint` for your configuration.

Official guide: [Upgrading to Airflow 3](https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html).

---

## Common mistakes

- **Copying an Airflow 2 snippet verbatim.** Check the imports and parameter names first; the error messages (`unexpected keyword argument 'schedule_interval'`, `cannot import name 'Dataset'`) tell you it's old code.
- **Assuming `catchup=True`.** In Airflow 3 nothing is backfilled unless you ask.
- **Assuming `"@daily"` gives a one-day data interval.** In Airflow 3 it's a trigger timetable with `data_interval_start == data_interval_end`.
- **Using `{{ next_ds }}` or `{{ yesterday_ds }}`.** These no longer exist, and because DAGs use Jinja's `StrictUndefined` by default, the task fails with `UndefinedError: 'next_ds' is undefined`. Use `macros.ds_add`.
- **Calling `/api/v1/` endpoints** from scripts. They return 404; use `/api/v2/` with a JWT.
- **Looking for "Browse ▸ SLA Misses"** in the UI. Use deadline alerts.

## Try it yourself

1. Paste the Airflow 2 DAG from section 2 into a scratch file outside `dags/` and run `uvx ruff check --select AIR3 <file>`. Compare ruff's suggestions with the Airflow 3 version.
2. In a copy of `01_hello_airflow`, add `{{ yesterday_ds }}` to `bash_command` and run it: the task fails while rendering its template (read the error in the log). Replace it with `{{ macros.ds_add(ds, -1) }}` and check **Rendered Template**.
3. Find an Airflow 2 example of `BranchPythonOperator` online and rewrite it with `@task.branch` like `choose_plan` in [../dags/05_branching_and_trigger_rules.py](../dags/05_branching_and_trigger_rules.py).

---

**Next →** [32-glossary.md](32-glossary.md) · [Back to the index](README.md)
