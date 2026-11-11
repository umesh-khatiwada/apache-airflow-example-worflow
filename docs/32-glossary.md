# Glossary

**What you'll learn:** short definitions of the Airflow terms used across these docs, in alphabetical order. Each entry links to the chapter that explains the term in depth, and names the example DAG that shows it where there is one.

> **Run it**
> This is a reference page; there's nothing to run. Use your browser's search (Ctrl/Cmd + F), or jump to a letter:
> [A](#a) · [B](#b) · [C](#c) · [D](#d) · [E](#e) · [F](#f) · [H](#h) · [I](#i) · [J](#j) · [K](#k) · [L](#l) · [M](#m) · [N](#n) · [O](#o) · [P](#p) · [R](#r) · [S](#s) · [T](#t) · [U](#u) · [V](#v) · [W](#w) · [X](#x) · [Z](#z)

---

## A

### .airflowignore
A file in the DAGs folder listing paths the [DAG processor](#dag-processor) should skip, using glob patterns like `.gitignore` by default. This project ignores `include/`, `data` and `sql` ([../dags/.airflowignore](../dags/.airflowignore)). → [30-troubleshooting-faq.md](30-troubleshooting-faq.md)

### API server
The `airflow api-server` component (FastAPI). It serves the web UI, the public [REST API](#rest-api) under `/api/v2`, and the [Execution API](#execution-api) that running tasks use. Replaces the Airflow 2 *webserver*. Port 8080 in this project. → [02-architecture.md](02-architecture.md)

### Asset
A logical name for a piece of data (a table, a file, a model), created with `Asset(name=..., uri=...)`. Tasks that update it list it in `outlets`; other DAGs can use it as their `schedule`. Called *Dataset* in Airflow 2. Example: `raw_orders` in `09_assets_*`, `sales_summary` in the capstone. → [13-assets-data-aware-scheduling.md](13-assets-data-aware-scheduling.md)

### @asset decorator
A shortcut that defines a DAG, a single task and the asset it produces from one function. `tutorial_daily_kpis` in [../dags/09_assets_data_aware_scheduling.py](../dags/09_assets_data_aware_scheduling.py) is one. → [13-assets-data-aware-scheduling.md](13-assets-data-aware-scheduling.md)

### Asset event
A record saying "this asset was updated", created when a task with the asset in its `outlets` succeeds. It can carry `extra` metadata (`outlet_events[raw_orders].extra = {...}`). Consumers read them via `triggering_asset_events`. Since 3.4, an asset-scheduled DAG gets one run per event by default. → [13-assets-data-aware-scheduling.md](13-assets-data-aware-scheduling.md)

### AssetOrTimeSchedule
A schedule that runs a DAG on a timetable **or** when assets update, whichever comes first. Used by `09_assets_or_time`. → [13-assets-data-aware-scheduling.md](13-assets-data-aware-scheduling.md)

### Auth manager
The pluggable component that handles login and permissions in Airflow 3: `SimpleAuthManager` (default, used by `make standalone`), the FAB auth manager (used by this project's Docker setup), Keycloak or AWS. → [29-executors-and-production.md](29-executors-and-production.md)

## B

### Backfill
Creating runs for past dates on purpose, for example after fixing a bug. In Airflow 3 the scheduler runs backfills; start one with `airflow backfill create --dag-id ... --from-date ... --to-date ...` or from the UI. → [18-scheduling-and-timetables.md](18-scheduling-and-timetables.md), [26-cli-and-rest-api.md](26-cli-and-rest-api.md)

### BaseHook
The base class for [hooks](#hook) (`from airflow.sdk import BaseHook`). It provides `get_connection()`. `OpenMeteoHook` subclasses it. → [17-custom-operators-hooks-plugins.md](17-custom-operators-hooks-plugins.md)

### BaseOperator
The base class every [operator](#operator) inherits from (`from airflow.sdk import BaseOperator`). It holds the common task arguments (`task_id`, `retries`, `pool`, ...). Custom operators override `execute()`. → [17-custom-operators-hooks-plugins.md](17-custom-operators-hooks-plugins.md)

### BaseSensorOperator
The base class for [sensors](#sensor); subclasses implement `poke()`. `CsvHasRowsSensor` is one. → [12-sensors-and-deferrable.md](12-sensors-and-deferrable.md)

### Branching
Choosing at run time which downstream path to follow. A `@task.branch` function returns the task id(s) to run; the others are skipped. `choose_plan` in `05_branching_and_trigger_rules`. → [09-branching-and-trigger-rules.md](09-branching-and-trigger-rules.md)

### Bundle
Short for **DAG bundle**: a source of DAG files, configured in `[dag_processor] dag_bundle_config_list`. The default is a local folder (`dags-folder`); a Git bundle reads a repository and enables [DAG versioning](#dag-version). → [29-executors-and-production.md](29-executors-and-production.md)

## C

### Callback
A function Airflow calls when something happens: `on_success_callback`, `on_failure_callback`, `on_retry_callback`, `on_skipped_callback`, on tasks or DAGs. Examples in [../plugins/tutorial_lib/callbacks.py](../plugins/tutorial_lib/callbacks.py). → [16-error-handling-retries-alerts.md](16-error-handling-retries-alerts.md)

### Catchup
Whether the scheduler creates runs for every missed interval between `start_date` and now. **Off by default in Airflow 3.** `14_schedule_catchup_backfill` turns it on. → [18-scheduling-and-timetables.md](18-scheduling-and-timetables.md)

### CeleryExecutor
An [executor](#executor) that sends tasks through a message broker (Redis) to a pool of Celery worker processes, possibly on many machines. → [29-executors-and-production.md](29-executors-and-production.md)

### Connection
Stored credentials and location of an external system (host, login, password, extra JSON), referenced in code by a *conn id* such as `demo_postgres`. Defined in the UI, as `AIRFLOW_CONN_<ID>` env vars (this project), or in a [secrets backend](#secrets-backend). → [15-connections-and-hooks.md](15-connections-and-hooks.md)

### Constraints file
A list of exact, tested versions of all Airflow dependencies for one Airflow + Python version, published by the Airflow project. Installing with `--constraint` keeps your environment reproducible; see the `Makefile`. → [28-best-practices.md](28-best-practices.md)

### Context
The dictionary of run information available to a task: `ti`, `dag_run`, `logical_date`, `ds`, `params`, `run_id`, ... TaskFlow tasks receive keys by naming parameters after them (`def task(ds=None)`); otherwise use `get_current_context()`. → [06-taskflow-api.md](06-taskflow-api.md), [14-params-variables-templating.md](14-params-variables-templating.md)

### Cron expression
Five fields (minute, hour, day of month, month, day of week) describing clock times, e.g. `"30 6 * * 1-5"` = 06:30 Monday to Friday. Presets such as `"@daily"` are shortcuts. → [18-scheduling-and-timetables.md](18-scheduling-and-timetables.md)

## D

### DAG
*Directed Acyclic Graph*: a workflow made of [tasks](#task) and the dependencies between them, with no loops. Defined in a Python file with `with DAG(...)` or `@dag`. → [04-core-concepts.md](04-core-concepts.md), [05-first-dag.md](05-first-dag.md)

### DAG processor
The `airflow dag-processor` component. It repeatedly imports the files in the DAG [bundles](#bundle) and stores the serialized DAGs in the [metadata DB](#metadata-db). A separate process in Airflow 3. Refreshes every 30 s in this project. → [02-architecture.md](02-architecture.md)

### DAG run
One execution of a DAG, identified by a [run_id](#run_id). It has a [run type](#run-type), a [logical date](#logical-date) (possibly `None`) and a state. → [04-core-concepts.md](04-core-concepts.md)

### DAG version
A snapshot of a DAG's code/structure. Airflow 3 records versions, so the UI shows which version each run used; with a versioned bundle (Git) a run keeps using the version it started with. → [29-executors-and-production.md](29-executors-and-production.md)

### Data interval
The period of data a run is responsible for, `data_interval_start` to `data_interval_end`. Data-interval timetables (`CronDataIntervalTimetable`) give it a real length; trigger timetables (the default for cron strings in Airflow 3) make it zero-length. → [18-scheduling-and-timetables.md](18-scheduling-and-timetables.md)

### Deadline alert
`DeadlineAlert(reference=..., interval=..., callback=...)`: calls a callback if a DAG run hasn't finished in time. Replaces Airflow 2 SLAs (3.1+). Used in `12_error_handling_and_alerts`. → [16-error-handling-retries-alerts.md](16-error-handling-retries-alerts.md)

### default_args
A dict of task arguments applied to every task in a DAG (or task group), such as `owner`, `retries`, `retry_delay`. A task can override them. → [05-first-dag.md](05-first-dag.md)

### Deferrable operator
An operator or sensor that, while waiting, hands the wait to the [triggerer](#triggerer) and frees its worker slot (state *deferred*). `TimeDeltaSensor(..., deferrable=True)` in `08_sensors_and_deferrable`. → [12-sensors-and-deferrable.md](12-sensors-and-deferrable.md)

### Dependency
An edge between two tasks: "A must finish before B starts". Set with `>>`/`<<`, `chain()`, `cross_downstream()`, or by passing one TaskFlow task's output to another. → [07-operators-and-dependencies.md](07-operators-and-dependencies.md)

### Dynamic DAG generation
Creating several DAGs from one file, usually in a loop over a config. `17_generated_ingest_*` are generated this way. → [21-dynamic-dag-generation.md](21-dynamic-dag-generation.md)

### Dynamic task mapping
Creating a variable number of task instances at run time with `.expand()` / `.partial()`. → [10-dynamic-task-mapping.md](10-dynamic-task-mapping.md) (see [Mapped task](#mapped-task))

## E

### EdgeExecutor
An executor from the `edge3` provider whose workers run on remote machines and fetch work from the api-server over HTTP(S). → [29-executors-and-production.md](29-executors-and-production.md)

### Execution API
The API (under `/execution` on the api-server) that running tasks use, through the [Task SDK](#task-sdk), to report state and read/write Variables, Connections and XComs. Tasks don't connect to the metadata DB directly in Airflow 3. → [02-architecture.md](02-architecture.md)

### Executor
The component that decides *where and how* ready task instances run: `LocalExecutor` (this project), `CeleryExecutor`, `KubernetesExecutor`, `EdgeExecutor`, or several at once. Set with `[core] executor`. → [29-executors-and-production.md](29-executors-and-production.md)

### Extra link
A button on a task in the UI that opens a URL, defined by a `BaseOperatorLink`. `OpenMeteoDocsLink` adds *Open-Meteo Docs* to `FetchWeatherOperator` tasks. → [17-custom-operators-hooks-plugins.md](17-custom-operators-hooks-plugins.md)

## F

### FAB auth manager
The [auth manager](#auth-manager) based on Flask-AppBuilder, from the `fab` provider: users and roles stored in the metadata DB. This project's Docker setup uses it (login `airflow` / `airflow`). → [29-executors-and-production.md](29-executors-and-production.md)

### Fernet key
The secret key (`[core] fernet_key`) used to encrypt connection passwords and variable values stored in the metadata DB. Empty by default in this project's `.env.example`; always set it outside local learning. → [29-executors-and-production.md](29-executors-and-production.md)

## H

### HITL
**Human-in-the-loop**: tasks that pause and wait for a person to approve, choose or enter something in the UI (`HITLOperator`, `ApprovalOperator`, `HITLEntryOperator`, `HITLBranchOperator`, Airflow 3.1+). Example `18_human_in_the_loop`. → [22-human-in-the-loop.md](22-human-in-the-loop.md)

### Hook
A Python class that knows how to talk to one external system, using a [connection](#connection) for credentials: `PostgresHook`, or this project's `OpenMeteoHook`. Operators and tasks use hooks. → [15-connections-and-hooks.md](15-connections-and-hooks.md)

### Human-in-the-loop
See [HITL](#hitl).

## I

### Idempotent
A task is idempotent if running it twice gives the same result as running it once, which makes retries and re-runs safe. The capstone's `load_to_postgres` deletes its batch before inserting. → [28-best-practices.md](28-best-practices.md), [24-capstone-sales-etl.md](24-capstone-sales-etl.md)

### Import error
An exception raised while the DAG processor imports a DAG file. The file's DAGs disappear and a red banner appears in the UI; list them with `airflow dags list-import-errors`. → [30-troubleshooting-faq.md](30-troubleshooting-faq.md)

### Inlet / outlet
Assets a task reads (`inlets`) or updates (`outlets`). A successful task with `outlets=[asset]` emits an [asset event](#asset-event). → [13-assets-data-aware-scheduling.md](13-assets-data-aware-scheduling.md)

## J

### Jinja
The templating language behind `{{ ... }}` placeholders in operator fields, rendered just before the task runs (`{{ ds }}`, `{{ params.x }}`, `{{ var.value.y }}`). → [14-params-variables-templating.md](14-params-variables-templating.md)

### JWT
*JSON Web Token*: a signed token. Airflow 3 uses JWTs to authenticate REST API calls (`POST /auth/token`) and task calls to the Execution API, signed with `[api_auth] jwt_secret`. → [26-cli-and-rest-api.md](26-cli-and-rest-api.md)

## K

### KubernetesExecutor
An [executor](#executor) that starts one Kubernetes pod per task instance. → [29-executors-and-production.md](29-executors-and-production.md)

### KubernetesPodOperator
An operator (from `cncf.kubernetes`) that runs any container image as a pod; useful to isolate heavy dependencies. → [28-best-practices.md](28-best-practices.md)

## L

### Label
Text shown on a dependency edge in the Graph view: `fan_out >> Label("only on weekends") >> bash_skip`. → [07-operators-and-dependencies.md](07-operators-and-dependencies.md)

### LocalExecutor
The default [executor](#executor): runs tasks as subprocesses on the scheduler's machine. Used by this project's compose file. → [29-executors-and-production.md](29-executors-and-production.md)

### Logical date
The date a DAG run "is about" (`logical_date`, `{{ ds }}`). Called `execution_date` in Airflow 2. In Airflow 3 it can be `None` for manual runs without a date and for asset-triggered runs. → [18-scheduling-and-timetables.md](18-scheduling-and-timetables.md), [30-troubleshooting-faq.md](30-troubleshooting-faq.md)

## M

### Macro
A function or value available in templates under `macros`, such as `{{ macros.ds_add(ds, 7) }}`. Add your own with `user_defined_macros` or a plugin (`macros.tutorial_plugin.to_upper`). → [14-params-variables-templating.md](14-params-variables-templating.md)

### Mapped task
A task expanded with `.expand()` into one [task instance](#task-instance) per input item, shown with `[ ]` in the Grid view. `square`, `process_file` and `mapped_bash` in `06_dynamic_task_mapping`. → [10-dynamic-task-mapping.md](10-dynamic-task-mapping.md)

### Metadata DB
Airflow's own database (Postgres in this project): DAG runs, task instances, XComs, connections, variables, users. Not the same as the `demo` database your DAGs write to. → [02-architecture.md](02-architecture.md)

### multiple_outputs
A TaskFlow option that stores each key of a returned dict as its own XCom, so downstream tasks can use `result["total"]`. Turned on automatically when the return annotation is `dict`. → [06-taskflow-api.md](06-taskflow-api.md)

## N

### Notifier
A reusable, templated callback class (`BaseNotifier`), for example `SlackNotifier` or this project's `LogNotifier`. Pass it as `on_failure_callback=...` or in `notifiers=[...]` on HITL operators. → [16-error-handling-retries-alerts.md](16-error-handling-retries-alerts.md)

## O

### Operator
A template for one kind of task (`BashOperator`, `PythonOperator`, `SQLExecuteQueryOperator`, ...). Instantiating it with a `task_id` inside a DAG creates a task. → [07-operators-and-dependencies.md](07-operators-and-dependencies.md)

### Owner
The `owner` task argument, usually set in `default_args`; shown in the UI so people know who maintains a DAG. → [28-best-practices.md](28-best-practices.md)

## P

### Param
A typed input for a DAG run (`Param(150, type="integer", minimum=0)`), validated with JSON Schema and shown as a form when you trigger the DAG. Read as `params["name"]` or `{{ params.name }}`. → [14-params-variables-templating.md](14-params-variables-templating.md)

### Parse time
When the DAG processor imports a DAG file to learn its structure (every 30 s here), as opposed to *run time*, when a worker runs a task. Top-level code runs at parse time. → [28-best-practices.md](28-best-practices.md)

### Paused / unpaused
A paused DAG gets no scheduled runs, and its triggered runs stay queued. In this project new DAGs start **paused**; switch the toggle on to unpause. → [05-first-dag.md](05-first-dag.md), [30-troubleshooting-faq.md](30-troubleshooting-faq.md)

### Plugin
A class inheriting `AirflowPlugin` that registers things Airflow itself must know about: macros, extra links, listeners, UI/API extensions, timetables. `tutorial_plugin.py`. Ordinary shared code doesn't need a plugin. → [17-custom-operators-hooks-plugins.md](17-custom-operators-hooks-plugins.md)

### Poke / poke_interval
A [sensor](#sensor)'s check (`poke()`), repeated every `poke_interval` seconds until it returns true or the `timeout` is reached. → [12-sensors-and-deferrable.md](12-sensors-and-deferrable.md)

### PokeReturnValue
What a sensor can return from `poke()` to say whether it's done **and** push an XCom: `PokeReturnValue(is_done=True, xcom_value=...)`. → [12-sensors-and-deferrable.md](12-sensors-and-deferrable.md)

### Pool
A named set of slots that limits how many tasks using it run at once, across all DAGs. `api_pool` (2) and `database_pool` (4) come from `config/pools.json`. → [19-pools-priority-concurrency.md](19-pools-priority-concurrency.md)

### Priority weight
A task's `priority_weight`: when slots are scarce, higher-weight queued tasks start first. `weight_rule` decides how upstream/downstream weights are added. → [19-pools-priority-concurrency.md](19-pools-priority-concurrency.md)

### Provider
A separately installed package that adds operators, hooks, sensors, connections types and more for one system: `apache-airflow-providers-postgres`, `...-standard`, `...-common-sql`, `...-fab`. → [15-connections-and-hooks.md](15-connections-and-hooks.md)

## R

### Rendered template
A task instance's template fields after Jinja has filled them in, shown in the **Rendered Template** tab; `airflow tasks render` prints them. → [14-params-variables-templating.md](14-params-variables-templating.md)

### REST API
Airflow's public HTTP API under `/api/v2/...`, authenticated with a [JWT](#jwt). Interactive docs at http://localhost:8080/docs. → [26-cli-and-rest-api.md](26-cli-and-rest-api.md)

### Retry
Running a failed task again automatically, controlled by `retries`, `retry_delay`, `retry_exponential_backoff` and `max_retry_delay`. The state between tries is *up_for_retry*. → [16-error-handling-retries-alerts.md](16-error-handling-retries-alerts.md)

### Retry policy
An object (`ExceptionRetryPolicy` with `RetryRule`s, or a `ChainRetryPolicy`) that decides per exception whether to retry, fail, or use a different delay. `smart_retries` in `12_error_handling_and_alerts`. → [16-error-handling-retries-alerts.md](16-error-handling-retries-alerts.md)

### run_after
The earliest time a DAG run may start (`dag_run.run_after`). Always set, even when the logical date is `None`. → [18-scheduling-and-timetables.md](18-scheduling-and-timetables.md)

### run_id
The unique id of a DAG run, e.g. `scheduled__2026-11-01T00:00:00+00:00` or `manual__...`. The capstone uses it as `batch_id`. → [04-core-concepts.md](04-core-concepts.md)

### Run type
Why a DAG run exists: `scheduled`, `manual`, `backfill` or `asset_triggered` (`dag_run.run_type`). → [18-scheduling-and-timetables.md](18-scheduling-and-timetables.md)

## S

### Schedule
The `schedule=` argument of a DAG: `None`, a cron string or preset, a `timedelta`, a [timetable](#timetable), or assets. → [18-scheduling-and-timetables.md](18-scheduling-and-timetables.md)

### Scheduler
The `airflow scheduler` component: creates DAG runs when they're due, decides which task instances are ready, and hands them to the [executor](#executor). → [02-architecture.md](02-architecture.md)

### Secrets backend
An external store (HashiCorp Vault, AWS Secrets Manager, GCP Secret Manager, Azure Key Vault) that Airflow reads connections and variables from, configured under `[secrets] backend`. → [29-executors-and-production.md](29-executors-and-production.md)

### Secrets masker
The feature that replaces known secrets (connection passwords, sensitive variables) with `***` in task logs. `show_connection_info` in `11_connections_and_hooks` demonstrates it. → [15-connections-and-hooks.md](15-connections-and-hooks.md)

### Sensor
An operator that waits for a condition (a file, a row count, a time) before succeeding. Modes: `poke` (holds a slot), `reschedule` (frees it between checks), or [deferrable](#deferrable-operator). → [12-sensors-and-deferrable.md](12-sensors-and-deferrable.md)

### Setup / teardown
Tasks that create (`@setup`) and always clean up (`@teardown`) a resource around the work, even when the work fails. Example `16_setup_teardown`. → [20-setup-teardown.md](20-setup-teardown.md)

### Short-circuit
A `@task.short_circuit` task that returns `False` to skip everything downstream. `is_business_day` in `05_branching_and_trigger_rules`. → [09-branching-and-trigger-rules.md](09-branching-and-trigger-rules.md)

### SimpleAuthManager
Airflow 3's default, minimal [auth manager](#auth-manager), meant for development. `make standalone` runs it with every user as admin. → [29-executors-and-production.md](29-executors-and-production.md)

### SLA
*Service Level Agreement*: an Airflow 2 feature (`sla=`) that was removed in Airflow 3. Use [deadline alerts](#deadline-alert). → [31-airflow-2-vs-3.md](31-airflow-2-vs-3.md)

### Standard provider
`apache-airflow-providers-standard`: the package that contains the everyday operators and sensors (`BashOperator`, `PythonOperator`, `EmptyOperator`, `FileSensor`, HITL operators) that lived in Airflow core in version 2. → [07-operators-and-dependencies.md](07-operators-and-dependencies.md), [31-airflow-2-vs-3.md](31-airflow-2-vs-3.md)

## T

### Tag
A label on a DAG (`tags=["tutorial", "01-basics"]`) used to filter the DAG list in the UI. → [28-best-practices.md](28-best-practices.md)

### Task
One node of a DAG: an instance of an operator, or a `@task` function, with a `task_id`. → [04-core-concepts.md](04-core-concepts.md)

### Task group
A way to group tasks visually and prefix their ids (`extract.download`), with `@task_group` or `TaskGroup`. It doesn't change how tasks run. Example `07_task_groups`. → [11-task-groups.md](11-task-groups.md)

### Task instance
One run of one task in one DAG run (and one map index, for mapped tasks). It has a state such as *queued*, *running*, *success*, *failed*, *skipped*, *up_for_retry*, *deferred* or *upstream_failed*, and a try number. The squares in the Grid view. → [04-core-concepts.md](04-core-concepts.md)

### Task loop
New in 3.4: a task group repeated with `.loop(max_iterations=..., until=...)`; each iteration sees `loop.index`, `loop.previous` and `loop.result`. Example `19_task_loop_*`. → [23-task-loops.md](23-task-loops.md)

### TaskFlow
The decorator-based way to write DAGs (`@dag`, `@task`): plain Python functions become tasks, and passing return values wires dependencies and XComs automatically. → [06-taskflow-api.md](06-taskflow-api.md)

### Task SDK
The `airflow.sdk` package: the stable interface DAG authors import from (`dag`, `task`, `Asset`, `Variable`, `BaseOperator`, ...) and the runtime that executes tasks and talks to the [Execution API](#execution-api). → [02-architecture.md](02-architecture.md), [31-airflow-2-vs-3.md](31-airflow-2-vs-3.md)

### Template field
An operator attribute listed in `template_fields`, which Airflow renders with [Jinja](#jinja) before the task runs (`bash_command`, `CsvQualityCheckOperator.file_path`). Other attributes are not templated. → [14-params-variables-templating.md](14-params-variables-templating.md), [17-custom-operators-hooks-plugins.md](17-custom-operators-hooks-plugins.md)

### Timetable
An object that decides when a DAG runs and what [data interval](#data-interval) each run covers: `CronTriggerTimetable`, `CronDataIntervalTimetable`, `DeltaTriggerTimetable`, `MultipleCronTriggerTimetable`, ... → [18-scheduling-and-timetables.md](18-scheduling-and-timetables.md)

### Trigger (a DAG)
Starting a DAG run on demand: the ▶ **Trigger** button, `airflow dags trigger`, the REST API, or `TriggerDagRunOperator` (`trigger_hello` in `03_operators_and_dependencies`). → [05-first-dag.md](05-first-dag.md), [26-cli-and-rest-api.md](26-cli-and-rest-api.md)

### Trigger (deferral)
A small async Python object that the [triggerer](#triggerer) runs on behalf of a [deferred](#deferrable-operator) task, waking the task when its event happens. → [12-sensors-and-deferrable.md](12-sensors-and-deferrable.md)

### Trigger rule
The condition on upstream states that lets a task run: `all_success` (default), `none_failed_min_one_success`, `one_failed`, `all_done`, `always`, ... → [09-branching-and-trigger-rules.md](09-branching-and-trigger-rules.md)

### Triggerer
The `airflow triggerer` component that runs [triggers](#trigger-deferral) for deferred tasks, and deadline-alert callbacks, in an async event loop. If it's down, deferred tasks never resume. → [12-sensors-and-deferrable.md](12-sensors-and-deferrable.md), [02-architecture.md](02-architecture.md)

## U

### Upstream / downstream
Tasks that run before (upstream) or after (downstream) a given task. *upstream_failed* is the state of a task that can't run because an upstream task failed. → [07-operators-and-dependencies.md](07-operators-and-dependencies.md)

## V

### Variable
A global key/value setting stored in Airflow (Admin ▸ Variables) or provided as `AIRFLOW_VAR_<KEY>` env vars. Read with `Variable.get("key", default=...)` inside tasks or `{{ var.value.key }}` in templates. → [14-params-variables-templating.md](14-params-variables-templating.md)

## W

### Worker
A process that executes task instances. With `LocalExecutor` these are subprocesses of the scheduler; with Celery or Kubernetes they are separate containers or pods. → [02-architecture.md](02-architecture.md), [29-executors-and-production.md](29-executors-and-production.md)

## X

### XCom
*Cross-communication*: small values tasks pass to each other, stored in the metadata DB. A TaskFlow return value is pushed automatically under the key `return_value`; read with `ti.xcom_pull()` or by passing the value to another task. Keep them small. → [08-xcom.md](08-xcom.md)

### XCom backend
The class that stores XComs (`[core] xcom_backend`). The default uses the metadata DB; the `common.io` provider's object-storage backend puts large values in S3/GCS/local files. → [08-xcom.md](08-xcom.md), [28-best-practices.md](28-best-practices.md)

## Z

### Zombie task
Airflow 2 name for a task whose process died or stopped sending heartbeats. Airflow 3 detects these with `[scheduler] task_instance_heartbeat_timeout` and fails or retries them. → [30-troubleshooting-faq.md](30-troubleshooting-faq.md)

---

## Common mistakes

- **Mixing up *DAG*, *DAG run*, *task* and *task instance*.** A DAG is the definition; a DAG run is one execution of it; a task is a node; a task instance is that node in one run.
- **Confusing the metadata DB with your data.** Airflow's own tables live in the `airflow` database; this project's pipelines write to `demo`.
- **Thinking an asset's `uri` is read by Airflow.** It's only an identifier; your tasks do the reading and writing.
- **Treating "trigger" as one thing.** Triggering a DAG starts a run; a *trigger* in the deferral sense is the async waiter run by the triggerer; a *trigger rule* decides whether a task runs.

## Try it yourself

1. Pick five terms from this page and find each one in the code of `20_capstone_sales_etl` ([../dags/20_capstone_sales_etl.py](../dags/20_capstone_sales_etl.py)).
2. Open any DAG in the UI and identify on screen: a DAG run, a task instance, its try number, its rendered template and its XCom.
3. Explain the difference between a *pool*, `max_active_tasks` and `max_active_tis_per_dag` in one sentence each, then check your answer in [19-pools-priority-concurrency.md](19-pools-priority-concurrency.md).

---

**Next →** [33-exercises.md](33-exercises.md) · [Back to the index](README.md)
