# Troubleshooting and FAQ

**What you'll learn:** how to diagnose and fix the problems beginners hit most often with Airflow 3.4 and this project's Docker setup. Each entry follows the same shape: **symptom → cause → fix**, with the commands to run.

> **Run it**
> Most commands below work in two places:
> - **Docker:** prefix them with `docker compose run --rm airflow-cli` (or open a shell with `make cli` and type them there). Example: `docker compose run --rm airflow-cli airflow dags list-import-errors`.
> - **Local venv:** `source scripts/local_env.sh` first, then run `airflow ...` directly.
>
> Container logs: `make logs` (scheduler), or `docker compose logs -f airflow-dag-processor` / `airflow-triggerer` / `airflow-apiserver`.

### Quick index

| Area | Questions |
|---|---|
| DAGs not visible | [DAG doesn't show up](#my-dag-doesnt-show-up-in-the-ui) · [Import error banner](#theres-a-red-import-errors-banner) |
| DAGs not running | [Nothing happens](#my-dag-is-in-the-list-but-never-runs) · [Stuck in queued/scheduled](#tasks-are-stuck-in-queued-or-scheduled) · [Stuck deferred](#tasks-are-stuck-in-deferred) · [HITL waiting](#a-human-in-the-loop-task-waits-forever) · [Sensors hogging slots](#sensors-are-using-all-my-slots) |
| Data & templates | [`logical_date` is None](#logical_date-ds-or-ts_nodash-is-none-or-undefined) · [Jinja not rendering](#my--jinja--isnt-rendered) · [XCom too large](#xcom-is-too-large--the-ui-is-slow) · [Connection not found](#the-conn_id-isnt-defined) · [Variable not found](#a-variable-is-not-found) · [Timezones](#times-are-off-by-a-few-hours) |
| Tasks dying | [Zombie / killed tasks](#tasks-fail-with-no-error-in-the-log-zombie--killed-tasks) |
| Docker | [`demo` DB missing](#database-demo-does-not-exist) · [Permission denied on logs/](#permission-denied-on-logs-linux) · [Port 8080 in use](#port-8080-is-already-in-use) · [Out of memory](#containers-keep-restarting-or-get-killed-out-of-memory) · [`docker compose` vs `docker-compose`](#docker-compose-vs-docker-compose) · [Reset everything](#how-do-i-reset-everything-and-start-fresh) |

---

## DAGs that don't appear

### My DAG doesn't show up in the UI

**Symptom:** you saved a new file in `dags/`, but the DAG isn't in the list.

**Causes and fixes, in the order to check them:**

1. **It hasn't been parsed yet.** The dag-processor looks for new files every 30 seconds in this project (`AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL: '30'`; the Airflow default is 5 minutes). Wait a minute and refresh the page.
2. **The file has an import error.** Look for the red banner at the top of the DAG list (next entry), or run:
   ```bash
   airflow dags list-import-errors
   ```
3. **You forgot to call the `@dag` function.** With TaskFlow, the decorator only *defines* a DAG factory; the DAG exists once you call it. Every TaskFlow file in this project ends with a call, for example the last line of [../dags/02_taskflow_api.py](../dags/02_taskflow_api.py):
   ```python
   # Calling the @dag function registers the DAG. Don't forget this line!
   taskflow_api()
   ```
4. **The file is ignored.** Paths matching [../dags/.airflowignore](../dags/.airflowignore) are skipped. This project ignores `include/`, `data` and `sql`, so a DAG file saved under `dags/include/` never loads. Patterns use glob syntax by default (`[core] dag_ignore_file_syntax = glob`).
5. **"Safe mode" skipped the file.** With `[core] dag_discovery_safe_mode = True` (the default), the dag-processor only imports a `.py` file if its text contains the word `airflow` **and** either `dag` or `asset` (case-insensitive). A helper file that builds DAGs but never mentions these words is skipped. Add an import such as `from airflow.sdk import dag`, or a comment.
6. **Duplicate `dag_id`.** Two files defining the same `dag_id` hide each other. Search for it: `grep -rn 'dag_id="my_id"' dags/`.
7. **The DAG object isn't reachable at module level.** A `with DAG(...)` block inside a function that's never called creates nothing. Generated DAGs like [../dags/17_dynamic_dag_generation.py](../dags/17_dynamic_dag_generation.py) must actually call the builder at the top level (its `for source_name, source_config in SOURCES.items(): build_ingest_dag(...)` loop).
8. **It's a different folder.** In Docker only `./dags` is mounted into `/opt/airflow/dags`. Check with `docker compose exec airflow-dag-processor ls /opt/airflow/dags`.

To parse one file on the spot and see its errors, run Python on it in the CLI container: `docker compose run --rm airflow-cli python /opt/airflow/dags/01_hello_airflow.py` (no output means it imported fine).

### There's a red "import errors" banner

**Symptom:** the DAG list shows *DAG Import Errors (n)*, and the DAGs from that file are gone or stale.

**Cause:** importing the file raised an exception: a syntax error, a missing package, a wrong import path, or an import that took longer than `[core] dagbag_import_timeout` (30 s by default).

**Fix:** open the banner, or run `airflow dags list-import-errors`, and read the traceback. Common ones:

| Error | Fix |
|---|---|
| `ModuleNotFoundError: No module named 'pandas'` (or any package) | add it to `requirements.txt`, switch the compose file to `build: .`, `docker compose build`. For a quick test only, set `_PIP_ADDITIONAL_REQUIREMENTS` in `.env` |
| `ModuleNotFoundError: No module named 'tutorial_lib'` (running locally) | `source scripts/local_env.sh` first: it sets the plugins folder, which is how `tutorial_lib` becomes importable |
| `ImportError: cannot import name 'Dataset' from 'airflow.sdk'` | Airflow 3 renamed datasets to assets: use `Asset`. See [31-airflow-2-vs-3.md](31-airflow-2-vs-3.md) |
| `TypeError: DAG.__init__() got an unexpected keyword argument 'schedule_interval'` | use `schedule=` |
| `AirflowTaskTimeout` / "DagBag import timeout" | remove heavy work from the top of the file ([28-best-practices.md](28-best-practices.md)) |

The test suite catches all of these before you deploy: `make test` runs `test_no_import_errors` in [../tests/test_dag_integrity.py](../tests/test_dag_integrity.py).

---

## DAGs that don't run

### My DAG is in the list but never runs

**Symptom:** no runs appear, or a triggered run stays *queued*.

1. **It's paused. This is the #1 cause.** This project sets `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'`, so **every new DAG starts paused**. A manually triggered run of a paused DAG stays queued. Flip the toggle in the DAG list, or:
   ```bash
   airflow dags unpause 01_hello_airflow
   ```
2. **`schedule=None`.** Most examples here (`02_*` to `13_*`) only run when you press **Trigger**. That's intentional.
3. **`start_date` is in the future.** Nothing is scheduled before it. Check the *Next run* column, or `airflow dags next-execution <dag_id>`.
4. **The next scheduled time hasn't come yet.** In Airflow 3 a cron string such as `"@daily"` uses `CronTriggerTimetable`: the run fires *at* the cron time (midnight UTC). With `catchup=False`, unpausing creates at most the most recent missed run, then waits for the next tick. With a data-interval timetable (`CronDataIntervalTimetable`, used by `14_schedule_catchup_backfill`) the run for a day starts only *after* that day ends. The *Next run* column shows when the next one is due. See [18-scheduling-and-timetables.md](18-scheduling-and-timetables.md).
5. **`catchup=False` (the Airflow 3 default)** means missed intervals aren't run; you only get the latest one. Use a backfill for history: `airflow backfill create --dag-id <id> --from-date 2026-11-01 --to-date 2026-11-07`, or the UI.
6. **`max_active_runs` is reached.** For example `14_schedule_catchup_backfill` has `max_active_runs=2`; `15_pools_priority_concurrency` and the capstone have `1`. Further runs wait until a running one finishes.
7. **An `end_date` has passed**, as in `14_schedule_catchup_backfill` (ends 2026-11-07).
8. **Asset-scheduled DAG, but the producer never succeeded.** `09_assets_consumer` only runs when a task with `outlets=[raw_orders]` *succeeds*. Trigger `09_assets_producer_orders` and make sure both DAGs are unpaused.

### Tasks are stuck in "queued" or "scheduled"

**Symptom:** grey task squares that never start.

| Check | How | Fix |
|---|---|---|
| Is the scheduler running? | `docker compose ps` (should be *healthy*), `make logs`, `curl -s localhost:8080/api/v2/monitor/health` | `docker compose up -d airflow-scheduler`; read its logs for the crash reason |
| Is the pool full? | **Admin ▸ Pools**: look at *Running* vs *Slots* | wait, raise the pool's slots, or check the pool exists. `api_pool` has 2 slots and `database_pool` 4 (from `config/pools.json`). A task in a pool that doesn't exist won't run; recreate pools with `airflow pools import /opt/airflow/config/pools.json` |
| Are executor slots used up? | many tasks running across all DAGs | `[core] parallelism` (32 per scheduler) or `max_active_tasks` on the DAG (`15_*` sets 6). A few poking sensors can fill everything (see below) |
| Task-level limit? | `max_active_tis_per_dag` (e.g. `write_to_single_file` in `15_*`, `mapped_bash` in `06_*`) | expected behaviour: they run one or two at a time |
| Upstream not finished? | the task's upstreams aren't done, or the trigger rule isn't met | look at the Graph view; ask Airflow: `airflow tasks failed-deps <dag_id> <task_id> <logical_date_or_run_id>` |
| Workers down (Celery) | `docker compose ps airflow-worker` | start or scale workers ([29-executors-and-production.md](29-executors-and-production.md)) |

### Tasks are stuck in "deferred"

**Symptom:** a deferrable sensor or operator (purple, *deferred*) never resumes, for example `wait_30_seconds` in `08_sensors_and_deferrable`.

**Cause:** deferred tasks are watched by the **triggerer**. If it isn't running, nothing wakes them up. Deadline-alert callbacks (`12_error_handling_and_alerts`) also run on the triggerer.

**Fix:**
```bash
docker compose ps airflow-triggerer          # should be "healthy"
docker compose logs --tail=100 airflow-triggerer
docker compose up -d airflow-triggerer
```
Running locally with `make standalone`, the triggerer is included. The `[triggerer] capacity` (default 1000 triggers per triggerer) is only a concern in big deployments.

### A human-in-the-loop task waits forever

**Symptom:** `reviewer_notes` or `choose_channel` in `18_human_in_the_loop` shows *awaiting input* for hours.

**Cause:** that's by design: a [HITL](32-glossary.md#hitl) task waits for a person. In Airflow 3.3+ it sits in the *awaiting input* state (no worker slot and no triggerer involved). Without `response_timeout`, there's no time limit. In the example, `approve_publish` (30 minutes) and `rate_this_tutorial` (1 hour) have a timeout and fall back to their `defaults`; `reviewer_notes` and `choose_channel` don't.

**Fix:** respond in the UI (**Browse ▸ Required Actions**, or click the waiting task), or give the operator `response_timeout=timedelta(...)` plus `defaults=` so it decides automatically. Make sure the people who must answer are actually notified (the example only uses `LogNotifier`, which writes to the log). See [22-human-in-the-loop.md](22-human-in-the-loop.md).

### Sensors are using all my slots

**Symptom:** many tasks stay queued while a handful of sensors run for a long time.

**Cause:** a sensor in `mode="poke"` (the default) holds a worker slot during its entire wait, even while sleeping between pokes.

**Fix:** for anything longer than a few minutes:
- `deferrable=True` where the sensor supports it (frees the slot; the triggerer waits), like `wait_30_seconds` in `08_*`;
- otherwise `mode="reschedule"`, like `wait_for_rows` (frees the slot between pokes);
- always set a `timeout`, and consider a dedicated pool for sensors.

See [12-sensors-and-deferrable.md](12-sensors-and-deferrable.md).

---

## Data, templates and configuration

### `logical_date`, `ds` or `ts_nodash` is None or undefined

**Symptom:** a task fails with `TypeError: ... NoneType`, a path contains `None`, or a template fails with `jinja2.exceptions.UndefinedError: 'ds' is undefined`.

**Cause:** in Airflow 3, a DAG run may have **no logical date**. Then `logical_date`, `ds`, `ds_nodash`, `ts`, `ts_nodash` and the data interval keys are missing from the context. A TaskFlow argument such as `ts_nodash=None` stays `None`, and a template that uses `{{ ds }}` fails, because DAGs render templates with Jinja's `StrictUndefined` by default. This happens for:
- runs triggered with `airflow dags trigger <dag_id>` **without** `-l/--logical-date`;
- runs triggered through the REST API with `"logical_date": null`;
- **asset-triggered** runs (`09_assets_consumer`, `20_capstone_sales_report`, ...);
- UI triggers where you cleared the *Logical Date* field. (The UI's Trigger form pre-fills it with "now" for normal DAGs.)

**Fix:**
- Handle `None` explicitly, as `is_business_day` does in [../dags/05_branching_and_trigger_rules.py](../dags/05_branching_and_trigger_rules.py):
  ```python
  @task.short_circuit
  def is_business_day(logical_date=None) -> bool:
      if logical_date is None:  # manual runs may have no logical date
          return True
      return logical_date.weekday() < 5
  ```
- Use something that always exists: `run_id`, `dag_run.run_after`, or `ts_nodash` with a fallback.
- When triggering from the CLI, pass a date: `airflow dags trigger 08_sensors_and_deferrable -l 2026-11-11T10:00:00+00:00`.

Examples in this project that expect a logical date: `08_sensors_and_deferrable` (file names use `ts_nodash`), `01_hello_airflow` (`{{ ds }}` in `say_hello`) and `10_params_variables_templating` (`days_between(..., ds)`). `16_setup_teardown` names its folder after `run_id`, so it works either way. Trigger them from the UI with the pre-filled date.

### My `{{ jinja }}` isn't rendered

**Symptom:** the log shows the literal text `{{ ds }}` instead of a date.

**Causes and fixes:**
- **The argument isn't a template field.** Only attributes listed in an operator's `template_fields` are rendered. `BashOperator.bash_command` and `env` are; arbitrary arguments aren't. Check the operator's docs, or in Python: `print(BashOperator.template_fields)`. For your own operators, add the field, as `CsvQualityCheckOperator` does with `template_fields = ("file_path",)`.
- **It's inside a Python function.** Code in a `@task` body isn't templated. Read the value from the context instead (`def my_task(ds=None)`), or use `templates_dict` as `templated_python` does in [../dags/10_params_variables_templating.py](../dags/10_params_variables_templating.py).
- **It's at the top level of the file.** Templates are rendered just before a task runs; module code runs at parse time.
- **f-string clash.** In an f-string, `{{` becomes a literal `{`. Don't mix f-strings with Jinja, or double the braces (`{{{{ ds }}}}`).

The **Rendered Template** tab of a task instance shows exactly what Airflow produced. `airflow tasks render <dag_id> <task_id> <logical_date>` does the same on the command line.

### XCom is too large / the UI is slow

**Symptom:** tasks returning big lists are slow, the database grows quickly, or the XCom tab takes ages to open.

**Cause:** every return value from a `@task` (and every operator result with `do_xcom_push=True`, the default) is stored in the metadata database. A `-> dict` annotation also turns on `multiple_outputs`, storing each key separately.

**Fix:** write data to a file, bucket or table and return the path or table name ([08-xcom.md](08-xcom.md), [28-best-practices.md](28-best-practices.md)). For classic operators whose result you don't need, set `do_xcom_push=False`. If you must pass larger objects, configure the object-storage XCom backend from the `common.io` provider. Another limit to know: mapping over an XCom list with more than `[core] max_map_length` (1024) items fails the upstream task.

### The conn_id isn't defined

**Symptom:** ``AirflowNotFoundException: The conn_id `demo_postgres` isn't defined``.

**Causes and fixes:**
- **Env var naming.** A connection from an environment variable must be named `AIRFLOW_CONN_` + the conn id in **upper case**: `demo_postgres` → `AIRFLOW_CONN_DEMO_POSTGRES`. The conn id you use in code is the lower-case form.
- **Case and typos.** `Demo_Postgres`, `demo-postgres` and `demo_postgres` are different ids.
- **Not available where the task runs.** Env-var connections exist only in containers/shells that have the variable. Locally, `source scripts/local_env.sh` sets the same three connections as Docker.
- **Looking in the wrong place.** Env-var connections **don't appear** in Admin ▸ Connections, but they work. Check from the CLI: `airflow connections get demo_postgres`.
- **Malformed JSON** in the env var. Validate it: `echo "$AIRFLOW_CONN_DEMO_POSTGRES" | python -m json.tool`.

Variables follow the same pattern: `AIRFLOW_VAR_ENVIRONMENT` provides `environment`.

### A Variable is not found

**Symptom:** a task fails with an `AirflowRuntimeError` mentioning `VARIABLE_NOT_FOUND` and `Variable environment not found`.

**Fix:** define it (Admin ▸ Variables, `airflow variables set environment dev`, or `AIRFLOW_VAR_ENVIRONMENT`), and in code always pass a default for optional variables: `Variable.get("environment", default="unknown")`. In templates use `{{ var.value.get("name", "fallback") }}`, as `show_params_vars_macros` does.

### Times are off by a few hours

**Symptom:** a `"0 6 * * *"` DAG runs at an unexpected local time, or log timestamps look wrong.

**Cause:** Airflow works in **UTC** by default (`[core] default_timezone = utc`), and this project's `start_date`s use `tz="UTC"`. Cron expressions are evaluated in the DAG's timezone, which is the timezone of its `start_date` (or of the timetable).

**Fix:** to schedule in local time, give the timetable or `start_date` a timezone: `CronTriggerTimetable("0 9 * * *", timezone="Asia/Kathmandu")` (see `14_schedule_trigger_timetable`) or `pendulum.datetime(2026, 1, 1, tz="Europe/Berlin")`. Use the timezone selector in the UI's user menu to *display* times in your zone. Inside tasks, `logical_date` and `data_interval_*` are timezone-aware pendulum objects; convert with `.in_timezone("Asia/Kathmandu")`.

---

## Tasks that die

### Tasks fail with no error in the log ("zombie" / killed tasks)

**Symptom:** a task is marked failed (or up for retry) although its log just stops, or the log says the task's heartbeat timed out. Older docs call these **zombie tasks**.

**Causes:**
- The process running the task was **killed**, most often by the out-of-memory killer (exit code `-9` / `SIGKILL` / `137`).
- The container or worker restarted mid-task (`docker compose restart`, a deploy, a laptop going to sleep).
- The task couldn't send heartbeats to the api-server for longer than `[scheduler] task_instance_heartbeat_timeout` (300 seconds by default), so the scheduler gave up on it.

**Fix:** give Docker more memory (next section), process data in smaller pieces (mapping, chunks, streaming instead of loading everything into a list), make sure the api-server is healthy, and keep `retries` on tasks so a killed attempt is retried. Check `dmesg` or `docker compose ps`/`docker inspect` for OOM kills.

---

## Docker and local setup

### `database "demo" does not exist`

**Symptom:** `11_connections_and_hooks` or `20_capstone_sales_etl` fails connecting to `demo_postgres`.

**Cause:** the `demo` database is created by [../scripts/postgres-init/01-create-demo-db.sql](../scripts/postgres-init/01-create-demo-db.sql), which Postgres runs **only the first time** its data volume is created. If you started Postgres before that file existed, or the volume was created another way, it never ran.

**Fix:** create it by hand (the command is also in the SQL file's comment):
```bash
docker compose exec postgres psql -U airflow -c "CREATE DATABASE demo;"
```
Or wipe the volume and start over with `make reset && make init && make up` (this deletes all Airflow history too). Running locally without Docker, `demo_postgres` points at `localhost:5432`, so you still need `docker compose up -d postgres`.

### Permission denied on `logs/` (Linux)

**Symptom:** `PermissionError: [Errno 13] Permission denied: '/opt/airflow/logs/...'`, or files in `./logs` and `./data` owned by `root`.

**Cause:** containers run as user `${AIRFLOW_UID}` (default `50000`). On Linux, bind-mounted folders keep their host ownership, so that user may not be allowed to write to your folders. (macOS and Windows Docker Desktop handle this for you.)

**Fix:** set `AIRFLOW_UID` to your user id and re-run the init container, which `chown`s the folders:
```bash
make env                         # creates .env with AIRFLOW_UID=$(id -u) if .env doesn't exist yet
grep AIRFLOW_UID .env            # check it; edit it if .env already existed
make init && make up
# if files are already owned by root:
sudo chown -R "$(id -u):0" logs data
```

### Port 8080 is already in use

**Symptom:** `Bind for 0.0.0.0:8080 failed: port is already allocated`.

**Fix:** find what's using it (`lsof -i :8080` on macOS/Linux), stop it (often another Airflow, a `make standalone` you left running, or another dev server), or move Airflow to another host port in `docker-compose.yaml`:
```yaml
  airflow-apiserver:
    ports:
      - "8081:8080"   # host:container. Then open http://localhost:8081
```
Only change the left number. The containers still talk to each other on port 8080 (`AIRFLOW__CORE__EXECUTION_API_SERVER_URL: 'http://airflow-apiserver:8080/execution/'`). The same applies to Postgres on `5432` if you already run a local Postgres: change `"5432:5432"` to `"5433:5432"` and update `AIRFLOW_CONN_DEMO_POSTGRES` in `scripts/local_env.sh` for local runs.

### Containers keep restarting or get killed (out of memory)

**Symptom:** containers flip between *starting* and *unhealthy*, `airflow-init` warns about memory, tasks die with exit code 137, or the whole machine slows down.

**Cause:** Airflow runs five long-lived containers plus Postgres. Docker Desktop's default memory limit is often too low.

**Fix:** in Docker Desktop → **Settings ▸ Resources**, give at least **4 GB** of memory (8 GB is comfortable) and 2+ CPUs; the official compose guide sets the same minimum. Stop other heavy containers. Pause `14_schedule_continuous` (it runs forever while unpaused). Check usage with `docker stats`.

### `docker compose` vs `docker-compose`

**Symptom:** commands in these docs fail with `docker: 'compose' is not a docker command`, or `docker-compose` complains about the file format (for example the `env_file` → `path`/`required` syntax).

**Cause:** `docker-compose` (with a hyphen) is the old, discontinued Compose v1. This project and its `Makefile` use Compose **v2**, which is a Docker plugin called as `docker compose` (with a space). The `env_file` entries with `required: false` need a recent v2 (2.24 or later).

**Fix:** update Docker Desktop, or install the Compose v2 plugin on Linux. Check with `docker compose version`.

### How do I reset everything and start fresh?

```bash
make reset   # docker compose down --volumes --remove-orphans: DELETES the database volume
make init    # migrate the DB again, recreate the airflow/airflow user and the pools
make up
```

This removes all DAG runs, task history, connections/variables created in the UI, and the `demo` database (it is recreated on the next start because the volume is new). Files in `./logs` and `./data` are on your disk and are **not** removed; delete them yourself if you want: `rm -rf logs/* data/*`. For the local setup, the SQLite DB lives in `.airflow_local/`; delete that folder to start fresh.

---

## Still stuck?

1. Read the task log (Grid view → task → **Logs**), then the component logs (`docker compose logs <service>`).
2. Check health: `curl -s localhost:8080/api/v2/monitor/health`.
3. Reproduce one task outside the scheduler: `airflow tasks test <dag_id> <task_id> 2026-11-11` (runs it without recording state).
4. Search the [Airflow docs](https://airflow.apache.org/docs/apache-airflow/stable/) and GitHub issues. When reading older answers, translate Airflow 2 names first: [31-airflow-2-vs-3.md](31-airflow-2-vs-3.md).

---

## Common mistakes

- **Not unpausing the DAG.** New DAGs start paused in this project.
- **Reading only the UI.** Many problems (import errors, triggerer down) are clearest in `airflow dags list-import-errors` and `docker compose logs`.
- **Triggering from the CLI without `-l`**, then wondering why `ds` is undefined.
- **Running `make reset` to "fix" something** and losing history you wanted. Try restarting the one broken service first: `docker compose restart airflow-scheduler`.
- **Changing ports or env vars in only one place** (the compose file but not `scripts/local_env.sh`, or the other way round).

## Try it yourself

1. Break a DAG on purpose: rename `taskflow_api()` at the bottom of `02_taskflow_api.py` to a comment. Watch the DAG disappear (within 30 s), then restore it.
2. Add `import pandas_does_not_exist` to a copy of a DAG file. Find the error with `airflow dags list-import-errors`, then delete the copy.
3. Stop the triggerer (`docker compose stop airflow-triggerer`), trigger `08_sensors_and_deferrable` and watch `wait_30_seconds` stay deferred. Start it again and watch the task finish.
4. Trigger `08_sensors_and_deferrable` from the CLI *without* `-l` and explain what happens to the sensors' file path. Then trigger it with `-l`.

---

**Next →** [31-airflow-2-vs-3.md](31-airflow-2-vs-3.md) · [Back to the index](README.md)
