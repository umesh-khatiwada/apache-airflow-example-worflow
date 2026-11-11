# The CLI and the REST API

**What you'll learn:** the `airflow` command-line commands you will actually use (grouped by job),
how to run them in this project (Docker and local), and how to drive Airflow from scripts with the
REST API: get a token, list DAGs, trigger a run with config, follow its state, read task instances
and XComs, and pause a DAG — with `curl` and with Python `requests`.

> **Run it**
> Examples use `01_hello_airflow` ([../dags/01_hello_airflow.py](../dags/01_hello_airflow.py)),
> `04_xcom` ([../dags/04_xcom.py](../dags/04_xcom.py)) and `05_branching_and_trigger_rules`
> ([../dags/05_branching_and_trigger_rules.py](../dags/05_branching_and_trigger_rules.py)).
> Unpause them first, or their runs will wait in `queued`.

---

## Part 1 — the `airflow` CLI

Every command below was checked with `--help` against Airflow 3.4.0. Run `airflow <group> --help`
or `airflow <group> <command> --help` any time to see all options, and `airflow cheat-sheet` for a
one-page list.

### Where to run it

| Setup | How | Which database it talks to |
|---|---|---|
| Docker, interactive shell | `make cli` (= `docker compose run --rm airflow-cli bash`), then `airflow …` | The Docker Postgres |
| Docker, one command | `docker compose run --rm airflow-cli dags list` (the image adds `airflow` for you) | The Docker Postgres |
| Docker, inside a running container | `docker compose exec airflow-scheduler airflow dags list` (here you type `airflow` yourself) | The Docker Postgres |
| Local, no Docker | `source scripts/local_env.sh`, then `airflow …` | The SQLite DB in `.airflow_local/` |

> ⚠️ The local CLI and the Docker stack have **separate databases**. `airflow dags list` on your
> laptop after `source scripts/local_env.sh` does not show runs you made in the Docker UI.

`docker compose exec` is faster (the container is already running); `docker compose run --rm`
starts a fresh container that is removed afterwards.

### DAGs

```bash
airflow dags list                                  # all DAGs: id, file, owner, paused, bundle
airflow dags list -o json                          # -o / --output: table (default), json, yaml, plain
airflow dags list-import-errors                    # files that failed to parse, with the error
airflow dags reserialize                           # re-parse all files now and store the result
airflow dags details 01_hello_airflow              # every attribute of one DAG
airflow dags next-execution 01_hello_airflow -n 3  # the next 3 logical dates

airflow dags unpause 01_hello_airflow
airflow dags pause 01_hello_airflow
airflow dags unpause '^19_' --treat-dag-id-as-regex -y   # several at once (-y: don't ask)

airflow dags trigger 01_hello_airflow -l 2026-11-11T00:00:00+00:00
airflow dags trigger 05_branching_and_trigger_rules -c '{"order_count": 20, "make_it_fail": true}' -r my_cli_run
airflow dags list-runs 01_hello_airflow            # filters: --state, -s/--start-date, -e/--end-date, --limit
airflow dags state 05_branching_and_trigger_rules my_cli_run   # state of one run (by run_id or logical date)
```

- `-c / --conf` passes a JSON dict that becomes the run's `conf` (and fills `params`).
- `-r / --run-id` lets you choose the run id instead of `manual__<timestamp>`.
- `-l / --logical-date` sets the logical date. **Without it, a CLI-triggered run has no logical
  date** in Airflow 3, so templates such as `{{ ds }}` and context keys such as `logical_date` are
  not available. `01_hello_airflow`'s `say_hello` task uses `{{ ds }}`, so give it
  `-l` when you trigger it from the CLI (`print_context` copes: it uses `context.get('logical_date')`). (`05_branching_and_trigger_rules` handles the missing date
  on purpose — see its `is_business_day` task.)
- Triggering a **paused** DAG creates the run, but it stays `queued` until you unpause.

### Testing without the scheduler

```bash
# Run ONE task in-process, print its log to the terminal, do NOT record state in the DB.
airflow tasks test 01_hello_airflow say_hello 2026-11-11

# Pass params to the task
airflow tasks test 10_params_variables_templating use_params_and_variables -t '{"customer_name": "Grace"}'

# Run a WHOLE DAG run in one process (no scheduler/executor needed)
airflow dags test 02_taskflow_api
```

`tasks test` ignores dependencies, so a task that pulls an XCom from an upstream task gets nothing.
`dags test` runs every task in order in your terminal; it is the quickest way to debug a DAG and is
covered in [27 · Testing DAGs](27-testing-dags.md). Unlike `tasks test`, it does record the run in
the metadata database.

### Tasks and task instances

```bash
airflow tasks list 01_hello_airflow                       # task ids in the DAG
airflow tasks states-for-dag-run 05_branching_and_trigger_rules my_cli_run   # every task in one run
airflow tasks state 05_branching_and_trigger_rules maybe_fail my_cli_run     # one task instance
airflow tasks render 01_hello_airflow say_hello 2026-11-11     # show rendered templates (logical date or run id)

# Clear = reset task instances so the scheduler re-runs them
airflow tasks clear 01_hello_airflow -t '^say_hello$' -d -s 2026-11-01 -e 2026-11-11 -y
#   -t/--task-regex  which tasks     -d/--downstream  include downstream tasks
#   -s/-e            logical-date range   -f/--only-failed   only failed ones   -y  no prompt
```

### Backfill

A **backfill** creates runs for a range of past dates — for example after fixing a bug.

```bash
airflow backfill create --dag-id 01_hello_airflow \
  --from-date 2026-11-01 --to-date 2026-11-03 --dry-run      # show what would be created

airflow backfill create --dag-id 01_hello_airflow \
  --from-date 2026-11-01 --to-date 2026-11-03 \
  --max-active-runs 2 --reprocess-behavior failed
```

- `--from-date` / `--to-date` are both **inclusive** and required.
- `--reprocess-behavior`: `none` (default — only dates with no run), `failed` (also re-run failed
  ones), `completed` (re-run everything).
- `--run-backwards` processes the newest date first; `--dag-run-conf '{...}'` sets `conf`.
- The command only *creates* the backfill; the scheduler runs it, and the DAG must be **unpaused**
  for its tasks to execute. Progress is visible in the DAG's **Backfills** tab.

The same thing is available in the UI: **Trigger → Backfill**. See
[18 · Scheduling](18-scheduling-and-timetables.md).

### Variables, connections and pools

```bash
# Variables
airflow variables list                               # keys only; --show-values for values
airflow variables get environment                    # works for env-var Variables too
airflow variables get tutorial_settings -j           # -j: parse JSON
airflow variables set my_key "my value" --description "set from the CLI"
airflow variables set my_json '{"a": 1}' -j
airflow variables export -                           # all DB variables as JSON to stdout
airflow variables import my_vars.json -a overwrite   # -a: overwrite | fail | skip existing keys

# Connections
airflow connections list                             # ids only; env-var connections are hidden (warning shown)
airflow connections get demo_postgres                # works for env-var connections too
airflow connections add my_pg --conn-type postgres --conn-host postgres --conn-port 5432 \
  --conn-login airflow --conn-password airflow --conn-schema demo
airflow connections test my_pg                       # needs [core] test_connection = Enabled (it is, here)
airflow connections export - --file-format yaml      # DB connections to stdout
airflow connections import my_conns.json --overwrite

# Pools
airflow pools list
airflow pools get api_pool
airflow pools set my_pool 3 "A pool created from the CLI"     # NAME slots description
airflow pools export pools_backup.json
airflow pools import /opt/airflow/config/pools.json           # path inside the container
```

`export` / `import` cover what is stored in the **database**; Variables and Connections defined as
environment variables are not exported.

### Configuration, database, environment

```bash
airflow config get-value core executor           # LocalExecutor
airflow config get-value core dags_are_paused_at_creation
airflow config list --section dag_processor --show-values   # option names only without --show-values

airflow db check                                 # can Airflow reach its metadata DB?
airflow db migrate                               # create/upgrade the schema (airflow-init does this)

airflow info                                     # versions, paths, executor, DB, providers (--anonymize to share)
airflow version                                  # 3.4.0
airflow plugins                                  # loaded plugins: tutorial_plugin with macros + extra link
airflow providers list                           # installed provider packages and versions
airflow cheat-sheet                              # every command on one page
```

> 💡 Airflow 3 also has **`airflowctl`**, a separate command-line tool that talks to the REST API
> instead of the database (useful against a remote Airflow). It is a separate package and is not
> installed in this project; everything here uses the classic `airflow` CLI.

---

## Part 2 — the REST API

The API server exposes a public, versioned REST API under **`/api/v2`**. The UI itself uses it, so
anything you can do in the UI you can script.

- **Interactive docs:** open **http://localhost:8080/docs** — every endpoint with its parameters and
  a *Try it out* button. (`/redoc` has the same content in another layout.)
- The OpenAPI specification shipped with Airflow 3.4 is the source for every path and field below.

### 1. Get a token

Most endpoints need a **JWT** (a signed access token) in the `Authorization` header. Get one from
the **auth manager**'s token endpoint, `POST /auth/token`.

**Docker (FAB auth manager)** — send the UI username and password as JSON:

```bash
TOKEN=$(curl -s -X POST http://localhost:8080/auth/token \
  -H "Content-Type: application/json" \
  -d '{"username": "airflow", "password": "airflow"}' | jq -r .access_token)
echo "$TOKEN"
```

**Local `make standalone` (SimpleAuthManager, everyone is admin)** — no credentials needed:

```bash
TOKEN=$(curl -s http://localhost:8080/auth/token | jq -r .access_token)
```

The response looks like `{"access_token": "eyJ..."}`. Tokens expire (by default after 24 hours,
`[api_auth] jwt_expiration_time`); request a new one when you get `401 Unauthorized`.

> `jq` is a small JSON command-line tool (`brew install jq`). Without it, copy the `access_token`
> value by hand.

Two endpoints work without a token: `GET /api/v2/monitor/health` and `GET /api/v2/version`.

```bash
curl -s http://localhost:8080/api/v2/version
curl -s http://localhost:8080/api/v2/monitor/health
```

### 2. List DAGs

```bash
curl -s "http://localhost:8080/api/v2/dags?tags=tutorial&limit=100" \
  -H "Authorization: Bearer $TOKEN" | jq '.total_entries, [.dags[].dag_id]'
```

Useful query parameters: `limit` / `offset` (paging), `tags` (repeatable), `paused=true|false`,
`dag_id_pattern=hello` (substring match), `order_by=dag_id`. The response is
`{"dags": [...], "total_entries": N}`.

### 3. Trigger a DAG run with config

`POST /api/v2/dags/{dag_id}/dagRuns`. The body field **`logical_date` is required**, but may be
`null`. Other optional fields: `dag_run_id`, `conf`, `note`, `data_interval_start` /
`data_interval_end`, `run_after`.

```bash
curl -s -X POST http://localhost:8080/api/v2/dags/05_branching_and_trigger_rules/dagRuns \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "dag_run_id": "api_run_1",
        "logical_date": null,
        "conf": {"order_count": 250, "make_it_fail": false},
        "note": "triggered from curl"
      }' | jq '{dag_run_id, state, logical_date, conf}'
```

- `conf` values are validated against the DAG's `params`; a wrong type returns an error instead of
  creating the run.
- With `"logical_date": null` the run has **no logical date** (fine for `05_…`). For a DAG whose
  tasks use `{{ ds }}` or `logical_date` — like `01_hello_airflow` — send a timestamp, e.g.
  `"logical_date": "2026-11-11T00:00:00Z"`. Two runs of the same DAG cannot share a logical date.
- Choosing your own `dag_run_id` makes the next calls easier: the generated ids contain `:` and `+`
  characters that you would have to URL-encode.

### 4. Get the run's state

```bash
curl -s http://localhost:8080/api/v2/dags/05_branching_and_trigger_rules/dagRuns/api_run_1 \
  -H "Authorization: Bearer $TOKEN" | jq '{state, run_type, start_date, end_date, duration}'
```

`state` is `queued`, `running`, `success` or `failed`.

### 5. List the run's task instances

```bash
curl -s http://localhost:8080/api/v2/dags/05_branching_and_trigger_rules/dagRuns/api_run_1/taskInstances \
  -H "Authorization: Bearer $TOKEN" | jq -r '.task_instances[] | "\(.task_id)\t\(.state)\t\(.try_number)"'
```

With `order_count: 250` you should see `process_large` succeeded and `process_small` **skipped** —
the branch at work. Filter with query parameters such as `state=failed` or `task_id=join`.

### 6. Read an XCom

`GET /api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries/{key}`. A task's
return value is stored under the key `return_value`. Using `04_xcom`, whose `produce_number` task
returns `42`:

```bash
curl -s -X POST http://localhost:8080/api/v2/dags/04_xcom/dagRuns \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"dag_run_id": "api_xcom_demo", "logical_date": null}' > /dev/null

# …wait a few seconds for the run to finish, then:
curl -s http://localhost:8080/api/v2/dags/04_xcom/dagRuns/api_xcom_demo/taskInstances/produce_number/xcomEntries/return_value \
  -H "Authorization: Bearer $TOKEN" | jq '{key, value}'
```

Custom keys work the same way, e.g. `.../taskInstances/push_with_keys/xcomEntries/file_path`.
For a mapped task, add `?map_index=0`.

### 7. Pause or unpause a DAG

```bash
curl -s -X PATCH http://localhost:8080/api/v2/dags/05_branching_and_trigger_rules \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"is_paused": true}' | jq '{dag_id, is_paused}'
```

Send `{"is_paused": false}` to unpause.

### Other endpoints worth knowing

| Endpoint | Purpose |
|---|---|
| `GET /api/v2/dags/{dag_id}/dagRuns` | List runs of a DAG (use `~` as `dag_id` for all DAGs) |
| `GET /api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}` | A task's log for one try |
| `POST /api/v2/dags/{dag_id}/clearTaskInstances` | Clear (re-run) task instances |
| `GET /api/v2/importErrors` | Files that failed to parse |
| `GET/POST /api/v2/variables`, `/api/v2/connections`, `/api/v2/pools` | Manage Variables, Connections, Pools |
| `GET /api/v2/assets`, `POST /api/v2/assets/events` | List Assets, create an asset event (triggers consumers) |

Check exact parameters at http://localhost:8080/docs.

### A Python `requests` example

The same flow as a script: get a token, trigger `05_branching_and_trigger_rules`, wait for it,
print each task's state. Run it from your laptop with `pip install requests` (or from the local
`.venv`, which already has it).

```python
"""Trigger a DAG through the Airflow 3 REST API and wait for the result."""

import time
import uuid

import requests

BASE = "http://localhost:8080"
DAG_ID = "05_branching_and_trigger_rules"

# 1. Token (FAB auth manager in Docker). For `make standalone`: requests.get(f"{BASE}/auth/token")
resp = requests.post(f"{BASE}/auth/token", json={"username": "airflow", "password": "airflow"}, timeout=10)
resp.raise_for_status()
session = requests.Session()
session.headers["Authorization"] = f"Bearer {resp.json()['access_token']}"

# 2. Make sure the DAG is unpaused, then trigger it with config
session.patch(f"{BASE}/api/v2/dags/{DAG_ID}", json={"is_paused": False}, timeout=10).raise_for_status()
run_id = f"script_{uuid.uuid4().hex[:8]}"
resp = session.post(
    f"{BASE}/api/v2/dags/{DAG_ID}/dagRuns",
    json={"dag_run_id": run_id, "logical_date": None, "conf": {"order_count": 20, "make_it_fail": True}},
    timeout=10,
)
resp.raise_for_status()
print("triggered", run_id)

# 3. Poll until the run is finished
while True:
    state = session.get(f"{BASE}/api/v2/dags/{DAG_ID}/dagRuns/{run_id}", timeout=10).json()["state"]
    print("run state:", state)
    if state in ("success", "failed"):
        break
    time.sleep(5)

# 4. Task instance states
tis = session.get(f"{BASE}/api/v2/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances", timeout=10).json()
for ti in tis["task_instances"]:
    print(f"{ti['task_id']:<20} {ti['state']}")
```

With `make_it_fail: true`, `maybe_fail` fails, while `on_any_failure` (trigger rule `one_failed`)
and `cleanup` (`all_done`) run — the same behaviour you would see in the UI
([09 · Branching & trigger rules](09-branching-and-trigger-rules.md)). Notice the final run state
printed by the script: Airflow decides a DAG run's state from its **leaf** tasks (tasks with nothing
downstream). Here the leaves are `send_report`, `on_any_failure` and `cleanup`, so a failure in
`maybe_fail` that is "handled" by those tasks does not by itself fail the run.

## Common mistakes

- **Running the local CLI and expecting to see Docker runs** (or the other way round). They use
  different databases.
- **Triggering a paused DAG** from the CLI or API and waiting forever — unpause first.
- **Triggering without a logical date** a DAG that uses `{{ ds }}` / `logical_date`. Use `-l` (CLI)
  or send a `logical_date` (API).
- **Forgetting `Content-Type: application/json`** on POST/PATCH requests, or omitting the required
  `logical_date` field (send `null` if you don't want one).
- **Using an expired token** — a `401` means "get a new token".
- **Not URL-encoding generated run ids** such as `manual__2026-11-11T10:00:00+00:00` in URLs —
  choose your own `dag_run_id` instead.

## Try it yourself

1. With the CLI, trigger `05_branching_and_trigger_rules` twice with different `order_count` values
   and use `airflow tasks states-for-dag-run` to compare which branch ran.
2. Use `airflow backfill create … --dry-run` on `01_hello_airflow` for the first week of November
   2026, then run it for real and watch the **Backfills** tab.
3. Write a `curl` command that lists only the **failed** task instances of a run
   (hint: `state=failed`).
4. Extend the Python script to download the log of every failed task
   (`.../taskInstances/{task_id}/logs/{try_number}`).

---

**Next →** [27 · Testing DAGs](27-testing-dags.md) · [Back to the index](README.md)

Official docs: [CLI reference](https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html) ·
[REST API reference](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html)
