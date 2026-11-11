# A tour of the Airflow UI

**What you'll learn:** where everything is in the Airflow 3 web UI — the home dashboard, the DAGs
list, a DAG's Grid and Graph views, a task instance's logs and XComs, the Trigger form, Assets,
and the Browse and Admin menus — plus a guided first 10 minutes with `01_hello_airflow`.

> **Run it**
> Start the stack (`make up`) and open **http://localhost:8080** (login `airflow` / `airflow`).
> The walkthrough uses `01_hello_airflow` ([../dags/01_hello_airflow.py](../dags/01_hello_airflow.py)).
> Other DAGs mentioned: `05_branching_and_trigger_rules`, `06_dynamic_task_mapping`,
> `10_params_variables_templating`, `09_assets_*`, `18_human_in_the_loop`.

---

Airflow 3 has a completely new UI written in React. If you have seen screenshots of Airflow 2
(the "Flask" UI with *Tree view*), things have moved. This chapter describes *what* is where, not
exact pixels — the UI is updated often, but the structure below has been stable through Airflow 3.x.

> 💡 Locally without Docker (`make standalone`) there is no login page: the SimpleAuthManager makes
> you admin automatically. In Docker the FAB auth manager shows a login page and adds a
> **Security** menu (users and roles).

## The navigation bar

A vertical bar on the left of every page:

| Item | What you find there |
|---|---|
| **Home** | The dashboard (below). |
| **Dags** | The list of all DAGs. |
| **Assets** | Every Asset, its events and which DAGs produce/consume it. |
| **Browse** | Cross-DAG lists: DAG runs, task instances, XComs, audit log, jobs, DAG bundles, deadlines, required actions (HITL). |
| **Admin** | Variables, Connections, Pools, Providers, Plugins, Config. |
| **Security** | (FAB auth manager only) users, roles, permissions. |
| **Docs** | Links to the documentation, the GitHub repo and the **REST API reference** (the interactive `/docs` page). |
| **User menu** | Your user, language, **timezone** for displayed dates, light/dark mode, logout. |

## Home (dashboard)

The landing page answers "is Airflow healthy and is anything on fire?":

- **Health** of the metadata database, scheduler, triggerer and DAG processor. If one shows as down,
  check `docker compose ps`.
- **Stats**: counts of failed, running and active DAGs — click one to jump to a filtered list.
- **Import errors**: Python files in `dags/` that failed to parse. Click to read the traceback.
  *This is the first place to look when a DAG does not appear.*
- **Pool slots**: how many slots are running, queued, deferred and free.
- **History**: recent DAG runs and task instances by state.
- **Favourites**: DAGs you starred in the DAGs list.
- **Asset events** and **deadlines** (upcoming and missed deadline alerts).

## Dags list

One row (or card — there is a table/card toggle) per DAG:

- **Pause toggle** — on = scheduled and runnable; off = paused. In this project every DAG starts
  **paused** (`DAGS_ARE_PAUSED_AT_CREATION=true`).
- **Name**, **schedule** (with the next run time), **latest run** and its state, recent run states.
- **Tags** — every example has `tutorial` plus a topic tag such as `01-basics` or `06-assets`.
  Use the tag filter to narrow the list.
- **Filters** at the top: paused / not paused, last run state, favourites, tags, and a search box
  for the DAG id.
- **Trigger** button (▶) — opens the Trigger form (below).
- A ⭐ to add the DAG to your favourites on the Home page.
- Import errors are shown as a banner at the top of the list.

## DAG details page

Click a DAG name. The page has three parts.

**Header:** the DAG name, schedule, next run, owner, tags, and buttons to **Trigger**, toggle
**pause**, **reparse** the DAG file now (instead of waiting for the next scan), and open the
**Dag Docs** — the `doc_md` text. All example DAGs set `doc_md=__doc__`, so this shows the
docstring at the top of each file.

**Left panel — Grid or Graph** (press `g` to switch):

- **Grid view** — columns are DAG runs (newest on the right), rows are tasks. Each square is a task
  instance, coloured by state (see the colour table in
  [02 · Architecture](02-architecture.md#task-instance-states)). The bar above each column is the
  DAG run. Task groups can be expanded/collapsed; mapped tasks show the number of instances. Click a
  square to open the task instance; click a bar to open the run.
- **Graph view** — the DAG's tasks and dependencies as boxes and arrows, coloured by the state of the
  selected run. Good for understanding branches, task groups (collapsible boxes), edge labels and
  setup/teardown pairs (shown with a dotted line). Options let you change direction and filter tasks.

**Right panel — tabs** (for the DAG as a whole):

| Tab | Shows |
|---|---|
| **Overview** | Recent failures, run duration chart, recently failed task logs, asset events. |
| **Runs** | Table of all DAG runs with state, run type (manual / scheduled / backfill / asset triggered), logical date, `run_after`, duration, conf. |
| **Tasks** | Every task with its operator, trigger rule and recent states. |
| **Calendar** | A heat-map of runs per day. |
| **Backfills** | Backfills created for this DAG and their progress. |
| **Required Actions** | Pending human-in-the-loop requests for this DAG. |
| **Audit Log** | Events: who paused, triggered, cleared or marked what, and when. |
| **Code** | The DAG file's source as Airflow parsed it, with *parsed at* time. Because Airflow 3 keeps **DAG versions**, you can view and compare the code of older versions. |
| **Details** | All DAG attributes: `dag_id`, file location, bundle, `max_active_runs`, `catchup`, params, timetable… |

**Backfill:** the Trigger button's dialog has a *Backfill* option next to *Single Run*: choose a
date range, which runs to (re)create (missing only, missing and failed, or all), max active runs,
and whether to run backwards. It shows how many runs will be created before you confirm. See
[18 · Scheduling](18-scheduling-and-timetables.md).

## Task instance panel

Click a square in the Grid (or a box in the Graph with a run selected). Tabs:

| Tab | Shows |
|---|---|
| **Logs** | The task's log. Choose the **try number** to see earlier attempts (useful with retries), filter by log level, search. |
| **Rendered Templates** | The value of every templated field *after* Jinja ran — e.g. the exact `bash_command` with `{{ ds }}` replaced. |
| **XCom** | Values this task pushed: `return_value` and any custom keys. |
| **Asset Events** | Asset events this task created (for tasks with `outlets`). |
| **Audit Log** | Actions on this task instance. |
| **Details** | State, try number, start/end, duration, operator, pool, queue, priority, hostname, the DAG version it ran with… |
| **Required Actions** | For HITL tasks: the question and the response form. |
| **Task Instances [n]** | For **mapped** tasks: one row per mapped instance (with its map index or a readable label from `map_index_template`). Click one to see its own logs/XCom. |

Actions on a task instance (buttons at the top of the panel):

- **Clear** — reset the task instance (optionally with its downstream/upstream tasks) so the
  scheduler runs it again. This is how you "re-run" a task. Shortcut `shift+c`.
- **Mark as… success / failed / skipped** — set the state by hand without running anything.
  Shortcuts `shift+s` and `shift+f`.
- Similar **Clear** and **Mark as** buttons exist for a whole DAG run, and you can add a **note** to a
  run or task instance to explain what you did.

## The Trigger form

Clicking **Trigger** opens a dialog:

- **Single Run** or **Backfill**.
- **Logical date** — pre-filled with the current time; change it to run "for" another date.
- **Run id** — optional; generated if left empty.
- **Params form** — if the DAG declares `params`, each becomes a form field with the right widget:
  checkbox for booleans, number inputs with min/max, drop-downs for `enum`, date pickers for
  `format="date"`, multi-line text for arrays, JSON editor for objects. Invalid values are rejected
  before the run is created. Try it with `10_params_variables_templating` (eight params of
  different types) or `05_branching_and_trigger_rules` (`order_count`, `make_it_fail`).
- **Configuration JSON** — the same values as raw JSON (`conf`), plus a list of **recent
  configurations** to re-use.
- A **note** for the run.

## Assets

The **Assets** page lists every Asset (`raw_orders`, `raw_customers`, `manual_override`,
`sales_summary`, …). Open one to see:

- its events (each time a producing task succeeded), with their **extra** metadata — for example
  `{"row_count": 5, "path": "..."}` from `09_assets_producer_orders`;
- the **producing tasks** and the **consuming DAGs** as a small dependency graph;
- a button to **create an asset event** by hand, which triggers the consumers — useful for testing.

See [13 · Assets](13-assets-data-aware-scheduling.md).

## Browse menu

| Page | Use it for |
|---|---|
| **Dag Runs** | All runs of all DAGs, filterable by DAG, state, run type, dates. |
| **Task Instances** | All task instances across DAGs: "show me every failed task today". |
| **XComs** | Every stored XCom, searchable by DAG, run, task and key. |
| **Audit Log** | Everything users and the system did (trigger, pause, clear, variable edits…). |
| **Jobs** | The running Airflow jobs (scheduler, triggerer, DAG processor) and their heartbeats. |
| **Dag Bundles** | Configured DAG bundles (here: `dags-folder`) and their files. |
| **Deadlines** | Deadline alerts and whether they were met or missed. See [16 · Error handling](16-error-handling-retries-alerts.md). |
| **Required Actions** | Every human-in-the-loop task waiting for a response, across DAGs. Respond from here for `18_human_in_the_loop`. See [22 · Human-in-the-loop](22-human-in-the-loop.md). |

## Admin menu

| Page | What you can do | Notes for this project |
|---|---|---|
| **Variables** | List, add, edit, delete, import/export Variables. | `environment` and `tutorial_settings` come from `AIRFLOW_VAR_*` env vars, so they are **not listed** here, but DAGs can read them. A Variable you create here with the same key is ignored while the env var exists (env vars are checked first). |
| **Connections** | List, add, edit, duplicate, delete, **Test**. | `demo_postgres`, `fs_default`, `open_meteo_api` are env-var connections and **not listed**. The **Test** button is enabled (`AIRFLOW__CORE__TEST_CONNECTION: 'Enabled'`) — create a connection here to try it. |
| **Pools** | See slots (running / queued / scheduled / deferred / open), add or resize pools. | `api_pool` (2), `database_pool` (4) and `default_pool`. Watch them while `15_pools_priority_concurrency` runs. |
| **Providers** | Installed provider packages and versions. | The official image ships many providers. |
| **Plugins** | Loaded plugins and what they add. | Shows `tutorial_plugin` with its macros and operator extra link. |
| **Config** | The effective Airflow configuration. | Visible because `AIRFLOW__API__EXPOSE_CONFIG: 'true'`. Never enable this in production — it can reveal secrets. |

## Your first 10 minutes

A guided first run with `01_hello_airflow`.

1. **Log in** at http://localhost:8080 with `airflow` / `airflow`. Glance at **Home**: all health
   items should be healthy and there should be **no import errors**.
2. **Find the DAG.** Click **Dags**, filter by the tag `01-basics`, and find `01_hello_airflow`.
   Note the schedule (`@daily`) and that it is **paused**.
3. **Read its docs.** Open the DAG and click **Dag Docs** — this is the docstring from the top of
   [`01_hello_airflow.py`](../dags/01_hello_airflow.py).
4. **Look at the shape.** Switch to the **Graph** view (`g`): `start → say_hello → print_context →
   end`. Open the **Code** tab and match each box to its operator in the code.
5. **Unpause it** with the toggle. Because `catchup=False`, Airflow creates **at most one** scheduled
   run (for the most recent `@daily` time), not one for every day since 2026-01-01. If such a run
   appears, that is expected.
6. **Trigger a run.** Click **Trigger**, keep the defaults, confirm. A new column appears in the
   **Grid**. Watch the squares go from no status → scheduled → queued → running → success (green).
   Each task takes a few seconds (`say_hello` sleeps 2 s).
7. **Read the logs.** Click the `say_hello` square → **Logs**. Find
   `Hello Airflow! This run is for 2026-…` — the `{{ ds }}` template was replaced by the run's date.
   Then open **Rendered Templates** to see the final `bash_command`.
8. **Look at XCom.** Click `print_context` → **Logs** (it prints the DAG id, run id, logical date and
   try number) → **XCom**: the return value `Hello from Python!` is stored under `return_value`.
9. **Re-run a task.** With `print_context` selected, click **Clear** and confirm. Its square resets
   and it runs again; in **Logs** you can now pick try 1 or try 2.
10. **Check the audit trail.** Open the DAG's **Audit Log** tab: your unpause, trigger and clear
    actions are recorded with your username.

Then pause the DAG again if you don't want a run every midnight.

## Common mistakes

- **Triggering a paused DAG** and waiting forever: the run stays `queued` until you unpause.
- **Looking for a DAG that has an import error.** It will not be in the list; check the import errors
  on Home or the banner on the Dags list.
- **Searching Admin → Connections/Variables for the tutorial ones.** They are env-var based and
  intentionally not listed.
- **Reading the wrong try's log** after retries — pick the try number in the Logs tab.
- **Using "Mark as success" to fix a pipeline.** It does not run the task; downstream tasks may then
  run on missing data. Prefer **Clear** after fixing the cause.

## Try it yourself

1. Trigger `10_params_variables_templating` with a different `customer_name` and `regions`, then
   compare the **Rendered Templates** of its Bash task between two runs.
2. Trigger `06_dynamic_task_mapping` and open a mapped task: how many instances were created, and
   what labels do they have?
3. Unpause the `09_assets_*` DAGs, trigger `09_assets_producer_orders`, then open **Assets →
   raw_orders** and find the event and its extra data. Which DAG runs started because of it?
4. Create a Postgres connection in **Admin → Connections** (host `postgres`, port 5432, login
   `airflow`, password `airflow`, database `demo`) and press **Test**.

---

**Next →** [26 · CLI and REST API](26-cli-and-rest-api.md) · [Back to the index](README.md)

Official docs: [UI overview](https://airflow.apache.org/docs/apache-airflow/stable/ui.html)
