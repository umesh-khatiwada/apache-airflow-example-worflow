# Exercises and mini-projects

**What you'll learn:** you'll practise every chapter by changing the real example DAGs, from small edits to "stretch" tasks that need some research, and then build three small pipelines of your own. Hints are hidden in collapsible blocks so you can try first. The very first exercise has a full worked solution to show the expected level of detail.

> **Run it**
> - Work on **copies**: copy an example to a new file with a new `dag_id` (e.g. `dags/my_01_hello.py` with `dag_id="my_01_hello"`), so the originals and the tests in [../tests/](../tests/) keep working. If you edit the originals, `git diff` and `git checkout -- dags/` take you back.
> - New DAGs start **paused**: unpause them in the UI.
> - Files are picked up within about 30 seconds. If a DAG doesn't appear, run `airflow dags list-import-errors` ([30-troubleshooting-faq.md](30-troubleshooting-faq.md)).
> - Quick check without the UI: `source scripts/local_env.sh && airflow dags test <dag_id>` (runs the whole DAG in one process) or `airflow tasks test <dag_id> <task_id>`.

Difficulty: 🟢 beginner · 🟡 intermediate · 🔴 stretch

---

## Part 1: exercises per example

### 01 · Hello Airflow · [../dags/01_hello_airflow.py](../dags/01_hello_airflow.py) · [05-first-dag.md](05-first-dag.md)

**1.1 🟢 Add a goodbye task.** Add a `BashOperator` called `say_goodbye` that prints `Goodbye from {{ dag.dag_id }}!` and runs between `print_context` and `end`.

<details><summary>Worked solution</summary>

1. Copy the file: `cp dags/01_hello_airflow.py dags/my_01_hello.py`.
2. In the copy, change `dag_id="01_hello_airflow"` to `dag_id="my_01_hello"`. (Two DAGs with the same id would hide each other.)
3. Inside the `with DAG(...)` block, after `print_context`, add:
   ```python
   say_goodbye = BashOperator(
       task_id="say_goodbye",
       bash_command='echo "Goodbye from {{ dag.dag_id }}!"',
   )
   ```
   `bash_command` is a template field, so `{{ dag.dag_id }}` is filled in at run time.
4. Replace the dependency line with:
   ```python
   start >> say_hello >> print_context >> say_goodbye >> end
   ```
5. Wait ~30 s, find `my_01_hello` in the UI, unpause it, press **Trigger**. In the Graph view you now see five boxes in a line. Open `say_goodbye` → **Logs**: it prints `Goodbye from my_01_hello!`. **Rendered Template** shows the command after rendering.
6. Bonus check from the terminal: `source scripts/local_env.sh && airflow tasks test my_01_hello say_goodbye 2026-11-11`.
7. Delete the copy when you're done (or keep it as your playground).

</details>

**1.2 🟢 Retry on your own.** Give only `say_hello` `retries=3` and `retry_delay=timedelta(seconds=10)`, and make its command fail (`exit 1`). Watch the try number grow in the log header.

<details><summary>Hint</summary>

Arguments passed to an operator override `default_args`. The task goes to *up_for_retry* (yellow/orange) between tries.

</details>

**1.3 🔴 A `start_date` experiment.** Set `catchup=True` and `start_date` to three days ago (a fixed date, not `now()`). Unpause and count the runs created. Then explain why `schedule="@daily"` with `catchup=True` creates the number of runs it does.

<details><summary>Hint</summary>

In Airflow 3, `"@daily"` is a `CronTriggerTimetable`: one run per midnight between `start_date` and now. Compare `logical_date` and `run_after` in the Runs list. [18-scheduling-and-timetables.md](18-scheduling-and-timetables.md) has the details.

</details>

### 02 · TaskFlow API · [../dags/02_taskflow_api.py](../dags/02_taskflow_api.py) · [06-taskflow-api.md](06-taskflow-api.md)

**2.1 🟢 Add an average.** Make `transform` also return `"average"` (total ÷ count) and print it in `load`.

<details><summary>Hint</summary>

`transform` has `multiple_outputs=True`, so a new key is available as `summary["average"]`. Add a parameter to `load` and pass it in the call.

</details>

**2.2 🟡 Unit-test the new logic.** Extend `test_taskflow_transform_logic` in [../tests/test_dag_structure.py](../tests/test_dag_structure.py) (in a copy of the test) to check the average.

<details><summary>Hint</summary>

`dag.get_task("transform").python_callable` is the plain function: call it with a dict and assert on the result. Run with `make test`.

</details>

**2.3 🔴 Replace the classic operator.** Rewrite `classic_bash_reads_xcom` as a `@task.bash` that receives `message` as an argument instead of using `ti.xcom_pull` in a template.

<details><summary>Hint</summary>

`report` already shows the pattern: a `@task.bash` function returns the command string.

</details>

### 03 · Operators & dependencies · [../dags/03_operators_and_dependencies.py](../dags/03_operators_and_dependencies.py) · [07-operators-and-dependencies.md](07-operators-and-dependencies.md)

**3.1 🟢 Change the exit code.** Change `bash_skip` to `exit 1` instead of `exit 99`. What state do `bash_skip` and `fan_in` end in, and why?

<details><summary>Hint</summary>

Exit code 99 means *skipped*; any other non-zero code means *failed*. `fan_in` uses `TriggerRule.NONE_FAILED`.

</details>

**3.2 🟡 Rewire with `chain`.** Replace `begin >> fan_out >> [bash_with_env, python_args, bash_skip] >> fan_in` with a single `chain(...)` call that builds the same graph. Compare the Graph view before and after.

<details><summary>Hint</summary>

Lists inside `chain()` fan out and fan in: `chain(begin, fan_out, [a, b, c], fan_in)`.

</details>

**3.3 🔴 Wait for the triggered DAG.** Make `trigger_hello` wait until `01_hello_airflow` finishes, and fail if that run fails.

<details><summary>Hint</summary>

Look at `wait_for_completion`, `poke_interval`, `allowed_states` and `failed_states` on `TriggerDagRunOperator`. With `deferrable=True` the wait uses the triggerer instead of a worker slot. `01_hello_airflow` must be unpaused.

</details>

### 04 · XCom · [../dags/04_xcom.py](../dags/04_xcom.py) · [08-xcom.md](08-xcom.md)

**4.1 🟢 Find the values.** Run the DAG and, in the UI's **XCom** tab, find every key that `push_with_keys` stored. Which task has an XCom with key `return_value` of `null`?

<details><summary>Hint</summary>

A `@task` that returns nothing still pushes `return_value` = `None`.

</details>

**4.2 🟡 Pull from a list.** Add a task that pulls `row_count` and `file_path` in one call and prints both.

<details><summary>Hint</summary>

`ti.xcom_pull(task_ids="push_with_keys", key=...)` takes one key at a time; call it twice, or pull the same key from several task ids. Remember to declare the dependency yourself (`keys >> new_task`), because explicit pulls don't create one.

</details>

**4.3 🔴 Big data the right way.** Add a task that generates 100,000 random numbers. Instead of returning them, write them to `DATA_DIR / "xcom_demo" / f"numbers_{ts_nodash}.json"` and return only the path; a downstream task reads the file and prints the sum.

<details><summary>Hint</summary>

Import `DATA_DIR` from `tutorial_lib.paths` (as `08_*` does). Trigger from the UI so `ts_nodash` exists ([30-troubleshooting-faq.md](30-troubleshooting-faq.md)).

</details>

### 05 · Branching & trigger rules · [../dags/05_branching_and_trigger_rules.py](../dags/05_branching_and_trigger_rules.py) · [09-branching-and-trigger-rules.md](09-branching-and-trigger-rules.md)

**5.1 🟢 Trigger with config.** Trigger three times: `order_count=5`; `order_count=500`; `make_it_fail=true`. Note which tasks are skipped, failed or successful each time.

<details><summary>Hint</summary>

**Trigger** opens a form built from the DAG's `params`. `on_any_failure` uses `ONE_FAILED`; `cleanup` uses `ALL_DONE`.

</details>

**5.2 🟡 A third branch.** Add `process_huge` for more than 1,000 orders. Update `choose_plan` and the wiring, with a `Label`.

<details><summary>Hint</summary>

The branch function returns the **task id** to follow. Every possible target must be a direct downstream of the branch task.

</details>

**5.3 🔴 Weekend-aware report.** Make `is_business_day` read the `run_after` time when `logical_date` is `None`, instead of always returning `True`.

<details><summary>Hint</summary>

Add `dag_run=None` to the function's parameters; `dag_run.run_after` is always set.

</details>

### 06 · Dynamic task mapping · [../dags/06_dynamic_task_mapping.py](../dags/06_dynamic_task_mapping.py) · [10-dynamic-task-mapping.md](10-dynamic-task-mapping.md)

**6.1 🟢 More squares.** Map `square` over `range(1, 11)` and check that `total` prints 385.

<details><summary>Hint</summary>

`.expand(x=list(range(1, 11)))`. In the Grid view, click the mapped task `[ ]` to see ten instances.

</details>

**6.2 🟡 Real files.** Make `list_files` return the names of the three sample CSVs in `dags/include/data/` by listing the folder at run time.

<details><summary>Hint</summary>

`from tutorial_lib.paths import SAMPLE_DATA_DIR`, then `sorted(p.name for p in SAMPLE_DATA_DIR.glob("*.csv"))`, **inside** the task, not at the top of the file ([28-best-practices.md](28-best-practices.md)).

</details>

**6.3 🔴 Filter before mapping.** Make `process_file` create only two instances by leaving out files whose name contains `"east"`.

<details><summary>Hint</summary>

`.map()` transforms each item one-to-one, so the number of mapped instances stays three; at best an unwanted item ends up *skipped* (see the official docs on transforming mapped data). To really get two instances, put a small `@task` between `list_files` and `process_file` that filters the list, and expand over its output.

</details>

### 07 · Task groups · [../dags/07_task_groups.py](../dags/07_task_groups.py) · [11-task-groups.md](11-task-groups.md)

**7.1 🟢 Reuse the group.** Add a third `ingest` instance for `"products"` and wire it like the others.

<details><summary>Hint</summary>

`ingest.override(group_id="ingest_products")("products")`, and add it to the list after `start`.

</details>

**7.2 🟡 Group defaults.** Give the `clean` group its own `default_args={"retries": 5}`. Which retry count do `dedupe` and `fix_types` end up with? Check with `airflow tasks list` or a small test.

<details><summary>Hint</summary>

The innermost group's `default_args` win. `dag.get_task("transform.clean.dedupe").retries` tells you in a test.

</details>

**7.3 🔴 Mapped group from upstream.** Make `publish` expand over the output of a new task `get_destinations()` instead of a fixed list.

<details><summary>Hint</summary>

Mapped task groups can expand over an XComArg just like tasks: `publish.expand(destination=get_destinations())`.

</details>

### 08 · Sensors & deferrable · [../dags/08_sensors_and_deferrable.py](../dags/08_sensors_and_deferrable.py) · [12-sensors-and-deferrable.md](12-sensors-and-deferrable.md)

**8.1 🟢 Make a sensor time out.** Change `drop_file`'s `time.sleep(15)` to 120 seconds and `wait_with_decorator`'s timeout to 60. What state does `wait_with_decorator` end in, and why not *failed*?

<details><summary>Hint</summary>

Look at `soft_fail=True`.

</details>

**8.2 🟡 Poke vs reschedule.** While the DAG runs, open **Admin ▸ Pools** (`default_pool`) and compare the running slots taken by `wait_for_file` (poke) and `wait_for_rows` (reschedule).

<details><summary>Hint</summary>

A rescheduled sensor goes to *up_for_reschedule* between checks and doesn't hold a slot.

</details>

**8.3 🔴 Deferrable file sensor.** Make `wait_for_file` deferrable and stop the triggerer (`docker compose stop airflow-triggerer`) during a run. Describe what you see, then restart it.

<details><summary>Hint</summary>

`FileSensor` accepts `deferrable=True`. Deferred tasks show as *deferred* and wait for the triggerer.

</details>

### 09 · Assets · [../dags/09_assets_data_aware_scheduling.py](../dags/09_assets_data_aware_scheduling.py) · [13-assets-data-aware-scheduling.md](13-assets-data-aware-scheduling.md)

**9.1 🟢 Watch the chain reaction.** Unpause all `09_*` DAGs and `tutorial_daily_kpis`, trigger `09_assets_producer_orders`, and list every DAG that starts on its own. Then trigger `09_assets_producer_customers` and explain what `09_assets_consumer_and_or` does.

<details><summary>Hint</summary>

`&` waits for **both** assets to have new events. The **Assets** page shows the lineage graph.

</details>

**9.2 🟡 Use the extras.** In `09_assets_consumer`, read `row_count` from the event's `extra` and fail the task if it's 0.

<details><summary>Hint</summary>

`event.extra["row_count"]` inside the loop over `triggering_asset_events`. Raise `AirflowFailException` from `airflow.sdk.exceptions`.

</details>

**9.3 🔴 One run per event vs batching.** Trigger the orders producer three times quickly. How many consumer runs appear? Then look up `batch_asset_events` and try it in a copy of the consumer.

<details><summary>Hint</summary>

Airflow 3.4 creates one run per asset event by default; `batch_asset_events=True` on the timetable restores batching ([13-assets-data-aware-scheduling.md](13-assets-data-aware-scheduling.md)).

</details>

### 10 · Params, Variables & templating · [../dags/10_params_variables_templating.py](../dags/10_params_variables_templating.py) · [14-params-variables-templating.md](14-params-variables-templating.md)

**10.1 🟢 Break the validation.** In the trigger form, set `max_rows` to 0 and `customer_name` to `"A"`. What does the form say? Why?

<details><summary>Hint</summary>

`minimum=1` and `minLength=2` are JSON-schema rules on the `Param`s.

</details>

**10.2 🟡 Change a Variable.** Create a Variable `environment` = `staging` in **Admin ▸ Variables** and run again. Which value wins, the UI one or `AIRFLOW_VAR_ENVIRONMENT=docker-local` from the compose file?

<details><summary>Hint</summary>

Lookup order: secrets backend → environment variables → metadata DB.

</details>

**10.3 🔴 Your own macro.** Add a macro `fiscal_quarter(ds)` to `user_defined_macros` that returns e.g. `"FY26-Q4"`, use it in `show_params_vars_macros`, and check **Rendered Template**.

<details><summary>Hint</summary>

Follow `days_between`: a plain function at the top of the file, added to the dict, called as `{{ fiscal_quarter(ds) }}`.

</details>

### 11 · Connections & hooks · [../dags/11_connections_and_hooks.py](../dags/11_connections_and_hooks.py) · [15-connections-and-hooks.md](15-connections-and-hooks.md)

**11.1 🟢 Add a city.** Add your own city to `CITIES` and check the new row with `make psql` → `SELECT * FROM weather_observations ORDER BY observed_at DESC LIMIT 5;`.

<details><summary>Hint</summary>

Each dict needs `city`, `latitude`, `longitude` (the names `fetch_weather` expects through `expand_kwargs`).

</details>

**11.2 🟡 Make the load idempotent.** Clear `save_to_postgres` twice and count rows for that `run_id`: they grow. Fix it so a re-run replaces the rows.

<details><summary>Hint</summary>

Copy the capstone pattern ([24-capstone-sales-etl.md](24-capstone-sales-etl.md)): `hook.run("DELETE FROM weather_observations WHERE run_id = %(run_id)s", parameters={"run_id": run_id})` before `insert_rows`.

</details>

**11.3 🔴 A new connection.** Add a second HTTP connection, `AIRFLOW_CONN_OPEN_METEO_ARCHIVE`, pointing at `https://archive-api.open-meteo.com`, and a hook method that fetches yesterday's temperature from it.

<details><summary>Hint</summary>

Add the env var to `docker-compose.yaml` **and** `scripts/local_env.sh`, then `docker compose up -d` to recreate the containers. Read [15-connections-and-hooks.md](15-connections-and-hooks.md) on env-var naming. Keep the offline fallback.

</details>

### 12 · Error handling & alerts · [../dags/12_error_handling_and_alerts.py](../dags/12_error_handling_and_alerts.py) · [16-error-handling-retries-alerts.md](16-error-handling-retries-alerts.md)

**12.1 🟢 Read the callbacks.** Run once with defaults and find the `ALERT` and `will be retried` lines from `tutorial_lib/callbacks.py` in the task logs.

<details><summary>Hint</summary>

`flaky_api_call` fails twice before succeeding, so `task_retry_alert` runs twice.

</details>

**12.2 🟡 Trigger the timeout.** Run with `sleep_seconds=15`. What exception appears in `slow_task`'s log, and does `summary` still run?

<details><summary>Hint</summary>

`execution_timeout=timedelta(seconds=10)`; `summary` uses `ALL_DONE`.

</details>

**12.3 🔴 Extend the retry policy.** Add a `RateLimitedError` rule that retries after 30 seconds, and make `smart_retries` raise it on try 2.

<details><summary>Hint</summary>

Add one more `RetryRule(exception=..., action=RetryAction.RETRY, retry_delay=...)` to `SMART_RETRY_POLICY`. Check the try timestamps in the log.

</details>

### 13 · Custom operators, hooks, sensors · [../dags/13_custom_operators_hooks_sensors.py](../dags/13_custom_operators_hooks_sensors.py) · [17-custom-operators-hooks-plugins.md](17-custom-operators-hooks-plugins.md)

**13.1 🟢 Another city.** Add a third `FetchWeatherOperator` task and include it in `compare` (you'll need to change its signature).

<details><summary>Hint</summary>

`compare` could take a list of weather dicts instead of `weather_a` and `weather_b`.

</details>

**13.2 🟡 A new quality rule.** Add a `max_rows` argument to `CsvQualityCheckOperator` and a unit test for it in a copy of [../tests/test_custom_components.py](../tests/test_custom_components.py).

<details><summary>Hint</summary>

Store it in `__init__`, check it in `execute()`, append to `problems`. Tests call `op.execute(context={})` directly.

</details>

**13.3 🔴 Make a field templated.** Make `min_rows` of `CsvHasRowsSensor` templated so `min_rows="{{ params.min_rows }}"` works. What type does the rendered value have?

<details><summary>Hint</summary>

Add it to `template_fields`. Rendered templates are strings unless the DAG sets `render_template_as_native_obj=True`; convert with `int()` in `poke()`.

</details>

### 14 · Scheduling & timetables · [../dags/14_scheduling_and_timetables.py](../dags/14_scheduling_and_timetables.py) · [18-scheduling-and-timetables.md](18-scheduling-and-timetables.md)

**14.1 🟢 Compare timing.** Unpause `14_schedule_catchup_backfill`. Compare `logical_date`, `data_interval_start/end` and `run_after` printed by `show_timing` for two of its runs.

<details><summary>Hint</summary>

It's a data-interval timetable with `catchup=True` and an `end_date`, limited to 2 parallel runs.

</details>

**14.2 🟡 Add a schedule.** Add an entry to `SCHEDULES` that runs every 15 minutes during office hours (09:00–17:45, Monday–Friday) in your local timezone.

<details><summary>Hint</summary>

`CronTriggerTimetable("*/15 9-17 * * 1-5", timezone="...")`. The DAG id must start with `14_` or `test_dag_ids_match_their_file_number` fails.

</details>

**14.3 🔴 Backfill from the CLI.** Use `airflow backfill create` with `--dry-run` to preview a backfill of `14_schedule_catchup_backfill` for 2026-11-01 to 2026-11-03, then run it with `--reprocess-behavior completed` and watch it in the UI.

<details><summary>Hint</summary>

`airflow backfill create --help` lists the flags. [26-cli-and-rest-api.md](26-cli-and-rest-api.md).

</details>

### 15 · Pools, priority & concurrency · [../dags/15_pools_priority_concurrency.py](../dags/15_pools_priority_concurrency.py) · [19-pools-priority-concurrency.md](19-pools-priority-concurrency.md)

**15.1 🟢 Resize a pool.** Change `api_pool` to 4 slots in **Admin ▸ Pools** and re-run. How much faster do the six `call_api` instances finish?

<details><summary>Hint</summary>

Each sleeps 8 seconds: 3 waves of 2 vs 2 waves of up to 4.

</details>

**15.2 🟡 Priority matters.** Set `urgent_report`'s `priority_weight` to 1. Does it still start first? Run twice and compare.

<details><summary>Hint</summary>

With equal weights, the order among queued tasks isn't guaranteed.

</details>

**15.3 🔴 Pools as code.** Add a `sensor_pool` (3 slots) to [../config/pools.json](../config/pools.json), import it without restarting, and put the sensors of `08_*` in it.

<details><summary>Hint</summary>

`docker compose run --rm airflow-cli airflow pools import /opt/airflow/config/pools.json`. Sensors accept `pool=` like any operator.

</details>

### 16 · Setup & teardown · [../dags/16_setup_teardown.py](../dags/16_setup_teardown.py) · [20-setup-teardown.md](20-setup-teardown.md)

**16.1 🟢 Fail on purpose.** Trigger with `fail_processing=true`. Check that `data/workspaces/<ts_nodash>` was deleted, and note the final DAG run state.

<details><summary>Hint</summary>

Teardowns run even when work fails, but don't hide the failure.

</details>

**16.2 🟡 A second worker task.** Add a task `count_files` that uses the workspace in parallel with `process`. Does the teardown wait for it?

<details><summary>Hint</summary>

Tasks between a setup and its teardown are in its *scope*. Look at the Graph view.

</details>

**16.3 🔴 Classic-operator teardown on failure.** Make `use_fake_service` fail (`exit 1`) and confirm `stop_fake_service` still runs. Then remove `.as_teardown(...)` and compare.

<details><summary>Hint</summary>

Without `as_teardown`, `stop_fake_service` is a normal task with the default `all_success` trigger rule.

</details>

### 17 · Dynamic DAG generation · [../dags/17_dynamic_dag_generation.py](../dags/17_dynamic_dag_generation.py) · [21-dynamic-dag-generation.md](21-dynamic-dag-generation.md)

**17.1 🟢 A new source.** Add `"hr": {"schedule": "@weekly", "tables": ["employees"], "owner": "people-eng"}` and find the new DAG in the UI.

<details><summary>Hint</summary>

The loop at the bottom builds one DAG per entry. Filter the DAG list by tag `source:hr`.

</details>

**17.2 🟡 Config from a file.** Move `SOURCES` into `dags/include/sources.json` and load it at the top of the file.

<details><summary>Hint</summary>

Reading a small local file is fine at parse time; a database or API call is not. `include/` is already in `.airflowignore`. Build the path from `Path(__file__).parent / "include" / "sources.json"`.

</details>

**17.3 🔴 Per-source alerts.** Add an optional `"alert_on_failure": true` field and, when set, attach `task_failure_alert` from `tutorial_lib.callbacks` as `on_failure_callback` in that DAG's `default_args`.

<details><summary>Hint</summary>

Build `default_args` as a dict in `build_ingest_dag` and add the key conditionally.

</details>

### 18 · Human-in-the-loop · [../dags/18_human_in_the_loop.py](../dags/18_human_in_the_loop.py) · [22-human-in-the-loop.md](22-human-in-the-loop.md)

**18.1 🟢 Approve and reject.** Run the DAG twice: approve once, reject once. Which tasks are skipped after a rejection?

<details><summary>Hint</summary>

Respond in **Browse ▸ Required Actions** or by clicking the waiting task.

</details>

**18.2 🟡 Add a timeout.** Give `choose_channel` a `response_timeout` of 5 minutes and `defaults=["publish_to_email"]`. Don't answer, and check what happens.

<details><summary>Hint</summary>

When the timeout passes, the defaults are used as if someone had chosen them.

</details>

**18.3 🔴 Use the answer.** Make `publish_to_email` print the reviewer's notes from `reviewer_notes`.

<details><summary>Hint</summary>

The `body` of `approve_publish` already reads them: `ti.xcom_pull(task_ids='reviewer_notes')['params_input']['notes']`.

</details>

### 19 · Task loops · [../dags/19_task_loops.py](../dags/19_task_loops.py) · [23-task-loops.md](23-task-loops.md)

**19.1 🟢 Count to five.** Change `19_task_loop_fixed` to 5 iterations and make `report` print the right total.

<details><summary>Hint</summary>

`max_iterations=5`; 5 × 10 = 50.

</details>

**19.2 🟡 Hit the safety cap.** Make `check_status` grow progress by 5% instead of 25% per iteration. What happens when `max_iterations=10` is reached before `done`?

<details><summary>Hint</summary>

Look at the loop group's final state and the `LoopDecision` reasons in the log. [23-task-loops.md](23-task-loops.md) describes what happens at the cap.

</details>

**19.3 🔴 Use the previous result.** Rewrite `check_status` so progress is `loop.previous["progress"] + 25` (starting from 0 on the first iteration) instead of being computed from `loop.index`.

<details><summary>Hint</summary>

`add_ten` in `19_task_loop_fixed` shows how to handle the first iteration (`loop.index == 1`).

</details>

### 20 · Capstone sales ETL · [../dags/20_capstone_sales_etl.py](../dags/20_capstone_sales_etl.py) · [24-capstone-sales-etl.md](24-capstone-sales-etl.md)

**20.1 🟢 Find the rejects.** Run the capstone and find which orders were rejected and why (two of them, in `south` and `west`).

<details><summary>Hint</summary>

The `transform` logs print `kept ... rows, rejected ... -> [...]`, and `publish` adds `rejected` to the asset event's extra. Look at `quantity` in the CSVs.

</details>

**20.2 🟡 A new region.** Add `dags/include/data/sales_east.csv` with the same columns and add `"east"` to `REGIONS`. Check the summary with `make psql`.

<details><summary>Hint</summary>

`SELECT * FROM sales_daily_summary ORDER BY batch_id DESC LIMIT 10;`. `region` is `VARCHAR(20)` and `order_id` + `batch_id` is the primary key of `sales_clean`, so keep order ids unique across files.

</details>

**20.3 🔴 Break the quality gate.** Duplicate an `order_id` inside one CSV. Which task fails first: `validate_raw_file`, `load_to_postgres` or `column_checks`? Why? Is the report DAG triggered?

<details><summary>Hint</summary>

Think about the `PRIMARY KEY (order_id, batch_id)` in [../dags/include/sql/create_sales_tables.sql](../dags/include/sql/create_sales_tables.sql) versus the `unique_check` in `column_checks`.

</details>

---

## Part 2: mini-projects

Each mini-project is a new DAG file you write from scratch, reusing what the examples taught. Write tests for them too ([27-testing-dags.md](27-testing-dags.md)), and review them with the checklist in [28-best-practices.md](28-best-practices.md).

### Mini-project A: weather alert pipeline 🟡

**Goal:** every hour, fetch the current weather for a list of cities and alert when a threshold is crossed.

**Requirements**

1. DAG `weather_alerts_hourly`, scheduled hourly, `catchup=False`, tagged.
2. A `Param` or Variable holds the thresholds, e.g. `{"max_temp": 30, "max_wind": 40}`.
3. One mapped task per city fetches weather with `OpenMeteoHook` from `tutorial_lib.hooks.open_meteo`, limited by `api_pool`.
4. Observations are stored idempotently in a new table `weather_alert_observations` in the `demo` database.
5. A task finds cities above a threshold. If none, everything after it is **skipped** (short-circuit).
6. An alert task "notifies" (a log line via `LogNotifier` is fine) with the list of cities and values.
7. Failures of any task call a failure callback; the run must finish within 15 minutes (deadline alert).

<details><summary>Hints</summary>

- Copy the structure of `fetch_weather` / `save_to_postgres` from `11_connections_and_hooks`, including `expand_kwargs(CITIES)`.
- Use `CREATE TABLE IF NOT EXISTS` in a `.sql` file under `dags/include/sql/` with `template_searchpath`.
- Idempotency: delete rows for the current `run_id` (or the hour from `logical_date`) before inserting.
- `@task.short_circuit` returns `False` to skip downstream ([09-branching-and-trigger-rules.md](09-branching-and-trigger-rules.md)).
- Deadline alerts: copy the `deadline=DeadlineAlert(...)` block from `12_error_handling_and_alerts`.
- Offline? The hook's fallback returns deterministic fake data, so pick thresholds that trigger an alert for at least one city.

</details>

### Mini-project B: file-drop ingestion with sensors, mapping and quality checks 🔴

**Goal:** when CSV files land in `data/landing/`, validate each one, load the good ones and quarantine the bad ones.

**Requirements**

1. DAG `file_drop_ingest`, scheduled every 10 minutes.
2. A **deferrable or reschedule-mode** sensor waits (max 9 minutes) until at least one `*.csv` is in `data/landing/`; if none arrive, the run is **skipped**, not failed.
3. A task lists the files; the rest of the pipeline is a **mapped task group** with one instance per file:
   - validate with `CsvQualityCheckOperator` (columns as in the capstone);
   - on success, load the rows into `sales_clean` (or a new table) idempotently, keyed by file name;
   - move the file to `data/processed/` on success or `data/quarantine/` on failure.
4. After all files: SQL checks (`SQLColumnCheckOperator` / `SQLTableCheckOperator`) on what was loaded.
5. On success, emit an asset `landing_sales` with the number of files and rows in its extra; a second DAG scheduled on that asset prints a summary.
6. Use `map_index_template` so the UI shows file names instead of 0, 1, 2.

<details><summary>Hints</summary>

- Sensor: `FileSensor` with a glob in `filepath` and `fs_conn_id="fs_default"`, or a `@task.sensor` that checks `Path(DATA_DIR / "landing").glob("*.csv")`. `soft_fail=True` skips instead of failing on timeout.
- Mapped task groups: `publish.expand(...)` in `07_task_groups` and `process_region.expand(...)` in the capstone.
- "Move to quarantine on failure": a task with `trigger_rule=TriggerRule.ALL_FAILED` or `ONE_FAILED` after the validation task, or catch the exception yourself and return a status.
- Drop test files with `cp dags/include/data/sales_north.csv data/landing/` and make a broken one by deleting a column.
- Each file is processed once; think about what happens if two runs overlap (`max_active_runs=1`).

</details>

### Mini-project C: approval-gated weekly report 🟡

**Goal:** build a weekly sales report from the capstone's tables, let a person approve it, and publish only approved reports.

**Requirements**

1. DAG `weekly_sales_report`, scheduled Mondays 08:00 in your timezone.
2. A task queries `sales_daily_summary` (from the capstone) for totals per region and writes a Markdown file to `data/reports/` (return only the path).
3. A quality gate: if total revenue is 0 or a region is missing, fail before asking anyone.
4. An `ApprovalOperator` shows the totals in its `body` and times out after 2 hours with `Reject` as the default.
5. On approval, an `HITLBranchOperator` lets the reviewer choose one or more channels (`publish_to_archive`, `publish_to_log`); each channel is a task.
6. A teardown task always deletes temporary files, even on rejection or failure.
7. The report path, the approver's decision and the chosen channels are visible in XCom.

<details><summary>Hints</summary>

- Schedule: `CronTriggerTimetable("0 8 * * 1", timezone="...")` ([18-scheduling-and-timetables.md](18-scheduling-and-timetables.md)).
- Querying: `PostgresHook(...).get_records(...)` as in `20_capstone_sales_report`.
- Body text: Jinja with `ti.xcom_pull(...)`, like `reviewer_notes` and `approve_publish` in `18_human_in_the_loop`.
- `HITLBranchOperator(options=[...])` options must match downstream task ids.
- Setup/teardown: `@setup` creates `data/reports/tmp_<ts_nodash>`, `@teardown` removes it ([20-setup-teardown.md](20-setup-teardown.md)).
- Run the capstone first so there's data to report on.

</details>

---

## Common mistakes

- **Editing the originals and breaking the tests.** Work on copies with new DAG ids, or restore with `git checkout -- dags/`.
- **Forgetting to unpause** a new DAG, then waiting for a run that never starts.
- **Copying a DAG without changing its `dag_id`.** One of the two disappears.
- **Testing a date-dependent DAG from the CLI without a logical date.** Pass one to `airflow dags trigger -l ...` or use the UI.
- **Skipping the hints entirely.** They often point at the line in an example that solves half the problem.

## Try it yourself

1. Pick the exercise you found hardest and write a short note (in a comment at the top of your copy) explaining *why* the solution works.
2. Turn one mini-project into a pull request on your own fork and review it with the checklist in [28-best-practices.md](28-best-practices.md).
3. Add an integrity test that checks your mini-project DAGs have an owner, tags and retries ([27-testing-dags.md](27-testing-dags.md)).

---

**Next →** [34 · Deploy to DigitalOcean with SkyPilot](34-deploy-digitalocean-skypilot.md) — run this project on a cloud VM. Or go back to the [index](README.md).
