# What is Apache Airflow?

**What you'll learn:** what "workflow orchestration" means, what Airflow is (and what it is not),
the handful of ideas everything else in this project builds on, and when Airflow is the right tool.

> **Run it**
> No code to run yet. When you have Airflow running ([03 · Installation](03-installation-and-setup.md)),
> open the DAG `01_hello_airflow` ([../dags/01_hello_airflow.py](../dags/01_hello_airflow.py)) —
> it is the smallest example of everything described on this page.

---

## The problem Airflow solves

Most data work is not one script. It is a *chain* of steps that must happen in the right order, on
a schedule, and keep working when something goes wrong:

1. Download yesterday's orders from an API.
2. Clean them and load them into a database.
3. Rebuild a summary table.
4. Check the numbers look sane.
5. Tell someone if anything failed.

You *could* glue these together with a shell script and a cron job. That works until:

- step 1 fails at 03:00 because the API was down for 30 seconds, and nothing retries it;
- step 3 runs even though step 2 failed, and publishes wrong numbers;
- nobody notices for two days because there is no dashboard;
- you need to re-run "the last 30 days" after fixing a bug;
- ten other pipelines now need the same thing.

**Workflow orchestration** is the job of deciding *what* runs, *when*, *in which order*, *on which
machine*, *what to do on failure*, and *showing you what happened*. A tool that does this is an
**orchestrator**. Apache Airflow is one of the most widely used open-source orchestrators.

## What Airflow is

Apache Airflow is a platform to **author, schedule and monitor workflows written in Python**.

- You describe a workflow as a Python file. Airflow calls this a **DAG** (more below).
- Airflow's **scheduler** starts each workflow at the right time and runs its steps (**tasks**) in
  the right order.
- Failed tasks can be **retried** automatically; you get **alerts** through callbacks.
- A web **UI** shows every run, every task, its logs and its history.
- Hundreds of **providers** (plug-in packages) give you ready-made building blocks for databases,
  clouds, APIs and SaaS tools.

## What Airflow is not

| Airflow is NOT… | Why it matters | Use instead / alongside |
|---|---|---|
| A data-processing engine | Airflow *tells other systems* what to do. Heavy work (joining 100 GB of data) should run in a database, Spark, dbt, a container… Airflow triggers it and waits. | Postgres, Spark, dbt, BigQuery, Snowflake |
| A streaming system | Airflow runs *batches*: a run starts, does work, finishes. It is not built to react to each event within milliseconds. | Kafka, Flink, Spark Streaming |
| A place to move large data between tasks | Tasks share small values (IDs, paths, counts) via **XCom**. Large data goes to files, buckets or tables. | S3/GCS, a database — see [08 · XCom](08-xcom.md) |
| A low-latency job queue | Scheduling has seconds of overhead. Fine for minutes/hours, wrong for "run this in 50 ms". | Celery, RQ, a message queue |

> 💡 A useful mental model: **Airflow is the conductor, not the orchestra.** It does not play the
> instruments; it makes sure each one starts at the right moment and notices when one stops.

## Real-world use cases

- **ETL / ELT pipelines** — extract from APIs and databases, load into a warehouse, transform with
  SQL. The capstone DAG [`20_capstone_sales_etl`](../dags/20_capstone_sales_etl.py) is a small
  version of this.
- **Machine-learning pipelines** — fetch features, train a model, evaluate it, deploy it if it is
  better than the current one.
- **Reporting** — build daily/weekly reports and email or publish them.
- **Infrastructure and operations** — create a temporary cluster, run jobs on it, always tear it
  down afterwards (see [20 · Setup & teardown](20-setup-teardown.md)).
- **Data-quality checks** — run SQL checks after every load and stop the pipeline if they fail.
- **Event-driven chains** — "when the orders table is updated, rebuild the dashboard" using
  **Assets** (see [13 · Assets](13-assets-data-aware-scheduling.md)).
- **Human approval steps** — pause a pipeline until a person approves (Airflow 3.1+, see
  [22 · Human-in-the-loop](22-human-in-the-loop.md)).

## The key ideas

You will meet these words on every page. Each has its own chapter later; the
[glossary](32-glossary.md) has one-line definitions.

### Workflows as code

A workflow is a normal Python file in the `dags/` folder. Because it is code you can put it in Git,
review it in pull requests, test it with `pytest` (see [27 · Testing DAGs](27-testing-dags.md)),
and generate many similar workflows from a loop (see
[21 · Dynamic DAG generation](21-dynamic-dag-generation.md)).

### DAG — Directed Acyclic Graph

A **DAG** is Airflow's word for a workflow. The name describes its shape:

- **Graph** — a set of tasks (boxes) connected by dependencies (arrows).
- **Directed** — arrows point one way: "A runs before B".
- **Acyclic** — no loops: you can never follow the arrows back to where you started. (Airflow 3.4
  adds *task loops* for "repeat until done", see [23 · Task loops](23-task-loops.md).)

Here is the DAG from [`01_hello_airflow.py`](../dags/01_hello_airflow.py):

```mermaid
flowchart LR
    start --> say_hello --> print_context --> end_task[end]
```

And the code that defines those arrows:

```python
start >> say_hello >> print_context >> end
```

### Tasks and operators

A **task** is one step in the DAG. You create tasks from **operators** — templates for a kind of
work (`BashOperator` runs a shell command, `PythonOperator` calls a Python function) — or with the
`@task` decorator on a plain Python function (the **TaskFlow** style, see
[06 · TaskFlow API](06-taskflow-api.md)).

### Scheduling

Each DAG has a `schedule`: a cron string like `"@daily"`, a `timedelta`, an **Asset** ("run when
this data changes"), or `None` (only when you trigger it by hand). `01_hello_airflow` uses:

```python
schedule="@daily",  # run once a day at midnight UTC (None = only manual runs)
start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),  # first date the DAG may run for
catchup=False,  # don't create runs for every missed day since start_date
```

See [18 · Scheduling](18-scheduling-and-timetables.md).

### Retries and failure handling

Things fail: networks blip, APIs rate-limit you. Airflow can retry a task automatically:

```python
default_args = {
    "owner": "tutorial",
    "retries": 1,  # re-try a failed task once...
    "retry_delay": timedelta(seconds=30),  # ...after waiting 30 seconds
}
```

Downstream tasks only run when their upstream tasks succeeded (unless you change the **trigger
rule**). See [16 · Error handling](16-error-handling-retries-alerts.md) and
[09 · Branching & trigger rules](09-branching-and-trigger-rules.md).

### Monitoring

Every run of every task is recorded in Airflow's **metadata database**. The UI shows a grid of
runs × tasks coloured by state, full logs for each attempt, how long things took, and what failed.
See [25 · UI tour](25-ui-tour.md).

## When to choose Airflow (and when not)

| Situation | Good choice |
|---|---|
| One script that runs once a day, no dependencies, failure is not a big deal | **cron** (or a scheduled CI job) is fine |
| Several steps with dependencies, retries, history, alerts, back-filling old dates | **Airflow** |
| Many pipelines owned by different people, needing one place to see them all | **Airflow** |
| Pipelines that depend on each other's data ("run B when A's table is updated") | **Airflow** (Assets) |
| Real-time processing of every event within milliseconds | A **streaming** tool (Kafka, Flink) |
| Long-running services (web servers, consumers that never stop) | Kubernetes, systemd, a PaaS — not an orchestrator |
| Purely in-warehouse SQL transformations | **dbt** — often *triggered by* Airflow |

Other orchestrators exist (for example Dagster, Prefect, Argo Workflows, cloud-native services such
as AWS Step Functions). They make different trade-offs. Airflow's strengths are its maturity, its
very large ecosystem of providers, its large community, and the fact that several clouds offer it as
a managed service.

### Airflow vs cron, side by side

| Need | cron | Airflow |
|---|---|---|
| Run at a time | ✅ | ✅ |
| Order between steps | Manual (`&&`) | Built-in dependencies |
| Automatic retries | ❌ | ✅ per task |
| History of every run | Log files you grep | UI + database |
| Re-run a past date ("backfill") | Manual | `airflow backfill create` or UI |
| Alerts on failure | DIY | Callbacks, notifiers |
| Parallelism limits (e.g. "max 2 API calls at once") | ❌ | Pools |
| Wait for a file / external event | DIY loop | Sensors, deferrable operators, Assets |

## A brief history

| Year | Milestone |
|---|---|
| 2014 | Maxime Beauchemin creates Airflow at **Airbnb** to manage their growing number of data pipelines. |
| 2015 | Airbnb open-sources it. |
| 2016 | Airflow enters the **Apache Software Foundation** incubator. |
| 2019 | It becomes a top-level Apache project. |
| Dec 2020 | **Airflow 2.0**: the **TaskFlow API** (`@task`), a highly-available scheduler, a stable REST API, and providers split into separate packages. Later 2.x releases add deferrable operators, dynamic task mapping and data-aware scheduling ("Datasets"). |
| Apr 2025 | **Airflow 3.0**: a new React **UI**; Datasets renamed to **Assets**; a separate **DAG processor**; the **Task SDK** (`airflow.sdk`) and the **Execution API**, so tasks no longer talk to the metadata database directly; DAG versioning; removal of long-deprecated features (SubDAGs, SLAs, `schedule_interval`, `execution_date`). |
| 2025 | **Airflow 3.1**: human-in-the-loop operators, deadline alerts (the replacement for SLAs). |
| 2026 | **Airflow 3.4** (used in this project): Python ≥ 3.11, task loops, retry policies, one DAG run per asset event by default. |

Airflow 2 and 3 differ in many details. If you read older tutorials or Stack Overflow answers, check
[31 · Airflow 2 vs 3](31-airflow-2-vs-3.md) before copying code.

## Common mistakes

- **Doing heavy data processing inside Airflow tasks** when a database or Spark could do it.
  Airflow workers are not sized for that; let Airflow *trigger* the heavy work.
- **Passing big data between tasks** through XCom. Write it to a file or table and pass the path.
- **Expecting real-time behaviour.** Airflow schedules in batches; seconds of delay are normal.
- **Copying Airflow 2 tutorials** (`schedule_interval`, `from airflow.operators.python import …`,
  `execution_date`). This project uses Airflow 3 imports from `airflow.sdk` and the
  `airflow.providers.standard` package.
- **Thinking a DAG file is "run" like a script.** Airflow *parses* the file to learn the shape of the
  workflow; the task code runs later, separately. See [02 · Architecture](02-architecture.md).

## Try it yourself

1. Write down a process from your own work (or life) that has 4–6 steps. Draw it as boxes and arrows.
   Is it acyclic? Which steps could run in parallel?
2. Open [`01_hello_airflow.py`](../dags/01_hello_airflow.py) and find the four tasks and the line
   that connects them. Without running anything, predict what each task will print.
3. For each row of the "When to choose Airflow" table, think of one real example you know.
4. Read the docstring at the top of [`20_capstone_sales_etl.py`](../dags/20_capstone_sales_etl.py)
   and list which of the "key ideas" above it uses.

---

**Next →** [02 · Architecture](02-architecture.md) · [Back to the index](README.md)

Official docs: [Airflow overview](https://airflow.apache.org/docs/apache-airflow/stable/index.html)
