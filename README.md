# Apache Airflow 3.4 — Learn by Example

A complete, beginner-friendly learning project for [Apache Airflow](https://airflow.apache.org).
It has **20 runnable example DAG files (37 DAGs)** that cover every major feature, a realistic
capstone ETL pipeline, custom operators/hooks/sensors, a test suite, and **34 documentation chapters**
that explain everything from "what is a DAG?" to production deployment.

Everything targets **Airflow 3.4.0** (released November 2026) and uses the modern `airflow.sdk` API.

---

## Quick start (Docker, ~5 minutes)

**You need:** [Docker Desktop](https://www.docker.com/products/docker-desktop/) with at least 4 GB of memory
for Docker, plus `make` (pre-installed on macOS and Linux).

```bash
make init
```

```bash
make up
```

Then open **http://localhost:8080** and log in with **airflow / airflow**.

> New DAGs start **paused**. Click the toggle next to a DAG to unpause it, then press ▶ **Trigger**.
> Start with `01_hello_airflow`.

No `make`? The same steps by hand:

```bash
cp .env.example .env
```

```bash
docker compose up airflow-init
```

```bash
docker compose up -d
```

Stop with `make down`. Delete everything, including the database, with `make reset`.
More options, including running without Docker, are in
[docs/03-installation-and-setup.md](docs/03-installation-and-setup.md).

---

## Run it in the cloud (temporary VM, auto-deletes)

Launch the same stack on a DigitalOcean droplet with [SkyPilot](https://docs.skypilot.co). The VM
deletes itself after 45 minutes, and the UI is reachable only through an SSH tunnel:

```bash
make cloud-up
```

```bash
make cloud-tunnel
```

Then open **http://localhost:8080** (user `airflow`; password from `make cloud-password`).
One-time setup (SkyPilot plus the DigitalOcean token from Bitwarden), security notes and
troubleshooting are in [docs/34-deploy-digitalocean-skypilot.md](docs/34-deploy-digitalocean-skypilot.md).

---

## Learning path

Follow the chapters in order. Each one explains a feature and points at a DAG you can run.

| # | Topic | Example DAG file | Guide |
|---|-------|------------------|-------|
| — | What Airflow is, architecture, setup, core concepts | — | [01](docs/01-what-is-airflow.md) · [02](docs/02-architecture.md) · [03](docs/03-installation-and-setup.md) · [04](docs/04-core-concepts.md) |
| 1 | Your first DAG | [`01_hello_airflow.py`](dags/01_hello_airflow.py) | [05](docs/05-first-dag.md) |
| 2 | TaskFlow API (`@dag`, `@task`) | [`02_taskflow_api.py`](dags/02_taskflow_api.py) | [06](docs/06-taskflow-api.md) |
| 3 | Operators & dependencies | [`03_operators_and_dependencies.py`](dags/03_operators_and_dependencies.py) | [07](docs/07-operators-and-dependencies.md) |
| 4 | XCom: passing data | [`04_xcom.py`](dags/04_xcom.py) | [08](docs/08-xcom.md) |
| 5 | Branching & trigger rules | [`05_branching_and_trigger_rules.py`](dags/05_branching_and_trigger_rules.py) | [09](docs/09-branching-and-trigger-rules.md) |
| 6 | Dynamic task mapping | [`06_dynamic_task_mapping.py`](dags/06_dynamic_task_mapping.py) | [10](docs/10-dynamic-task-mapping.md) |
| 7 | Task groups | [`07_task_groups.py`](dags/07_task_groups.py) | [11](docs/11-task-groups.md) |
| 8 | Sensors & deferrable operators | [`08_sensors_and_deferrable.py`](dags/08_sensors_and_deferrable.py) | [12](docs/12-sensors-and-deferrable.md) |
| 9 | Assets & data-aware scheduling | [`09_assets_data_aware_scheduling.py`](dags/09_assets_data_aware_scheduling.py) | [13](docs/13-assets-data-aware-scheduling.md) |
| 10 | Params, Variables, Jinja templating | [`10_params_variables_templating.py`](dags/10_params_variables_templating.py) | [14](docs/14-params-variables-templating.md) |
| 11 | Connections & hooks (Postgres, HTTP API) | [`11_connections_and_hooks.py`](dags/11_connections_and_hooks.py) | [15](docs/15-connections-and-hooks.md) |
| 12 | Retries, timeouts, callbacks, deadline alerts | [`12_error_handling_and_alerts.py`](dags/12_error_handling_and_alerts.py) | [16](docs/16-error-handling-retries-alerts.md) |
| 13 | Custom operators, hooks, sensors, plugins | [`13_custom_operators_hooks_sensors.py`](dags/13_custom_operators_hooks_sensors.py) | [17](docs/17-custom-operators-hooks-plugins.md) |
| 14 | Scheduling, timetables, catchup, backfill | [`14_scheduling_and_timetables.py`](dags/14_scheduling_and_timetables.py) | [18](docs/18-scheduling-and-timetables.md) |
| 15 | Pools, priority & concurrency | [`15_pools_priority_concurrency.py`](dags/15_pools_priority_concurrency.py) | [19](docs/19-pools-priority-concurrency.md) |
| 16 | Setup & teardown tasks | [`16_setup_teardown.py`](dags/16_setup_teardown.py) | [20](docs/20-setup-teardown.md) |
| 17 | Generating DAGs dynamically | [`17_dynamic_dag_generation.py`](dags/17_dynamic_dag_generation.py) | [21](docs/21-dynamic-dag-generation.md) |
| 18 | Human-in-the-loop approvals (3.1+) | [`18_human_in_the_loop.py`](dags/18_human_in_the_loop.py) | [22](docs/22-human-in-the-loop.md) |
| 19 | Task loops (new in 3.4) | [`19_task_loops.py`](dags/19_task_loops.py) | [23](docs/23-task-loops.md) |
| 20 | **Capstone:** production-style sales ETL | [`20_capstone_sales_etl.py`](dags/20_capstone_sales_etl.py) | [24](docs/24-capstone-sales-etl.md) |

Then: [UI tour](docs/25-ui-tour.md) · [CLI & REST API](docs/26-cli-and-rest-api.md) ·
[Testing DAGs](docs/27-testing-dags.md) · [Best practices](docs/28-best-practices.md) ·
[Executors & production](docs/29-executors-and-production.md) · [Troubleshooting](docs/30-troubleshooting-faq.md) ·
[Airflow 2 vs 3](docs/31-airflow-2-vs-3.md) · [Glossary](docs/32-glossary.md) · [Exercises](docs/33-exercises.md) ·
[Deploy to DigitalOcean with SkyPilot](docs/34-deploy-digitalocean-skypilot.md)

The full table of contents is in [docs/README.md](docs/README.md).

---

## Project layout

```
.
├── dags/                         # Example DAGs: the Airflow DAG processor reads this folder
│   ├── 01_hello_airflow.py … 20_capstone_sales_etl.py
│   ├── include/                  # Non-DAG files used by DAGs (ignored by the parser via .airflowignore)
│   │   ├── sql/                  # SQL files used by templates (create tables, build summary)
│   │   └── data/                 # Sample CSVs for the capstone (sales_north/south/west.csv)
│   └── .airflowignore
├── plugins/
│   ├── tutorial_lib/             # Reusable code imported by the DAGs
│   │   ├── hooks/open_meteo.py   #   custom Hook (weather API, works offline too)
│   │   ├── operators/            #   custom Operators (weather, CSV quality check)
│   │   ├── sensors/csv_rows.py   #   custom Sensor
│   │   ├── notifiers.py          #   custom Notifier
│   │   ├── callbacks.py          #   success/failure/retry/deadline callbacks
│   │   └── paths.py              #   where data files live
│   └── tutorial_plugin.py        # AirflowPlugin: macros + operator extra link
├── tests/                        # pytest: DAG integrity, structure, unit & integration tests
├── docs/                         # 34 beginner-friendly chapters
├── deploy/skypilot/              # One-command cloud VM (DigitalOcean) via SkyPilot
├── config/pools.json             # Pools created at startup (api_pool, database_pool)
├── scripts/                      # Local (no-Docker) helpers + Postgres init script
├── data/                         # Files written by the example DAGs (git-ignored)
├── docker-compose.yaml           # Airflow 3.4 + Postgres, LocalExecutor
├── Dockerfile / requirements.txt # Optional: extend the image with extra packages
├── Makefile                      # Shortcuts: make help
└── pyproject.toml                # pytest + ruff configuration
```

## What's pre-configured

| Thing | Value | Used by |
|-------|-------|---------|
| UI login | `airflow` / `airflow` | — |
| Connection `demo_postgres` | Postgres database `demo` (separate from Airflow's own DB) | 11, 20 |
| Connection `open_meteo_api` | https://api.open-meteo.com (free, no key; DAGs fall back to fake data if offline) | 11, 13 |
| Connection `fs_default` | local filesystem, for `FileSensor` | 08 |
| Variables | `environment`, `tutorial_settings` (JSON) | 10 |
| Pools | `api_pool` (2 slots), `database_pool` (4 slots) | 11, 15, 20 |

Connections and variables are set as environment variables in `docker-compose.yaml`.
They work in every DAG but don't appear in the UI lists (see chapter 15).

## Running the tests

```bash
make venv
```

```bash
make test
```

`make venv` creates a local `.venv` with Airflow 3.4 using the official constraints file (needs
[uv](https://docs.astral.sh/uv/)). `make test` runs the fast tests (DAG integrity, structure, unit).
`make test-all` also runs whole DAGs in-process with `dag.test()`. `make lint` runs ruff.
Details are in [docs/27-testing-dags.md](docs/27-testing-dags.md).

## Notes

- **Not for production as-is.** The compose file is for learning: default passwords, no TLS,
  and a single machine. [docs/29-executors-and-production.md](docs/29-executors-and-production.md) covers what to change.
- **Known Airflow 3.4.0 issue:** `EventsTimetable` from `airflow.sdk` fails to serialize, so the
  scheduling examples leave it out. The details are in chapter 18.
- Official documentation: https://airflow.apache.org/docs/apache-airflow/stable/
