# Installation and setup

**What you'll learn:** three ways to run this project — Docker Compose (recommended), a local
Python environment without Docker, or a plain `pip install` — plus what every file and folder in
the project is for, how to add Python packages, and how to check that everything works.

> **Run it**
> The goal of this chapter is to see the DAG `01_hello_airflow`
> ([../dags/01_hello_airflow.py](../dags/01_hello_airflow.py)) in the UI at http://localhost:8080,
> unpause it and run it once.

---

## Which option should I pick?

| Option | You need | You get | Pick it when |
|---|---|---|---|
| **A · Docker Compose** | Docker Desktop | The full Airflow 3 architecture (separate API server, scheduler, DAG processor, triggerer, Postgres) — closest to production | You are following this tutorial (recommended) |
| **B · Local, no Docker** | Python ≥ 3.11, `uv` | Everything in one `airflow standalone` process with SQLite | Docker is not available, or you want fast tests and IDE autocomplete |
| **C · Plain pip** | Python ≥ 3.11 | An Airflow install in any virtualenv | You want to understand what `make venv` does, or install Airflow elsewhere |

You can combine them: many people run the stack with Docker **and** keep a local `.venv` for
running tests and getting autocomplete in their editor.

---

## Option A — Docker Compose (recommended)

### Prerequisites

- **Docker Desktop** (macOS/Windows) or Docker Engine + the Compose plugin (Linux).
  Use a recent Docker Compose v2 — `docker compose version` should print **v2.24 or newer**
  (the `env_file: required: false` syntax in the compose file needs it).
- **At least 4 GB of memory for Docker** (8 GB is more comfortable). In Docker Desktop:
  *Settings → Resources → Memory*. With less, containers get killed and restart in a loop.
- **make** (pre-installed on macOS/Linux; on Windows use WSL 2). Every `make` target has a raw
  `docker compose` equivalent shown below, so `make` is optional.
- Free ports **8080** (Airflow UI) and **5432** (Postgres).

> 💡 **Docker in one paragraph.** A *container* is a lightweight, isolated box running a program
> with everything it needs installed. An *image* is the recipe for a container (here
> `apache/airflow:3.4.0` and `postgres:16`). *Docker Compose* starts several containers together
> from one file, `docker-compose.yaml`. A *volume* is storage that survives when containers are
> removed — this project keeps the Postgres data in a volume called `postgres-db-volume`.

### Step 1 — create the `.env` file

```bash
make env
```

This copies [`.env.example`](../.env.example) to `.env` and sets `AIRFLOW_UID` to your user id, so
files Airflow writes into `logs/`, `data/` etc. are owned by you. Raw equivalent:

```bash
cp .env.example .env
# Linux: make sure files written by containers are owned by you
sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env      # (macOS sed: sed -i '' ...)
```

`.env` also holds the image name (`apache/airflow:3.4.0`), the UI login (`airflow` / `airflow`),
an optional `FERNET_KEY` (encrypts passwords stored in Connections/Variables; empty is fine for
learning) and the JWT secret used between components. `.env` is in `.gitignore` — never commit it.

### Step 2 — one-time initialisation

```bash
make init
# raw: docker compose up airflow-init
```

The first time, Docker downloads the images (≈1–2 GB, a few minutes). The `airflow-init` container
then:

1. prints the Airflow version,
2. migrates (creates) the metadata database in Postgres,
3. creates the admin user **airflow / airflow**,
4. writes a default `config/airflow.cfg`,
5. imports the pools `api_pool` (2 slots) and `database_pool` (4 slots) from
   [`config/pools.json`](../config/pools.json),
6. fixes folder ownership and exits.

Wait for the line `airflow-init finished OK` and for the container to exit with code 0.

> 💡 `airflow-init` is an ordinary service in `docker-compose.yaml`, and the other Airflow services
> wait for it to complete (`depends_on: … service_completed_successfully`). So `make up` would run it
> too — running it in the foreground first lets you watch its output and spot errors.

### Step 3 — start Airflow

```bash
make up
# raw: docker compose up -d
```

`-d` means "detached": containers run in the background. Check their status:

```bash
make ps
# raw: docker compose ps
```

After about a minute every service should be `running` and, where it has a health check,
`(healthy)`. You should see `postgres`, `airflow-apiserver`, `airflow-scheduler`,
`airflow-dag-processor` and `airflow-triggerer`. (`airflow-init` has exited; `airflow-cli` only runs
on demand.)

### Step 4 — log in

Open **http://localhost:8080** and log in with **airflow / airflow**.

### Step 5 — unpause and run a DAG

This project sets `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'`, so **every DAG starts
paused**. A paused DAG never runs — not even when you press Trigger (the run waits in `queued`).

1. In the **Dags** list, find `01_hello_airflow` (the `tutorial` tag filter helps).
2. Switch its toggle **on** (unpause).
3. Click the **Trigger** (▶) button and confirm.
4. Open the DAG, click a task square in the **Grid** view and open its **Logs**.

From the command line instead:

```bash
docker compose run --rm airflow-cli dags unpause 01_hello_airflow
```

> ⚠️ Unpause DAGs one at a time as you reach their chapter. In particular,
> `14_schedule_continuous` starts a new run as soon as the previous one finishes, forever, and
> `14_schedule_catchup_backfill` immediately creates several catch-up runs.

The [UI tour](25-ui-tour.md#your-first-10-minutes) walks through this first run in detail.

### Stopping, restarting and resetting

| What | make | raw `docker compose` | Keeps your data? |
|---|---|---|---|
| Stop everything | `make down` | `docker compose down` | ✅ Database volume is kept |
| Start again | `make up` | `docker compose up -d` | ✅ |
| Follow scheduler logs | `make logs` | `docker compose logs -f airflow-scheduler` | — |
| Delete **everything** (DB, runs, users, history) | `make reset` | `docker compose down --volumes --remove-orphans` | ❌ Next `make init` / `make up` starts from an empty database |

Your DAG files, plugins and logs live in normal folders on your machine and are never deleted by
these commands.

### The `demo` database

The example DAGs that use SQL (11, 20 and parts of others) write to a separate Postgres database
called **`demo`**, so tutorial tables never mix with Airflow's own tables. It is created by
[`scripts/postgres-init/01-create-demo-db.sql`](../scripts/postgres-init/01-create-demo-db.sql),
which Postgres runs **only the first time its data volume is created**.

If you started Postgres before that file existed (or you see `database "demo" does not exist`),
create it by hand:

```bash
docker compose exec postgres psql -U airflow -c "CREATE DATABASE demo;"
```

Inspect the tutorial tables with `make psql` (raw:
`docker compose exec postgres psql -U airflow -d demo`) or any SQL client at
`localhost:5432`, user `airflow`, password `airflow`, database `demo`.

---

## Option B — local, without Docker

This runs Airflow on your machine with a SQLite database in `.airflow_local/`. It uses the same DAGs,
plugins, connections and variables as the Docker setup.

### Prerequisites

- **Python 3.11 or newer** (Airflow 3.4 dropped Python 3.10). `make venv` asks `uv` for 3.12.
- **[uv](https://docs.astral.sh/uv/)**, a fast Python package installer
  (`brew install uv` or `curl -LsSf https://astral.sh/uv/install.sh | sh`). `uv` can download the
  right Python version for you.

### Create the virtual environment

```bash
make venv
```

which runs:

```bash
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r requirements-dev.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.12.txt
```

[`requirements-dev.txt`](../requirements-dev.txt) installs `apache-airflow[postgres,http]==3.4.0`,
the `standard` and `common-sql` providers, and `pytest`. The `--constraint` file is explained in
[Option C](#option-c--pip-install-with-constraints).

### Run Airflow

```bash
make standalone
```

This runs [`scripts/run_local.sh`](../scripts/run_local.sh), which:

1. sources [`scripts/local_env.sh`](../scripts/local_env.sh) — it sets `AIRFLOW_HOME=.airflow_local`,
   points Airflow at `dags/` and `plugins/`, turns off example DAGs, and defines the same tutorial
   connections and variables as `docker-compose.yaml` (with `demo_postgres` pointing at
   `localhost:5432` and `environment=local`);
2. runs `airflow db migrate` (creates `.airflow_local/airflow.db`);
3. imports the pools from `config/pools.json`;
4. starts `airflow standalone` — the API server, scheduler, DAG processor and triggerer in one
   terminal, with the LocalExecutor.

Open **http://localhost:8080**. The local setup uses Airflow's built-in **SimpleAuthManager** with
`AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=true`, so you are admin without a password (learning
only!). Stop with **Ctrl+C**.

> ⚠️ Don't run `make standalone` and the Docker stack at the same time — both want port 8080.
> DAGs that use the `demo_postgres` connection (11, 20) still need Postgres; start only that
> container with `docker compose up -d postgres`.

### Use the CLI and tests locally

```bash
source scripts/local_env.sh      # activates .venv and sets AIRFLOW_HOME etc.
airflow dags list
make test                        # runs pytest on tests/
```

---

## Option C — pip install with constraints

If you want Airflow in your own virtualenv:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install "apache-airflow[postgres,http]==3.4.0" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.12.txt"
```

Replace `3.12` in the URL with your Python version (`3.11`, `3.12`, …).

### Why the constraints file matters

Airflow depends on hundreds of Python packages (directly and indirectly). Their newest versions are
not always compatible with each other or with Airflow. The Airflow team publishes a **constraints
file** for every release and Python version: a list of *exact* versions that were tested together.

- `--constraint` does **not** install anything by itself; it only says "if you install package X,
  use exactly this version".
- Without it, `pip install apache-airflow==3.4.0` may pick a brand-new version of some dependency
  released yesterday and break your install in surprising ways.
- Use the **same** constraints file when you later add providers, e.g.
  `pip install apache-airflow-providers-slack --constraint <same URL>`, so pip does not silently
  upgrade or downgrade Airflow's dependencies.

Then initialise and run it the same way as Option B (`airflow db migrate`, `airflow standalone`),
with `AIRFLOW_HOME` pointing where you want the config and SQLite database to live.

---

## Project structure

```text
apache-airflow/
├── dags/                         # DAG files — the DAG processor scans this folder
│   ├── 01_hello_airflow.py … 20_capstone_sales_etl.py   # 20 numbered examples (37 DAGs in total)
│   └── include/                  # non-DAG files the DAGs use
│       ├── sql/                  # .sql files (create tables, build summary)
│       └── data/                 # sample CSVs (sales_north/south/west.csv)
├── plugins/                      # on the Python path inside Airflow
│   ├── tutorial_plugin.py        # an AirflowPlugin: Jinja macros + an operator extra link
│   └── tutorial_lib/             # shared code imported by DAGs
│       ├── hooks/open_meteo.py
│       ├── operators/weather.py, csv_quality.py
│       ├── sensors/csv_rows.py
│       ├── callbacks.py, notifiers.py
│       └── paths.py              # where DAGs read/write files (DATA_DIR, SQL_DIR …)
├── tests/                        # pytest suite (integrity, structure, components, runs)
├── config/
│   ├── pools.json                # pools imported by airflow-init / run_local.sh
│   └── airflow.cfg               # generated on first start (git-ignored)
├── data/                         # files written by the example DAGs (git-ignored contents)
├── logs/                         # task logs (git-ignored contents)
├── scripts/
│   ├── local_env.sh              # env vars for running WITHOUT Docker
│   ├── run_local.sh              # used by `make standalone`
│   └── postgres-init/01-create-demo-db.sql   # creates the `demo` database
├── docs/                         # this documentation
├── docker-compose.yaml           # the whole Airflow stack
├── Dockerfile                    # optional custom image (extra Python packages)
├── requirements.txt              # extra packages for the Docker image
├── requirements-dev.txt          # packages for the local .venv
├── pyproject.toml                # project metadata, pytest + ruff settings
├── Makefile                      # shortcuts: make help
├── .env.example                  # template for .env (copied by `make env`)
├── .venv/                        # local virtualenv (created by `make venv`, git-ignored)
└── .airflow_local/               # local AIRFLOW_HOME: SQLite DB, config, logs (git-ignored)
```

| Path | What it is for |
|---|---|
| `dags/` | Every `.py` file here is parsed by the DAG processor. Mounted at `/opt/airflow/dags`. One file can define several DAGs (09, 14, 17, 19, 20 do). |
| `dags/include/` | SQL and CSV files used by DAGs. Not DAGs themselves. The DAGs find them through `tutorial_lib.paths`. |
| `plugins/` | Airflow adds this folder to the Python path, so DAGs can `from tutorial_lib... import ...`. Files defining an `AirflowPlugin` subclass are loaded as plugins. Mounted at `/opt/airflow/plugins`. See [17 · Custom operators, hooks & plugins](17-custom-operators-hooks-plugins.md). |
| `tests/` | Tests you run with `make test`. See [27 · Testing DAGs](27-testing-dags.md). |
| `config/pools.json` | Pool definitions (`api_pool`, `database_pool`). |
| `config/airflow.cfg` | Airflow's config file inside Docker (`AIRFLOW_CONFIG`). Settings in `docker-compose.yaml` environment variables (`AIRFLOW__SECTION__KEY`) override it. |
| `data/` | Shared "data lake" folder: `data/incoming/` (sensor example), `data/assets/` (asset example), `data/workspaces/` (setup/teardown). Mounted at `/opt/airflow/data`. |
| `logs/` | One folder per DAG/run/task with the task log files. Mounted at `/opt/airflow/logs`. Safe to delete old contents. |
| `scripts/` | Helper shell scripts and the Postgres init SQL. |
| `docker-compose.yaml` | Services, shared settings, tutorial connections (`AIRFLOW_CONN_*`) and variables (`AIRFLOW_VAR_*`). See [02 · Architecture](02-architecture.md#how-this-projects-docker-compose-maps-to-the-components). |
| `Dockerfile`, `requirements.txt` | Only used if you need extra Python packages (next section). |
| `requirements-dev.txt`, `pyproject.toml` | Local development: packages for `.venv`, pytest config (`pythonpath = ["plugins", "dags"]`), ruff linting with the `AIR` Airflow rules. |
| `Makefile` | Shortcuts. `make help` lists them: `env init up down reset ps logs cli psql venv test standalone`. |

---

## Adding Python packages

The official `apache/airflow:3.4.0` image already contains many providers (Postgres, HTTP, common-sql,
standard, FAB, Celery, Kubernetes, Amazon, Google, …). If your DAG needs something else:

**In Docker (the proper way):**

1. Add the package to [`requirements.txt`](../requirements.txt), e.g.
   `apache-airflow-providers-slack`.
2. In `docker-compose.yaml`, comment out the `image:` line and uncomment `build: .`:

   ```yaml
   # image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.4.0}
   build: .
   ```

3. Build and restart:

   ```bash
   docker compose build
   docker compose up -d
   ```

The [`Dockerfile`](../Dockerfile) installs your requirements **together with**
`apache-airflow==${AIRFLOW_VERSION}`, so pip cannot change the Airflow version by accident:

```dockerfile
FROM apache/airflow:3.4.0
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
```

**Quick experiment only:** set `_PIP_ADDITIONAL_REQUIREMENTS=some-package` in `.env`. The package is
installed every time a container starts — slow, and not for anything you keep.

**Locally:** add the package to `requirements-dev.txt` and run `make venv` again (or
`uv pip install --python .venv/bin/python <package> --constraint <constraints URL>`).

---

## Verify the installation

Run these after `make up`.

**1. All containers healthy**

```bash
docker compose ps
```

**2. Airflow sees the DAGs and the plugin** — open a shell in an Airflow container:

```bash
make cli                 # raw: docker compose run --rm airflow-cli bash
```

then, inside it:

```bash
airflow version                     # 3.4.0
airflow dags list                   # 37 DAGs: 01_hello_airflow … tutorial_daily_kpis
airflow dags list-import-errors     # should report no errors
airflow plugins                     # shows tutorial_plugin with its macros and extra link
airflow pools list                  # default_pool, api_pool (2), database_pool (4)
airflow connections get demo_postgres
airflow variables get environment   # docker-local
exit
```

You can also run a single command without opening a shell:

```bash
docker compose run --rm airflow-cli dags list
```

**3. The API server is up**

```bash
curl http://localhost:8080/api/v2/monitor/health
```

returns JSON with the health of the metadata database, scheduler, triggerer and DAG processor.

**4. A DAG runs** — unpause and trigger `01_hello_airflow` (Step 5 above). All four tasks should
turn green.

> 💡 The tutorial connections (`demo_postgres`, `fs_default`, `open_meteo_api`) are defined as
> environment variables, so they **do not appear** in *Admin → Connections* in the UI, but
> `airflow connections get` and every DAG can use them. Same for the Variables `environment` and
> `tutorial_settings`. See [15 · Connections & hooks](15-connections-and-hooks.md).

If something does not work, see [30 · Troubleshooting & FAQ](30-troubleshooting-faq.md) — it covers
containers restarting, port conflicts, permission errors on `logs/`, DAGs not showing up, import
errors and the missing `demo` database.

## Common mistakes

- **Opening the UI too early.** The API server needs up to a minute (longer on the very first
  start) before it is `healthy`; until then the browser shows a connection error.
- **Too little memory for Docker.** Containers restart in a loop; give Docker at least 4 GB.
- **Forgetting to unpause** a DAG and wondering why the triggered run stays queued.
- **Port 8080 or 5432 already in use** (another Airflow, a local Postgres, `make standalone`).
- **Expecting `pip install` inside a running container to survive.** Containers are recreated;
  use `requirements.txt` + `build: .`.
- **Installing Airflow without the constraints file**, then fighting dependency errors.

## Try it yourself

1. Run `make help` and match each target to the raw command in the Makefile.
2. Change `AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL` to `'10'` in `docker-compose.yaml`, run
   `docker compose up -d` (it recreates changed containers) and measure how fast a new DAG file
   appears.
3. Inside `make cli`, run `airflow config get-value core executor` and
   `airflow config get-value core dags_are_paused_at_creation`. Where do these values come from?
4. Create a local `.venv` with `make venv`, then run `make test` and read which tests pass.

---

**Next →** [04 · Core concepts](04-core-concepts.md) · [Back to the index](README.md)

Official docs: [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) ·
[Installation from PyPI](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html)
