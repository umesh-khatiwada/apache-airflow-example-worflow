# Executors and running Airflow in production

**What you'll learn:** what an executor is and how the available executors compare; how to switch this project from `LocalExecutor` to `CeleryExecutor`; and what changes when you move from a laptop to a real deployment: deploying DAG code, scaling, database upkeep, logs, monitoring, security and upgrades. It ends with a production readiness checklist.

> **Run it**
> This chapter is about the platform, not one DAG. The files to look at are
> [../docker-compose.yaml](../docker-compose.yaml) (this project's stack, `LocalExecutor`),
> [../Dockerfile](../Dockerfile), [../Makefile](../Makefile) and [../.env.example](../.env.example).
> Any DAG works for testing an executor change; `15_pools_priority_concurrency` ([../dags/15_pools_priority_concurrency.py](../dags/15_pools_priority_concurrency.py)) is a good one because it runs many tasks in parallel.

> ⚠️ This project's `docker-compose.yaml` says it at the top: *for local development only. Not a production deployment.* Everything below explains what you would do differently.

---

## 1. What an executor does

The [scheduler](32-glossary.md#scheduler) decides *which* [task instances](32-glossary.md#task-instance) are ready. The [executor](32-glossary.md#executor) decides *where and how* they run: as a subprocess next to the scheduler, on a pool of worker machines, or in a fresh Kubernetes pod.

```mermaid
flowchart LR
    S["Scheduler<br/>(task is ready)"] --> E{"Executor"}
    E -->|LocalExecutor| L["subprocess on the<br/>scheduler's machine"]
    E -->|CeleryExecutor| Q["queue (Redis)"] --> W["Celery workers"]
    E -->|KubernetesExecutor| P["one pod per task"]
    E -->|EdgeExecutor| X["remote edge workers<br/>(pull work over HTTP)"]
    L & W & P & X -->|"Task SDK → Execution API"| A["api-server"]
```

Whatever the executor, a running task in Airflow 3 reports its state, XComs and logs through the **Execution API** on the api-server (`AIRFLOW__CORE__EXECUTION_API_SERVER_URL` in the compose file). Tasks don't connect to the metadata database. That's what makes remote workers (Celery on another host, Kubernetes pods, Edge workers) simpler and safer than in Airflow 2. See [02-architecture.md](02-architecture.md).

The executor is set with `[core] executor` (env var `AIRFLOW__CORE__EXECUTOR`). The default is `LocalExecutor`. You can check yours with:

```bash
source scripts/local_env.sh && airflow config get-value core executor   # LocalExecutor
```

---

## 2. Executors compared

| | **LocalExecutor** | **CeleryExecutor** | **KubernetesExecutor** | **EdgeExecutor** |
|---|---|---|---|---|
| Where tasks run | subprocesses on the scheduler's machine | long-running Celery worker processes on any number of machines | a new pod per task instance | worker processes on remote machines that poll the api-server over HTTP(S) |
| Extra infrastructure | none | a message broker (Redis or RabbitMQ) + worker machines | a Kubernetes cluster | the `edge3` provider; edge workers that can reach the api-server |
| Provider package | built in | `apache-airflow-providers-celery` (in the official image) | `apache-airflow-providers-cncf-kubernetes` (in the official image) | `apache-airflow-providers-edge3` |
| Scaling | one machine; limited by `[core] parallelism` (32) | add workers; each runs `[celery] worker_concurrency` (16) tasks | as large as the cluster | add edge workers anywhere |
| Task start latency | very low | low (workers are already running) | higher (pod start-up, image pull) | low to medium (polling) |
| Isolation | none: tasks share the machine and image | per worker; all tasks share the worker image | per task: own pod, image, CPU/memory | per worker site |
| Typical use | learning, small teams, single server | steady high volume of short tasks | spiky workloads, very different resource needs, strong isolation | tasks that must run in another network, data centre, cloud or on-prem site |
| This project | ✅ used by `docker-compose.yaml` | see section 3 | needs a cluster (Helm chart) | not covered |

The older fixed "hybrid" executors (`CeleryKubernetesExecutor`, `LocalKubernetesExecutor`) are not supported in Airflow 3; the general mechanism below replaces them.

### Running several executors at once

Since Airflow 2.10 you can configure **multiple executors**. List them comma-separated in `[core] executor`; the **first one is the default**, and a task chooses another with the `executor=` argument:

```yaml
# docker-compose.yaml (x-airflow-common environment)
AIRFLOW__CORE__EXECUTOR: "LocalExecutor,CeleryExecutor"
```

```python
@task  # runs on the default (first) executor: LocalExecutor
def small_step(): ...

@task(executor="CeleryExecutor")  # this one goes to a Celery worker
def heavy_step(): ...
```

You can also give an executor a short alias with `alias:path`, which is useful for custom or provider executors:

```text
AIRFLOW__CORE__EXECUTOR="CeleryExecutor,k8s:airflow.providers.cncf.kubernetes.executors.kubernetes_executor.KubernetesExecutor"
```

and then use `@task(executor="k8s")`. Set `executor` in a DAG's `default_args` to move a whole DAG. (There is also a per-team syntax with `;team=...` for the experimental `[core] multi_team` mode; you won't need it while learning.)

This syntax was checked against `airflow/executors/executor_loader.py` in the installed 3.4.0.

---

## 3. Switching this project to CeleryExecutor

The project's compose file is the official 3.4.0 `docker-compose.yaml` with Celery removed. To put it back, make five changes in [../docker-compose.yaml](../docker-compose.yaml). The snippets below come from the official file.

**1. Change the executor and add the Celery settings** in the `x-airflow-common` → `environment` block:

```yaml
  environment:
    &airflow-common-env
    # ---- Core ---------------------------------------------------------------
    AIRFLOW__CORE__EXECUTOR: CeleryExecutor            # was: LocalExecutor
    AIRFLOW__CELERY__BROKER_URL: redis://:@redis:6379/0
    AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql+psycopg://airflow:airflow@postgres/airflow
    # ...keep every other line as it is
```

The **broker** (Redis) carries "please run this task" messages from the scheduler to the workers. The **result backend** stores Celery's own bookkeeping; here it reuses the Postgres metadata database.

**2. Make every Airflow container wait for Redis**, in the shared `depends_on` anchor:

```yaml
  depends_on:
    &airflow-common-depends-on
    redis:
      condition: service_healthy
    postgres:
      condition: service_healthy
```

**3. Add the Redis service** under `services:`:

```yaml
  redis:
    # Redis is limited to 7.2-bookworm due to licencing change
    image: redis:7.2-bookworm
    expose:
      - 6379
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 30s
      retries: 50
      start_period: 30s
    restart: always
```

**4. Add a Celery worker.** It inherits the image, environment and volumes (including `./data`, which the sensor and capstone examples need) from `x-airflow-common`:

```yaml
  airflow-worker:
    <<: *airflow-common
    command: celery worker
    healthcheck:
      test: ["CMD-SHELL", 'celery --app airflow.providers.celery.executors.celery_executor.app inspect ping -d "celery@$${HOSTNAME}" || celery --app airflow.executors.celery_executor.app inspect ping -d "celery@$${HOSTNAME}"']
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    environment:
      <<: *airflow-common-env
      # Required to handle warm shutdown of the celery workers properly
      DUMB_INIT_SETSID: "0"
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-apiserver:
        condition: service_healthy
      airflow-init:
        condition: service_completed_successfully
```

**5. (Optional) Add Flower**, a web UI for Celery workers on http://localhost:5555. It only starts when you ask for its profile:

```yaml
  flower:
    <<: *airflow-common
    command: celery flower
    profiles:
      - flower
    ports:
      - "5555:5555"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:5555/"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully
```

Then restart the stack. The metadata database doesn't need to be reset:

```bash
docker compose down
docker compose up -d                          # starts redis + airflow-worker too
docker compose --profile flower up -d         # optional: also start Flower
docker compose ps                             # airflow-worker should become "healthy"
docker compose up -d --scale airflow-worker=3 # three workers instead of one
```

Trigger `15_pools_priority_concurrency` and run `docker compose logs -f airflow-worker`: the task logs now come from the worker container, not the scheduler. With Celery, Docker Desktop needs more memory (the official guide asks for at least 4 GB; 8 GB is comfortable).

**Celery queues.** Every task goes to the `default` queue unless you set `queue=`. A worker started with `airflow celery worker --queues high_memory` only takes tasks from that queue, so you can send heavy tasks to bigger machines:

```python
@task(queue="high_memory")
def big_join(): ...
```

---

## 4. Production deployment options

| Option | What it is | You manage | Good fit |
|---|---|---|---|
| **Official Helm chart** (`apache-airflow/airflow`) | the Apache Airflow project's chart for Kubernetes; supports Celery, Kubernetes and multiple executors, git-sync, PgBouncer, KEDA autoscaling | the cluster, upgrades, database, monitoring | teams already running Kubernetes who want full control |
| **Docker Compose / VMs** | the setup in this repo, hardened, or systemd services on VMs | everything | small internal setups; not recommended by the Airflow project for production |
| **Astronomer (Astro)** | a commercial managed Airflow platform from a company that contributes heavily to Airflow; runs on AWS, GCP and Azure | DAGs and configuration | teams that want a managed service across clouds |
| **Amazon MWAA** | Amazon Managed Workflows for Apache Airflow | DAGs (from S3), requirements, configuration | AWS-centric teams |
| **Google Cloud Composer** | Google Cloud's managed Airflow, on GKE | DAGs (from a GCS bucket), PyPI packages, configuration | Google Cloud-centric teams |
| **Microsoft Azure** | Azure has offered managed Airflow through Azure Data Factory's *Workflow Orchestration Manager*; many Azure users instead run the Helm chart on AKS or use Astronomer on Azure | depends on the choice | Azure-centric teams; check the current status of Azure's offering |

Managed services usually lag behind new Airflow releases. Before choosing one, check which Airflow 3.x versions it supports and whether features you rely on (for example task loops, new in 3.4) are available there.

A minimal Helm install looks like this (see the chart's docs for every value):

```bash
helm repo add apache-airflow https://airflow.apache.org
helm upgrade --install airflow apache-airflow/airflow \
  --namespace airflow --create-namespace \
  -f values.yaml
```

Check the chart's release notes for which Airflow versions a chart version supports.

---

## 5. Deploying DAG code: bundles, images and git-sync

In this project the `dags/` folder is a Docker volume, so editing a file on your laptop is "deploying" it. In production you need a repeatable way to get code onto every component that parses or runs it (dag-processor, workers, triggerer).

### DAG bundles (Airflow 3)

Airflow 3 reads DAGs from **[DAG bundles](32-glossary.md#bundle)**, configured in `[dag_processor] dag_bundle_config_list`. The default is one bundle called `dags-folder`, a `LocalDagBundle` pointing at `[core] dags_folder`. That's what this project uses:

```bash
source scripts/local_env.sh && airflow config get-value dag_processor dag_bundle_config_list
# [{"name": "dags-folder", "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle", "kwargs": {}}]
```

A **Git bundle** (from the `apache-airflow-providers-git` package) makes Airflow pull DAGs straight from a Git repository. The example below comes from the option's own documentation in 3.4.0; `git_conn_id` names a connection of type *git* that holds the repository URL and an access token:

```text
AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='[
  {"name": "my-git-repo",
   "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
   "kwargs": {"git_conn_id": "my_git_repo", "subdir": "dags", "tracking_ref": "main", "refresh_interval": 0}}
]'
```

Bundles that support versions (like Git) give you **DAG versioning**: a run that started on commit A keeps running commit A's code even if commit B lands mid-run, and the UI shows which version each run used. The plain `LocalDagBundle` has no versions, so runs always use whatever is in the folder. You can run several bundles side by side (one per team or repository) and start dag-processors for specific bundles with `airflow dag-processor --bundle-name <name>`.

### The other two common approaches

| Approach | How | Pros | Cons |
|---|---|---|---|
| **Bake DAGs into the image** | `COPY dags/ plugins/` in your `Dockerfile`, deploy a new image tag | immutable, reproducible, code and dependencies always match | every DAG change is an image build and rollout |
| **git-sync sidecar** | the Helm chart's `dags.gitSync` settings run a sidecar that pulls a repo into a shared volume | fast DAG updates without new images | code and image dependencies can drift apart |
| **Git DAG bundle** | Airflow itself pulls the repo (above) | built-in versioning; no sidecar | newer; requires the git provider and a git connection |

Whichever you choose: run CI ([27-testing-dags.md](27-testing-dags.md)) before code reaches the branch Airflow reads from, and keep `plugins/` (shared code like `tutorial_lib`) deployed in step with `dags/`.

---

## 6. Scaling the components

| Component | How to scale | Notes |
|---|---|---|
| **Scheduler** | run 2+ schedulers (active-active) | high availability and more throughput; needs PostgreSQL or MySQL 8+ (row locking). `[core] parallelism` is **per scheduler** |
| **DAG processor** | run several; give each a `--bundle-name` or raise `[dag_processor] parsing_processes` (default 2) | slow parsing shows up as late DAG updates and import timeouts |
| **Triggerer** | run 2+ for HA; each runs up to `[triggerer] capacity` (1000) triggers | needed for deferrable operators/sensors and deadline-alert callbacks |
| **API server** | more replicas behind a load balancer, or `[api] workers` (default 1) | serves the UI, the public REST API **and** the Execution API that every running task talks to |
| **Workers** | more Celery workers / bigger Kubernetes cluster / more edge workers | tune `[celery] worker_concurrency`; use queues for different machine sizes |
| **Metadata DB** | a managed Postgres with enough connections; PgBouncer in front | every component holds connections; the Helm chart can deploy PgBouncer |

Concurrency limits (`parallelism`, `max_active_tasks`, `max_active_runs`, pools) are explained in [19-pools-priority-concurrency.md](19-pools-priority-concurrency.md).

---

## 7. Keeping the metadata database healthy

The metadata database grows forever: every DAG run, task instance, XCom, log entry and asset event stays in it. Old rows slow down the scheduler and the UI. Purge them on a schedule with `airflow db clean`:

```bash
# See what would be deleted (rows older than 90 days, all tables)
airflow db clean --clean-before-timestamp "2026-08-13" --dry-run

# Do it, without the interactive prompt; only some tables
airflow db clean --clean-before-timestamp "2026-08-13" --tables dag_run,task_instance,log,job --yes

# By default purged rows are copied to _airflow_deleted__* archive tables first.
# Export or drop those archives afterwards:
airflow db export-archived --export-format csv --output-path /tmp/archive   # optional
airflow db drop-archived --yes
```

`--skip-archive` deletes without archiving. Run `airflow db clean --help` to see the full list of tables in your version.

Where to run it: a Kubernetes `CronJob` or another scheduler with database access is the simplest choice. In Airflow 3, tasks talk to the Execution API instead of the database, so a "maintenance DAG" that calls `airflow db clean` only works if its worker has database credentials configured.

Also back up the metadata database (point-in-time recovery if your provider offers it). It holds run history, connections, variables and users.

---

## 8. Logging

By default task logs are files under `logs/` on the machine that ran the task (in this project `./logs`, mounted into every container). In production, workers come and go (especially pods), so ship logs to remote storage:

```text
AIRFLOW__LOGGING__REMOTE_LOGGING=True
AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=s3://my-company-airflow-logs/prod   # or gs://... for GCS, wasb://... for Azure
AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=aws_default                             # a connection with write access
AIRFLOW__LOGGING__DELETE_LOCAL_LOGS=True                                     # optional: remove the local copy after upload
```

The S3 handler comes from the `amazon` provider, GCS from `google`, Azure Blob from `microsoft-azure`; Elasticsearch and OpenSearch handlers exist too. The UI reads logs back from the same place. Component logs (scheduler, api-server) go to stdout/stderr in containers; collect them with your platform's log stack.

---

## 9. Monitoring

### Health checks

- **`GET /api/v2/monitor/health`** on the api-server returns the status of the metadata database, scheduler, triggerer and dag-processor, with their latest heartbeats. This project's compose file uses it as the api-server's Docker health check:

  ```bash
  curl -s http://localhost:8080/api/v2/monitor/health | python -m json.tool
  ```

- **Scheduler health server.** With `[scheduler] enable_health_check = True` (set in this project's compose file), the scheduler serves a small health endpoint on port `8974` (`[scheduler] scheduler_health_check_server_port`).
- **`airflow jobs check --job-type SchedulerJob|DagProcessorJob|TriggererJob`** exits non-zero if that component stopped heart-beating. The compose file uses these as the other containers' health checks.

### Metrics

Airflow emits metrics (counters, gauges, timers) via **StatsD** or **OpenTelemetry**:

```text
# StatsD (then e.g. statsd-exporter → Prometheus, or Datadog)
AIRFLOW__METRICS__STATSD_ON=True
AIRFLOW__METRICS__STATSD_HOST=statsd-exporter
AIRFLOW__METRICS__STATSD_PORT=9125
AIRFLOW__METRICS__STATSD_PREFIX=airflow

# or OpenTelemetry (an OTel collector); traces have their own [traces] section
AIRFLOW__METRICS__OTEL_ON=True
AIRFLOW__METRICS__OTEL_HOST=otel-collector
AIRFLOW__METRICS__OTEL_PORT=4318
```

Things worth alerting on: the scheduler heartbeat stopping, DAG import errors, tasks piling up in *queued*, no open executor slots or pool slots for a long time, a spike in task failures, and DAG run duration growing. The exact metric names are in the [metrics reference](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/metrics.html).

For "did my pipeline finish on time?" use [deadline alerts](32-glossary.md#deadline-alert) inside the DAGs ([16-error-handling-retries-alerts.md](16-error-handling-retries-alerts.md)). Platform metrics don't know your business deadlines.

---

## 10. Security

### Keys and secrets

| Setting | What it protects | Production advice |
|---|---|---|
| `[core] fernet_key` (`AIRFLOW__CORE__FERNET_KEY`) | encrypts connection passwords and variable values stored in the metadata DB | **always set it.** This project leaves `FERNET_KEY=` empty in `.env.example` for convenience, which stores secrets unencrypted. Generate one with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`. To rotate: set `new_key,old_key`, run `airflow rotate-fernet-key`, then remove the old key |
| `[api_auth] jwt_secret` (`AIRFLOW__API_AUTH__JWT_SECRET`) | signs the JWT tokens used by the REST API and the Execution API | a long random value, **identical on every component**, kept in a secret store. `.env.example` uses `change-me-local-dev-only` |
| `[api] secret_key` | signs session data in the UI | random, identical on all api-server replicas |
| Connections & variables | credentials | a [secrets backend](32-glossary.md#secrets-backend) (below) |

### Secrets backends

A secrets backend makes Airflow look up connections and variables in an external vault before (or instead of) the metadata DB. Configure it under `[secrets]`:

```text
AIRFLOW__SECRETS__BACKEND=airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
AIRFLOW__SECRETS__BACKEND_KWARGS={"connections_prefix": "airflow/connections", "variables_prefix": "airflow/variables"}
```

Then a secret named `airflow/connections/demo_postgres` provides the `demo_postgres` connection. Other providers ship backends for HashiCorp Vault (`hashicorp`), GCP Secret Manager (`google`) and Azure Key Vault (`microsoft-azure`). DAG code doesn't change: `PostgresHook(postgres_conn_id="demo_postgres")` works the same. Lookup order is: secrets backend → environment variables → metadata DB.

### Authentication and authorization (auth managers)

In Airflow 3, login and permissions are handled by a pluggable **auth manager**, set with `[core] auth_manager`:

| Auth manager | Class path | Users and roles | Use for |
|---|---|---|---|
| **SimpleAuthManager** (default) | `airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager` | users listed in `[core] simple_auth_manager_users` (`bob:admin,peter:viewer`), roles `viewer`, `user`, `op`, `admin`; passwords generated into a file | development only. `make standalone` uses it with `simple_auth_manager_all_admins=True` (no login at all) |
| **FAB auth manager** | `airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager` | users and roles in the metadata DB (Admin, Op, User, Viewer, Public); LDAP/OAuth/OIDC through Flask-AppBuilder settings | what this project's Docker setup uses (login `airflow` / `airflow`); the classic Airflow 2 style RBAC |
| **Keycloak auth manager** | provider `apache-airflow-providers-keycloak` | users, groups and permissions managed in Keycloak | companies with Keycloak SSO |
| **AWS auth manager** | provider `apache-airflow-providers-amazon` | AWS IAM Identity Center + Amazon Verified Permissions | AWS-centric setups |

Role-based access control (RBAC) lets you, for example, give analysts read-only access and only the platform team access to Connections. With the FAB auth manager you can also restrict a DAG to certain roles with the DAG's `access_control` argument.

### Other hardening for this compose file

The project turns on some learning conveniences you should switch **off** in production:

- `AIRFLOW__API__EXPOSE_CONFIG: 'true'` shows the full configuration in the UI.
- `AIRFLOW__CORE__TEST_CONNECTION: 'Enabled'` lets any user with access make Airflow connect to arbitrary hosts.
- Postgres is published on `localhost:5432` with password `airflow`.
- The UI admin user is `airflow` / `airflow`.
- There's no TLS. In production, put the api-server behind HTTPS (a load balancer or ingress).

---

## 11. Upgrading Airflow safely

1. **Read the release notes** ("Significant changes") for every version between yours and the target.
2. **Pin everything with the new constraints file**, as the [../Makefile](../Makefile) and [../Dockerfile](../Dockerfile) do for 3.4.0 (`constraints-3.4.0/constraints-3.12.txt`). Upgrade providers deliberately, not by accident.
3. **Lint your code and config.** `ruff check --select AIR3 dags plugins` flags removed and moved Airflow APIs; `airflow config lint` flags renamed or removed options (it was built for the 2.x → 3.0 move).
4. **Run your tests** against the new version in a separate environment (`make venv` with the new versions, then `make test`).
5. **Back up the metadata database**, then upgrade a staging environment first.
6. **Migrate the database** with `airflow db migrate` (the official image runs it when `_AIRFLOW_DB_MIGRATE=true`, as `airflow-init` does here). `airflow db check-migrations` waits until migrations are done, which is useful in start-up scripts.
7. **Roll out** all components on the same version. Watch import errors, the health endpoint and failure rates.
8. Have a rollback plan: the previous image plus the database backup (`airflow db downgrade` exists, but restoring a backup is the safer route).

The jump from Airflow 2 to 3 is a bigger topic with its own chapter: [31-airflow-2-vs-3.md](31-airflow-2-vs-3.md).

---

## 12. Production readiness checklist

| Area | Check |
|---|---|
| Executor | Chosen deliberately (Celery / Kubernetes / multiple); worker sizes and concurrency tuned |
| High availability | 2+ schedulers, 2+ triggerers, 2+ api-server replicas; dag-processor(s) monitored |
| Database | Managed PostgreSQL, backups and restore tested, connection pooling (PgBouncer), `airflow db clean` scheduled |
| DAG deployment | DAGs reach Airflow only through CI (tests + ruff); image, git-sync or Git bundle chosen; plugins deployed in step |
| Dependencies | Custom image built from `apache/airflow:<version>` with the constraints file; no `_PIP_ADDITIONAL_REQUIREMENTS` |
| Secrets | Fernet key set; JWT secret and `[api] secret_key` random and shared; connections in a secrets backend; no secrets in code |
| Auth | FAB/Keycloak/AWS auth manager with SSO; least-privilege roles; default `airflow/airflow` user removed |
| Network | HTTPS in front of the api-server; database not publicly reachable; `expose_config` and test-connection off |
| Logging | Remote logging to S3/GCS/Azure/Elasticsearch; component logs collected |
| Monitoring | Health endpoint and `airflow jobs check` probes; StatsD/OpenTelemetry metrics with alerts; DAG failure callbacks or notifiers |
| Pipelines | Retries, timeouts, pools, idempotent loads and data-quality checks ([28-best-practices.md](28-best-practices.md)) |
| Upgrades | Staging environment on the same setup; documented upgrade and rollback runbook |

---

## Common mistakes

- **Running the compose file as production.** It has default passwords, no TLS, one of everything, and convenience settings switched on.
- **Leaving the Fernet key empty.** Connection passwords end up readable in the database.
- **Different JWT secrets on different components.** Tasks then fail to authenticate to the Execution API.
- **Installing packages on only some components.** With Celery or Kubernetes, the dag-processor, workers and triggerer all need the same image.
- **Never cleaning the metadata DB.** The UI and scheduler slow down month after month.
- **Mounting `logs/` on ephemeral pods without remote logging.** The logs disappear with the pod.

## Try it yourself

1. Follow section 3 to switch to `CeleryExecutor`, scale to two workers, trigger `15_pools_priority_concurrency` and watch Flower (http://localhost:5555) while it runs.
2. Configure `AIRFLOW__CORE__EXECUTOR: "LocalExecutor,CeleryExecutor"` and add `executor="CeleryExecutor"` to only `heavy_query` in `15_pools_priority_concurrency`. Check in the logs which container ran which task.
3. Generate a Fernet key, put it in `.env`, run `make down && make up`, create a connection in the UI, then open `docker compose exec postgres psql -U airflow -d airflow` and run `SELECT conn_id, password, is_encrypted FROM connection;` to see that the password is encrypted.
4. Run `airflow db clean --clean-before-timestamp "<a date>" --dry-run` in `make cli` and read which tables it would touch.

---

**Next →** [30-troubleshooting-faq.md](30-troubleshooting-faq.md) · [Back to the index](README.md)
