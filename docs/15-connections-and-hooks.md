# Connections & hooks

**What you'll learn:** how Airflow stores credentials for outside systems (*Connections*), how your
code uses them through *hooks*, and how to run SQL safely against a database — all by walking through a
DAG that fetches live weather data and saves it to Postgres.

> **Run it**
> - DAG id: `11_connections_and_hooks` — file [`../dags/11_connections_and_hooks.py`](../dags/11_connections_and_hooks.py)
> - Custom hook: [`../plugins/tutorial_lib/hooks/open_meteo.py`](../plugins/tutorial_lib/hooks/open_meteo.py)
> - SQL file: [`../dags/include/sql/create_weather_table.sql`](../dags/include/sql/create_weather_table.sql)
> - Unpause it in the UI, click **Trigger**, then look at the data with `make psql` (see the end of this chapter).
> - No internet? The weather task falls back to fake data, so the DAG still succeeds.

---

## 1. The problem connections solve

Your pipelines talk to databases, APIs, cloud storage, mail servers… All of them need a host name,
a user, a password or token. You **never** want those values in your DAG files:

- DAG files live in Git, so secrets would leak to everybody with repository access.
- The same DAG must point at a test database on your laptop and the real one in production.
- Rotating a password should not need a code change.

A **Connection** (see [glossary](32-glossary.md)) is a named bundle of those details, identified by a
`conn_id` such as `demo_postgres`. Code only knows the `conn_id`; Airflow looks up the rest at run time.

### The fields of a connection

| Field | Example (`demo_postgres` in Docker) | Notes |
|-------|-------------------------------------|-------|
| `conn_id` | `demo_postgres` | The name your code uses. Must be unique. |
| `conn_type` | `postgres` | Decides which hook/UI form applies (`postgres`, `http`, `aws`, `fs`…). |
| `host` | `postgres` | The Docker service name; `localhost` when running without Docker. |
| `port` | `5432` | |
| `login` | `airflow` | User name. |
| `password` | `airflow` | Stored encrypted in the metadata DB, masked in logs. |
| `schema` | `demo` | For databases this is the **database name** (an old naming quirk). |
| `extra` | `{"sslmode": "require"}` | A JSON blob for anything else. Read it with `conn.extra_dejson`. |
| `description` | free text | Shown in the UI. |

### The two connections this DAG uses

Both are defined as environment variables in `docker-compose.yaml` (and the same in
`scripts/local_env.sh` for running without Docker):

```yaml
AIRFLOW_CONN_DEMO_POSTGRES: '{"conn_type": "postgres", "host": "postgres", "port": 5432, "login": "airflow", "password": "airflow", "schema": "demo"}'
AIRFLOW_CONN_OPEN_METEO_API: '{"conn_type": "http", "host": "https://api.open-meteo.com"}'
```

| conn_id | type | points to |
|---------|------|-----------|
| `demo_postgres` | postgres | the `demo` database in the Postgres container |
| `open_meteo_api` | http | https://api.open-meteo.com (free weather API, no key needed) |

> ⚠️ **Environment-variable connections do not appear in the UI's connection list**, but they work.
> Don't be surprised when *Admin → Connections* is empty — the DAG still runs. You can confirm the CLI
> sees them with `airflow connections get demo_postgres` (it prints the password in clear text, so don't
> do this on a shared screen).

---

## 2. Four ways to create a connection

### a) In the UI

1. Open **Admin → Connections** and click **Add Connection** (the **+** button).
2. Fill in *Connection Id*, choose a *Connection Type* (the form changes per type — e.g. the Postgres
   form shows host, database, login, password, port), and fill in the fields.
3. Click **Test**. Airflow tries to connect and shows success or the error. The test button is disabled
   by default for security; this project turns it on with `AIRFLOW__CORE__TEST_CONNECTION: 'Enabled'`
   in `docker-compose.yaml`.
4. **Save**. The connection is stored in the metadata database, with the password encrypted (by the
   Fernet key).

### b) With the CLI

Run inside the containers (`make cli`) or locally after `source scripts/local_env.sh`:

```bash
# field by field
airflow connections add my_postgres \
    --conn-type postgres --conn-host localhost --conn-port 5432 \
    --conn-login airflow --conn-password airflow --conn-schema demo

# or as a URI
airflow connections add my_api --conn-uri 'http://api.example.com/?timeout=10'

# or as JSON
airflow connections add my_api2 --conn-json '{"conn_type": "http", "host": "https://api.example.com"}'

airflow connections list          # what's stored in the metadata DB
airflow connections get my_postgres
airflow connections test my_postgres
airflow connections delete my_api
airflow connections export conns.json   # / import conns.json
```

### c) As environment variables (what this project does)

Name the variable `AIRFLOW_CONN_<CONN_ID in UPPER CASE>`. Two value formats are accepted:

```bash
# JSON format (clearest; used in this project)
AIRFLOW_CONN_DEMO_POSTGRES='{"conn_type": "postgres", "host": "postgres", "port": 5432,
                             "login": "airflow", "password": "airflow", "schema": "demo"}'

# URI format:  <conn_type>://<login>:<password>@<host>:<port>/<schema>?<extra key>=<value>
AIRFLOW_CONN_DEMO_POSTGRES='postgresql://airflow:airflow@postgres:5432/demo'
```

The URI scheme becomes the `conn_type` (`postgresql` is normalised to `postgres`), the path becomes
`schema`, and query parameters go into `extra`. Special characters in a password must be URL-encoded
in a URI (`@` → `%40`), which is one reason the JSON format is easier.

Env-var connections are read-only (you cannot edit them in the UI), are never written to the
database, and are a good fit for Docker / Kubernetes deployments where config comes from the
environment.

### d) From a secrets backend (production)

In production, credentials usually live in a dedicated secret store. Airflow can read connections
(and variables) directly from it — you configure a **secrets backend** in `airflow.cfg`:

```ini
[secrets]
backend = airflow.providers.hashicorp.secrets.vault.VaultBackend
backend_kwargs = {"connections_path": "connections", "url": "http://vault:8200", "mount_point": "airflow"}
```

Other common backends: AWS Secrets Manager / SSM Parameter Store (`apache-airflow-providers-amazon`),
GCP Secret Manager (`...-google`), Azure Key Vault (`...-microsoft-azure`). When code asks for a
`conn_id`, Airflow searches in this order and stops at the first hit:

```mermaid
flowchart LR
    A["get_connection('demo_postgres')"] --> B["Secrets backend<br/>(if configured)"]
    B -- not found --> C["Environment variable<br/>AIRFLOW_CONN_DEMO_POSTGRES"]
    C -- not found --> D["Metadata DB<br/>(UI / CLI connections)"]
    D -- not found --> E["Error: connection not defined"]
```

More: [Managing connections](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html)
and [Secrets backends](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html).

---

## 3. Hooks: code that knows how to talk to one system

A **hook** (see [glossary](32-glossary.md)) is a Python class that:

1. takes a `conn_id`,
2. looks up the connection,
3. opens a client/session to the external system, and
4. offers convenient methods (`get_records`, `insert_rows`, `run`, …).

Operators are built **on top of** hooks: `SQLExecuteQueryOperator` uses a database hook internally,
`HttpOperator` uses `HttpHook`. In `@task` functions you use hooks directly.

```mermaid
flowchart LR
    T["@task / operator"] -->|"conn_id"| H["Hook<br/>(PostgresHook, HttpHook, OpenMeteoHook)"]
    H -->|"get_connection()"| C["Connection<br/>host, login, password…"]
    H -->|"driver / HTTP"| X[("External system<br/>Postgres, API…")]
```

Hooks come from **provider packages** (`apache-airflow-providers-postgres`, `-http`, `-amazon`, …).
The official `apache/airflow` Docker image already contains the `postgres`, `http`, `common-sql`
and `standard` providers (and many more); extra ones go in `requirements.txt`.

---

## 4. Walking through the DAG

```
create_table ─► fetch_weather (mapped per city) ─► save_to_postgres ─► latest_rows (SQL) ─► print_rows
show_connection_info
```

### The DAG definition and `template_searchpath`

```python
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import BaseHook, dag, task
from tutorial_lib.hooks.open_meteo import OpenMeteoHook
from tutorial_lib.paths import SQL_DIR

@dag(
    dag_id="11_connections_and_hooks",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    tags=["tutorial", "08-integrations"],
    doc_md=__doc__,
    # Folders where Jinja looks for template files such as "create_weather_table.sql".
    template_searchpath=[str(SQL_DIR)],
)
```

`template_searchpath` is a list of folders where Airflow looks for **template files**. `SQL_DIR` comes
from [`../plugins/tutorial_lib/paths.py`](../plugins/tutorial_lib/paths.py) and resolves to
`dags/include/sql`. Templating is covered in depth in
[chapter 14](14-params-variables-templating.md).

### Task `show_connection_info` — reading a connection

```python
@task
def show_connection_info():
    """Read a connection object. NEVER print passwords — Airflow masks them in logs anyway."""
    conn = BaseHook.get_connection("demo_postgres")
    print(f"type={conn.conn_type} host={conn.host} port={conn.port} db={conn.schema} user={conn.login}")
    print(f"password printed -> {conn.password}  (look: it's masked as ***)")
```

`BaseHook.get_connection(conn_id)` returns a `Connection` object with the fields from the table above
(plus `extra_dejson` and `get_uri()`). In the task log you will see the password replaced by `***`:
Airflow's **secrets masker** registers every connection password it loads and redacts it from logs.

> 💡 Masking works by searching the log text for the secret value. In this demo the password is
> `airflow` — the same as the user name — so you will probably see `user=***` too. With real, long,
> random passwords this doesn't happen. Masking is a safety net, not permission to print secrets.

### Task `create_table` — SQL from a file

```python
create_table = SQLExecuteQueryOperator(
    task_id="create_table",
    conn_id="demo_postgres",
    sql="create_weather_table.sql",
)
```

`SQLExecuteQueryOperator` (from the `common-sql` provider) runs SQL against **any** database that has a
provider — Postgres, MySQL, Snowflake, BigQuery and more. It picks the right hook from the connection's
`conn_type`.

Because `sql` ends in `.sql` (one of the operator's `template_ext`), Airflow treats it as a **file
name**, finds it in `template_searchpath`, renders it with Jinja, and runs the content:

```sql
-- dags/include/sql/create_weather_table.sql
CREATE TABLE IF NOT EXISTS weather_observations (
    id           SERIAL PRIMARY KEY,
    city         VARCHAR(50)  NOT NULL,
    temperature  NUMERIC(5, 1),
    windspeed    NUMERIC(5, 1),
    source       VARCHAR(10),
    run_id       VARCHAR(250) NOT NULL,
    observed_at  TIMESTAMP    NOT NULL DEFAULT NOW()
);
```

`IF NOT EXISTS` makes the task **idempotent**: running it twice does no harm. Keeping SQL in `.sql`
files gives you syntax highlighting, easier review, and the full rendered SQL in the task's
**Rendered Template** tab.

### Task `fetch_weather` — a custom hook, mapped, in a pool

```python
# pool="api_pool" (2 slots) => at most 2 API calls at once, even with 3 cities.
@task(pool="api_pool", retries=2)
def fetch_weather(city: str, latitude: float, longitude: float) -> dict:
    hook = OpenMeteoHook(open_meteo_conn_id="open_meteo_api")
    return {"city": city, **hook.get_current_weather(latitude, longitude)}
```

and later:

```python
weather = fetch_weather.expand_kwargs(CITIES)
```

Three ideas meet here:

- **Dynamic task mapping** — `expand_kwargs(CITIES)` creates one task instance per city
  (Kathmandu, London, New York). See [chapter 10](10-dynamic-task-mapping.md).
- **Pool** — `api_pool` has 2 slots (from [`../config/pools.json`](../config/pools.json)), so only two of
  the three mapped instances can call the API at the same time; the third waits in the *scheduled*
  state. This is how you protect a rate-limited API. See
  [chapter 19](19-pools-priority-concurrency.md).
- **Retries** — network calls fail sometimes; `retries=2` gives two more tries. See
  [chapter 16](16-error-handling-retries-alerts.md).

The hook itself, in [`../plugins/tutorial_lib/hooks/open_meteo.py`](../plugins/tutorial_lib/hooks/open_meteo.py):

```python
class OpenMeteoHook(BaseHook):
    # These four class attributes tell Airflow how this hook relates to connections.
    conn_name_attr = "open_meteo_conn_id"
    default_conn_name = "open_meteo_api"
    conn_type = "http"
    hook_name = "Open-Meteo"

    def get_base_url(self) -> str:
        conn = self.get_connection(self.open_meteo_conn_id)
        host = conn.host or "https://api.open-meteo.com"
        if "://" not in host:
            host = f"{conn.schema or 'https'}://{host}"
        return host.rstrip("/")
```

`get_current_weather()` calls `GET <base url>/v1/forecast?latitude=…&longitude=…&current_weather=true`
with `requests` and returns `{"temperature": …, "windspeed": …, "source": "api"}`. If the request
fails (no internet, bad response) and `offline_fallback=True` (the default), it logs a warning and
returns made-up but stable numbers with `"source": "fallback"`. Writing your own hooks is covered in
[chapter 17](17-custom-operators-hooks-plugins.md).

### Task `save_to_postgres` — `PostgresHook`

```python
@task
def save_to_postgres(observations: list[dict], run_id=None) -> int:
    hook = PostgresHook(postgres_conn_id="demo_postgres")
    rows = [(o["city"], o["temperature"], o["windspeed"], o["source"], run_id) for o in observations]
    # insert_rows handles the INSERT statement and commits for you.
    hook.insert_rows(
        table="weather_observations",
        rows=rows,
        target_fields=["city", "temperature", "windspeed", "source", "run_id"],
    )
    # get_records / get_first / get_pandas_df run queries and return results.
    total = hook.get_first("SELECT COUNT(*) FROM weather_observations")[0]
    print(f"Inserted {len(rows)} rows; table now has {total} rows")
    return len(rows)
```

- `observations` receives the list of results of all mapped `fetch_weather` instances.
- `run_id=None` in the signature asks Airflow to inject the `run_id` context value; it's stored with
  every row so later tasks can find "this run's" rows.

The most useful `PostgresHook` methods (most come from the shared `DbApiHook` base class, so they work
the same for MySQL, SQLite, Snowflake…):

| Method | What it does | Returns |
|--------|--------------|---------|
| `run(sql, parameters=None, autocommit=False, handler=None)` | Execute one or more statements (DDL, UPDATE, DELETE…) | `None`, or the handler's result |
| `get_records(sql, parameters=None)` | Run a SELECT | list of row tuples |
| `get_first(sql, parameters=None)` | Run a SELECT | the first row tuple (or `None`) |
| `insert_rows(table, rows, target_fields=None, commit_every=1000, replace=False)` | Build and run INSERTs for a list of tuples, commit in batches | `None` |
| `get_df(sql, parameters=None, df_type="pandas")` | Query into a pandas (or polars) DataFrame | DataFrame |
| `get_pandas_df(sql, parameters=None)` | Older name for the above — **deprecated** in the installed `common-sql` provider; prefer `get_df` | DataFrame |
| `get_conn()` | The raw DB-API connection (psycopg) for anything else | connection |
| `get_sqlalchemy_engine()` | A SQLAlchemy engine (e.g. for `DataFrame.to_sql`) | engine |
| `copy_expert(sql, filename)` / `bulk_load(table, tmp_file)` | Postgres `COPY` for big files | `None` |

### Task `latest_rows` — query results as XCom, and safe parameters

```python
# The results of a SELECT are returned as XCom (a list of rows).
latest_rows = SQLExecuteQueryOperator(
    task_id="latest_rows",
    conn_id="demo_postgres",
    sql="""
        SELECT city, temperature, windspeed, source
        FROM weather_observations
        WHERE run_id = %(run_id)s
        ORDER BY city
    """,
    parameters={"run_id": "{{ run_id }}"},  # always use parameters, never f-strings, for values
)
```

Two important things:

**1. Results go to XCom.** `SQLExecuteQueryOperator` fetches the rows of the (last) statement and
returns them, so they're pushed to XCom as a list of rows. `print_rows(latest_rows.output)` then
receives them:

```python
@task
def print_rows(rows: list) -> None:
    for city, temperature, windspeed, source in rows:
        print(f"{city:<10} {temperature:>6} °C  {windspeed:>6} km/h  ({source})")
```

XCom lives in the metadata database, so only do this for **small** results (a few rows, a count).
For big results, write to a table or file and pass the location. See [chapter 8](08-xcom.md).

**2. Values go in `parameters`, not into the SQL text.** `parameters` is a template field, so
`{{ run_id }}` is rendered first; then the database driver substitutes `%(run_id)s` safely. Compare:

| Approach | Example | Safe? |
|----------|---------|-------|
| Driver parameters | `WHERE run_id = %(run_id)s` + `parameters={"run_id": ...}` | ✅ The driver escapes the value. |
| Jinja in the SQL text | `WHERE run_id = '{{ run_id }}'` | ⚠️ OK for values Airflow controls (dates, run ids), dangerous for anything a user can type (Params, conf, API data). |
| Python f-string | `f"WHERE city = '{city}'"` | ❌ **SQL injection**: a value like `x'; DROP TABLE weather_observations; --` becomes code. |

Rule of thumb: use Jinja for **structure** you control (table names, date partitions), use
`parameters` for **values**.

### Dependencies

```python
weather = fetch_weather.expand_kwargs(CITIES)
saved = save_to_postgres(weather)
create_table >> weather
saved >> latest_rows
print_rows(latest_rows.output)
show_connection_info()
```

`save_to_postgres(weather)` and `print_rows(latest_rows.output)` create dependencies automatically
(data flows through XCom). `create_table >> weather` and `saved >> latest_rows` are explicit because no
data passes between them. `show_connection_info` has no dependencies, so it runs in parallel.

---

## 5. HTTP APIs: `HttpHook` and `HttpOperator`

This project wrote a small custom hook for the weather API to show how hooks work. For everyday
HTTP calls the `http` provider already gives you:

```python
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.http.operators.http import HttpOperator

# Inside a @task
hook = HttpHook(method="GET", http_conn_id="open_meteo_api")
response = hook.run("v1/forecast", data={"latitude": 27.71, "longitude": 85.32, "current_weather": "true"})
print(response.json())

# As an operator; the response text is pushed to XCom (response_filter lets you pick a part)
get_weather = HttpOperator(
    task_id="get_weather",
    http_conn_id="open_meteo_api",
    method="GET",
    endpoint="v1/forecast",
    data={"latitude": 27.71, "longitude": 85.32, "current_weather": "true"},
    response_filter=lambda r: r.json()["current_weather"],
)
```

`HttpOperator` also supports `response_check`, `pagination_function` and `deferrable=True`. Put
API-calling tasks in a pool, as `fetch_weather` does, so you don't hammer the API.

---

## 6. Looking at the data

With Docker running:

```bash
make psql          # = docker compose exec postgres psql -U airflow -d demo
```

```sql
\dt                                           -- list tables
SELECT city, temperature, windspeed, source, run_id, observed_at
FROM weather_observations
ORDER BY observed_at DESC
LIMIT 10;
\q                                            -- quit
```

Postgres is also exposed on `localhost:5432` (user `airflow`, password `airflow`, database `demo`),
so a desktop tool such as DBeaver or `psql -h localhost -U airflow -d demo` works too.

Each DAG run adds three rows (one per city). The `source` column tells you whether the numbers came
from the real API (`api`) or the offline fallback (`fallback`).

---

## Common mistakes

- **Hard-coding credentials** in DAG files or `.sql` files. Always use a `conn_id`.
- **Looking for env-var connections in the UI** and concluding they don't exist. They don't show up
  there; use `airflow connections get <id>` or simply run the task.
- **Calling `get_connection()` or a hook at the top level of the DAG file.** That runs on every parse
  (every 30 seconds) in the dag-processor. Only use hooks inside tasks.
- **Building SQL with f-strings** from user input → SQL injection. Use `parameters`.
- **Returning huge query results** via XCom. Write them to a table/file instead.
- **Using `schema` wrongly**: for database connections it's the database name (`demo`), not a
  Postgres schema like `public`.

## Try it yourself

1. Add a fourth city to `CITIES` (e.g. Tokyo, 35.68 / 139.69). Trigger the DAG and watch in the
   Grid view how only two `fetch_weather` instances run at a time because of `api_pool`.
2. In `save_to_postgres`, replace `get_first(...)` with `hook.get_records("SELECT city, COUNT(*) FROM
   weather_observations GROUP BY city")` and print the result.
3. Create a connection `my_demo_db` in the UI with the same settings as `demo_postgres` but host
   `postgres` (Docker) or `localhost` (local), press **Test**, then change `conn_id` in
   `show_connection_info` to use it.
4. Change `latest_rows` to only return cities warmer than a threshold, passing the threshold through
   `parameters` (e.g. `AND temperature > %(min_temp)s` with `"min_temp": 10`).

---

**Next →** [16 · Error handling, retries & alerts](16-error-handling-retries-alerts.md) ·
[Back to the index](README.md)
