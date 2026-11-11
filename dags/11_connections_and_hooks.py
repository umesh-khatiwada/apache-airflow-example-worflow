"""
# 11 · Connections & Hooks — talking to databases and APIs

**Concepts:** Connections (credentials kept OUT of code), reading a connection,
`SQLExecuteQueryOperator` with a `.sql` file, `PostgresHook`, `HttpHook`-style API
calls through a custom hook, `template_searchpath`, `pool`.

Connections used (pre-configured in docker-compose.yaml as env vars):

| conn_id | type | points to |
|---------|------|-----------|
| `demo_postgres`  | postgres | the `demo` database in the Postgres container |
| `open_meteo_api` | http     | https://api.open-meteo.com (free weather API) |

```
create_table ─► fetch_weather (mapped per city) ─► save_to_postgres ─► latest_rows (SQL) ─► print_rows
show_connection_info
```
No internet? `fetch_weather` falls back to fake data so the DAG still succeeds.

📖 docs/15-connections-and-hooks.md
"""

from __future__ import annotations

import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import BaseHook, dag, task

from tutorial_lib.hooks.open_meteo import OpenMeteoHook
from tutorial_lib.paths import SQL_DIR

CITIES = [
    {"city": "Kathmandu", "latitude": 27.71, "longitude": 85.32},
    {"city": "London", "latitude": 51.51, "longitude": -0.13},
    {"city": "New York", "latitude": 40.71, "longitude": -74.01},
]


@dag(
    dag_id="11_connections_and_hooks",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    tags=["tutorial", "08-integrations"],
    doc_md=__doc__,
    # Folders where Jinja looks for template files such as "create_weather_table.sql".
    template_searchpath=[str(SQL_DIR)],
)
def connections_and_hooks():
    @task
    def show_connection_info():
        """Read a connection object. NEVER print passwords — Airflow masks them in logs anyway."""
        conn = BaseHook.get_connection("demo_postgres")
        print(f"type={conn.conn_type} host={conn.host} port={conn.port} db={conn.schema} user={conn.login}")
        print(f"password printed -> {conn.password}  (look: it's masked as ***)")

    # SQL operator: runs SQL against any database that has a provider (Postgres, MySQL,
    # Snowflake, BigQuery…). A string ending in .sql is loaded from template_searchpath.
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="demo_postgres",
        sql="create_weather_table.sql",
    )

    # pool="api_pool" (2 slots) => at most 2 API calls at once, even with 3 cities.
    @task(pool="api_pool", retries=2)
    def fetch_weather(city: str, latitude: float, longitude: float) -> dict:
        hook = OpenMeteoHook(open_meteo_conn_id="open_meteo_api")
        return {"city": city, **hook.get_current_weather(latitude, longitude)}

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
        # get_records / get_first / get_df run queries and return results.
        total = hook.get_first("SELECT COUNT(*) FROM weather_observations")[0]
        print(f"Inserted {len(rows)} rows; table now has {total} rows")
        return len(rows)

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

    @task
    def print_rows(rows: list) -> None:
        for city, temperature, windspeed, source in rows:
            print(f"{city:<10} {temperature:>6} °C  {windspeed:>6} km/h  ({source})")

    weather = fetch_weather.expand_kwargs(CITIES)
    saved = save_to_postgres(weather)
    create_table >> weather
    saved >> latest_rows
    print_rows(latest_rows.output)
    show_connection_info()


connections_and_hooks()
