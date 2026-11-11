"""
# 08 · Sensors & deferrable operators — waiting for things

**Concepts:** `FileSensor`, `@task.sensor` + `PokeReturnValue`, custom sensor class,
`poke_interval`, `timeout`, `soft_fail`, modes `poke` vs `reschedule`,
**deferrable** sensors (handed to the *triggerer*, freeing the worker slot).

```
        ┌──► drop_file  (writes the file after ~15 s)
        ├──► wait_for_file        (FileSensor, poke) ───────────┐
start ──┼──► wait_for_rows        (custom sensor, reschedule) ──┼──► done
        ├──► wait_30_seconds      (TimeDeltaSensor, deferrable) ┘
        └──► wait_with_decorator  (@task.sensor, soft_fail)
```
All five start **in parallel** right after `start`: the sensors keep checking until
`drop_file` has written the file. `wait_with_decorator` is not connected to `done`
on purpose: with `soft_fail=True` a timeout would SKIP it, and that skip would
otherwise also skip `done`.

> Runs need a logical date here (`{{ ts_nodash }}`): trigger from the UI (it fills one in)
> or with `airflow dags trigger -l <date> 08_sensors_and_deferrable`.

📖 docs/12-sensors-and-deferrable.md
"""

from __future__ import annotations

import csv
import time
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from airflow.sdk import PokeReturnValue, dag, task

from tutorial_lib.paths import DATA_DIR
from tutorial_lib.sensors.csv_rows import CsvHasRowsSensor

# One file per run so runs don't interfere. {{ run_id }} can contain ':' and '+',
# so we use the ts_nodash template (e.g. 20261111T120000).
INCOMING_FILE = str(DATA_DIR / "incoming" / "orders_{{ ts_nodash }}.csv")


@dag(
    dag_id="08_sensors_and_deferrable",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    tags=["tutorial", "05-waiting"],
    doc_md=__doc__,
)
def sensors_and_deferrable():
    start = EmptyOperator(task_id="start")
    done = EmptyOperator(task_id="done")

    @task
    def drop_file(ts_nodash=None) -> str:
        """Simulates an upstream system delivering a file a little later."""
        time.sleep(15)
        path = DATA_DIR / "incoming" / f"orders_{ts_nodash}.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["order_id", "amount"])
            writer.writerows([[1, 10.5], [2, 99.0], [3, 7.25]])
        print(f"Wrote {path}")
        return str(path)

    # 1) Built-in FileSensor. mode="poke" keeps the worker slot busy while waiting —
    #    fine for short waits.
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=INCOMING_FILE,  # templated
        fs_conn_id="fs_default",
        poke_interval=5,  # check every 5 seconds
        timeout=timedelta(minutes=5),  # give up (fail) after 5 minutes
        mode="poke",
    )

    # 2) Our custom sensor (plugins/tutorial_lib/sensors/csv_rows.py).
    #    mode="reschedule" frees the worker slot between checks — use for long waits.
    wait_for_rows = CsvHasRowsSensor(
        task_id="wait_for_rows",
        file_path=INCOMING_FILE,
        min_rows=3,
        poke_interval=10,
        timeout=timedelta(minutes=5),
        mode="reschedule",
    )

    # 3) A sensor written as a decorated function. Return PokeReturnValue to also push XCom.
    @task.sensor(poke_interval=5, timeout=300, mode="poke", soft_fail=True)
    def wait_with_decorator(ts_nodash=None) -> PokeReturnValue:
        path = Path(DATA_DIR / "incoming" / f"orders_{ts_nodash}.csv")
        size = path.stat().st_size if path.exists() else 0
        # soft_fail=True: on timeout the task is SKIPPED instead of FAILED.
        return PokeReturnValue(is_done=size > 0, xcom_value={"bytes": size})

    # 4) Deferrable sensor: the wait is handed to the *triggerer* process (async),
    #    the task shows as DEFERRED (purple) and uses NO worker slot while waiting.
    wait_30_seconds = TimeDeltaSensor(
        task_id="wait_30_seconds",
        delta=timedelta(seconds=30),
        deferrable=True,
    )

    start >> [drop_file(), wait_for_file, wait_for_rows, wait_with_decorator(), wait_30_seconds]
    [wait_for_file, wait_for_rows, wait_30_seconds] >> done


sensors_and_deferrable()
