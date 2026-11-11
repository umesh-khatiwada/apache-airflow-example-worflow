"""
Integration tests: actually RUN a DAG in-process with `dag.test()`.

`dag.test()` executes every task in order inside this Python process — no scheduler or
workers needed — and returns the finished DagRun. It needs an initialised metadata DB:

    source scripts/local_env.sh && airflow db migrate

Run only these tests with:  pytest -m integration
"""

from __future__ import annotations

from pathlib import Path

import pendulum
import pytest

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module", autouse=True)
def require_metadata_db():
    import os

    db_file = Path(os.environ["AIRFLOW_HOME"]) / "airflow.db"
    if not db_file.exists():
        pytest.skip("Metadata DB not initialised. Run: source scripts/local_env.sh && airflow db migrate")


@pytest.mark.parametrize(
    "dag_id",
    ["01_hello_airflow", "02_taskflow_api", "04_xcom", "06_dynamic_task_mapping", "07_task_groups"],
)
def test_dag_runs_successfully(dagbag, dag_id):
    dag = dagbag.get_dag(dag_id)
    dag_run = dag.test(logical_date=pendulum.datetime(2026, 1, 5, tz="UTC"))
    assert dag_run.state == "success", f"{dag_id} finished in state {dag_run.state}"


def test_branching_follows_param(dagbag):
    dag = dagbag.get_dag("05_branching_and_trigger_rules")
    dag_run = dag.test(
        logical_date=pendulum.datetime(2026, 1, 6, tz="UTC"),
        run_conf={"order_count": 5, "make_it_fail": False},
    )
    states = {ti.task_id: ti.state for ti in dag_run.get_task_instances()}
    assert states["process_small"] == "success"
    assert states["process_large"] == "skipped"
    assert states["join"] == "success"
    assert states["on_any_failure"] == "skipped"
