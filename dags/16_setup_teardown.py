"""
# 16 · Setup & teardown tasks — always clean up

**Concepts:** `@setup`, `@teardown`, `.as_teardown(setups=...)`, setup/teardown
scopes, and why teardown runs even when work fails.

Typical use: create a temporary resource (cluster, table, folder, VM), do work with it,
and **always** delete it afterwards — even if the work failed — so you don't leak costs.

```
create_workspace (setup) ─► process ─► summarise ─► delete_workspace (teardown)
                     └─────────────────────────────────────┘ (dotted line in UI)
```

Trigger with `fail_processing = true`: `process` fails, but `delete_workspace` still
runs. The DAG run is still marked **failed** (teardown results don't hide failures).

📖 docs/20-setup-teardown.md
"""

from __future__ import annotations

import shutil

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, setup, task, teardown

from tutorial_lib.paths import DATA_DIR


@dag(
    dag_id="16_setup_teardown",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    params={"fail_processing": Param(False, type="boolean")},
    tags=["tutorial", "09-reliability"],
    doc_md=__doc__,
)
def setup_teardown():
    @setup
    def create_workspace(ts_nodash=None) -> str:
        workspace = DATA_DIR / "workspaces" / ts_nodash
        workspace.mkdir(parents=True, exist_ok=True)
        print(f"Created {workspace}")
        return str(workspace)

    @task(retries=0)
    def process(workspace: str, params=None) -> str:
        if params["fail_processing"]:
            raise RuntimeError("Processing failed (on purpose)")
        output = f"{workspace}/result.txt"
        with open(output, "w") as f:
            f.write("42\n")
        return output

    @task
    def summarise(output: str) -> None:
        with open(output) as f:
            print(f"Result: {f.read().strip()}")

    @teardown
    def delete_workspace(workspace: str) -> None:
        shutil.rmtree(workspace, ignore_errors=True)
        print(f"Deleted {workspace}")

    workspace = create_workspace()
    result = process(workspace)
    summarise(result) >> delete_workspace(workspace)
    # Passing `workspace` (the setup's output) into the teardown links them as a pair.

    # ---- Classic operators: mark an existing task as teardown with as_teardown() -----
    start_fake_service = BashOperator(task_id="start_fake_service", bash_command="echo 'service started'")
    use_fake_service = BashOperator(task_id="use_fake_service", bash_command="echo 'using service'")
    stop_fake_service = BashOperator(task_id="stop_fake_service", bash_command="echo 'service stopped'")
    start_fake_service >> use_fake_service >> stop_fake_service.as_teardown(setups=start_fake_service)


setup_teardown()
