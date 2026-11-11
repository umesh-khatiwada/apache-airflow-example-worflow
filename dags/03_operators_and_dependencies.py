"""
# 03 · Operators tour & ways to set dependencies

**Concepts:** EmptyOperator, BashOperator (env vars, exit codes, templating),
PythonOperator (`op_args`/`op_kwargs`), `@task.bash`, `chain()`, `cross_downstream()`,
lists in dependencies, edge `Label`s, `TriggerDagRunOperator`.

```
                 ┌─► bash_with_env ─┐
begin ─► fan_out ┼─► python_args ───┼─► fan_in ─► [a1, a2] ═cross═► [b1, b2] ─► chain_1 ─► chain_2 ─► trigger_hello
                 └─► bash_skip ─────┘
```
`bash_skip` exits with code **99**, which BashOperator treats as *skipped* (pink in the UI).

📖 docs/07-operators-and-dependencies.md
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Label, TriggerRule, chain, cross_downstream, task


def greet(name: str, greeting: str = "Hello", *, punctuation: str = "!") -> str:
    message = f"{greeting}, {name}{punctuation}"
    print(message)
    return message


with DAG(
    dag_id="03_operators_and_dependencies",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    tags=["tutorial", "01-basics"],
    doc_md=__doc__,
) as dag:
    begin = EmptyOperator(task_id="begin")
    fan_out = EmptyOperator(task_id="fan_out")

    # --- BashOperator ---------------------------------------------------------------
    bash_with_env = BashOperator(
        task_id="bash_with_env",
        # `env` sets environment variables for the command; values can be templated too.
        env={"RUN_ID": "{{ run_id }}", "GREETING": "hi"},
        append_env=True,  # keep the normal environment (PATH etc.) as well
        bash_command='echo "$GREETING from run $RUN_ID"; uname -a',
    )

    bash_skip = BashOperator(
        task_id="bash_skip",
        bash_command="echo 'nothing to do today' && exit 99",  # exit 99 => task is SKIPPED
        # skip_on_exit_code=99 is the default. Any other non-zero code => FAILED.
    )

    # --- PythonOperator -------------------------------------------------------------
    python_args = PythonOperator(
        task_id="python_args",
        python_callable=greet,
        op_args=["Airflow learner"],  # positional arguments
        op_kwargs={"greeting": "Welcome", "punctuation": " 🎉"},  # keyword arguments
    )

    # NONE_FAILED: run even though one upstream (bash_skip) was skipped.
    # The default trigger rule ALL_SUCCESS would skip fan_in too. More in example 05.
    fan_in = EmptyOperator(task_id="fan_in", trigger_rule=TriggerRule.NONE_FAILED)

    # Lists: one task -> many tasks, many -> one.
    begin >> fan_out >> [bash_with_env, python_args, bash_skip] >> fan_in

    # Labels annotate edges in the Graph view.
    fan_out >> Label("only on weekends") >> bash_skip

    # --- cross_downstream: every task in list 1 -> every task in list 2 --------------
    a1, a2 = EmptyOperator(task_id="a1"), EmptyOperator(task_id="a2")
    b1, b2 = EmptyOperator(task_id="b1"), EmptyOperator(task_id="b2")
    fan_in >> [a1, a2]
    cross_downstream([a1, a2], [b1, b2])

    # --- chain(): readable long sequences -------------------------------------------
    @task.bash
    def chain_1() -> str:
        return "echo 'step 1 of the chain'"

    @task.bash
    def chain_2() -> str:
        return "echo 'step 2 of the chain'"

    # --- Trigger another DAG ----------------------------------------------------------
    # Creates a run of 01_hello_airflow (it only executes if that DAG is unpaused).
    trigger_hello = TriggerDagRunOperator(
        task_id="trigger_hello",
        trigger_dag_id="01_hello_airflow",
        conf={"triggered_by": "03_operators_and_dependencies"},
        wait_for_completion=False,
    )

    # chain(a, b, c) == a >> b >> c. Lists inside chain fan out/in.
    chain([b1, b2], chain_1(), chain_2(), trigger_hello)
