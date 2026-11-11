"""
# 10 · Params, Variables, Jinja templating & macros

**Concepts:**
* **Params** – typed inputs for a run; Airflow renders a **form** when you click
  *Trigger*. Validated with JSON-schema.
* **Variables** – global key/value settings stored in Airflow (Admin ▸ Variables) or
  env vars `AIRFLOW_VAR_<KEY>`.
* **Jinja templates** – `{{ ... }}` placeholders in operator fields, filled at runtime:
  `ds`, `run_id`, `params`, `var`, `conn`, `macros`, `ti`…
* **Custom macros** – `user_defined_macros` and plugin macros.

**Try it:** Trigger ▸ fill the form ▸ Trigger. Then look at a task's **Rendered
Template** tab to see what each `{{ }}` became.

📖 docs/14-params-variables-templating.md
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, Variable, dag, task


def days_between(start: str, end: str) -> int:
    """A custom macro usable as {{ days_between(a, b) }}."""
    return (pendulum.parse(end) - pendulum.parse(start)).days


@dag(
    dag_id="10_params_variables_templating",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    tags=["tutorial", "07-configuration"],
    doc_md=__doc__,
    user_defined_macros={"days_between": days_between, "company": "Acme Corp"},
    params={
        # Each Param becomes a field in the trigger form.
        "customer_name": Param("Ada Lovelace", type="string", minLength=2, title="Customer name"),
        "report_type": Param(
            "daily",
            type="string",
            enum=["daily", "weekly", "monthly"],
            description="Which report to build",
        ),
        "max_rows": Param(100, type="integer", minimum=1, maximum=10_000),
        "dry_run": Param(True, type="boolean", description="If true, nothing is written"),
        # Defaults must be static: never compute them with today()/now() at parse time.
        "report_date": Param("2026-01-01", type="string", format="date", title="Report date"),
        "regions": Param(["north", "south"], type="array", description="One region per line"),
        "options": Param({"compress": True, "format": "csv"}, type="object"),
        "comment": Param(None, type=["null", "string"], description="Optional free text"),
    },
)
def params_variables_templating():
    # ---- Params & Variables inside Python tasks ----------------------------------------
    @task
    def use_params_and_variables(params=None) -> dict:
        # Variable.get reads at RUNTIME. Always give a default for optional ones.
        environment = Variable.get("environment", default="unknown")
        settings = Variable.get("tutorial_settings", default={}, deserialize_json=True)
        print(f"Environment: {environment}; team: {settings.get('team')}")
        print(f"Building a {params['report_type']} report for {params['customer_name']}")
        print(f"Regions: {params['regions']}, options: {params['options']}, dry_run: {params['dry_run']}")
        return {"environment": environment, "row_limit": min(params["max_rows"], settings.get("row_limit", 100))}

    # ---- Built-in template variables ----------------------------------------------------
    show_builtin_templates = BashOperator(
        task_id="show_builtin_templates",
        bash_command=(
            "echo 'run_id         = {{ run_id }}' && "
            "echo 'logical date   = {{ logical_date }}' && "
            "echo 'ds / ds_nodash = {{ ds }} / {{ ds_nodash }}' && "
            "echo 'interval       = {{ data_interval_start }} -> {{ data_interval_end }}' && "
            "echo 'dag / task     = {{ dag.dag_id }} / {{ task.task_id }}' && "
            "echo 'try number     = {{ ti.try_number }}'"
        ),
    )

    # ---- Params, Variables, macros & filters in templates ---------------------------
    show_params_vars_macros = BashOperator(
        task_id="show_params_vars_macros",
        bash_command=(
            "echo 'customer       = {{ params.customer_name }}' && "
            "echo 'environment    = {{ var.value.environment }}' && "
            "echo 'team (json var)= {{ var.json.tutorial_settings.team }}' && "
            "echo 'missing var    = {{ var.value.get(\"does_not_exist\", \"fallback\") }}' && "
            "echo 'ds + 7 days    = {{ macros.ds_add(ds, 7) }}' && "
            "echo 'formatted      = {{ macros.ds_format(ds, \"%Y-%m-%d\", \"%d %b %Y\") }}' && "
            "echo 'uppercase      = {{ params.report_type | upper }}' && "
            "echo 'custom macro   = {{ days_between(\"2026-01-01\", ds) }} days since new year at {{ company }}' && "
            "echo 'plugin macro   = {{ macros.tutorial_plugin.env_banner(var.value.environment) }}'"
        ),
    )

    # ---- Templates in TaskFlow: use templates_dict, or read the context -------------
    @task(templates_dict={"target_table": "reports_{{ params.report_type }}_{{ ds_nodash }}"})
    def templated_python(templates_dict=None, ds=None):
        print(f"Would write to table {templates_dict['target_table']} for {ds}")

    use_params_and_variables() >> [show_builtin_templates, show_params_vars_macros] >> templated_python()


params_variables_templating()
