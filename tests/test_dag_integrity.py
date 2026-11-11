"""
DAG integrity tests — the most valuable tests for any Airflow project.

They catch, before deploying:
* syntax errors / bad imports (a broken file makes its DAGs disappear from the UI!)
* cycles, duplicate task ids, invalid arguments
* team conventions (tags, owners, retries…)
"""

from __future__ import annotations

import pytest

EXPECTED_DAG_IDS = {
    "01_hello_airflow",
    "02_taskflow_api",
    "03_operators_and_dependencies",
    "04_xcom",
    "05_branching_and_trigger_rules",
    "06_dynamic_task_mapping",
    "07_task_groups",
    "08_sensors_and_deferrable",
    "09_assets_producer_orders",
    "09_assets_producer_customers",
    "09_assets_consumer",
    "09_assets_consumer_and_or",
    "09_assets_or_time",
    "tutorial_daily_kpis",
    "10_params_variables_templating",
    "11_connections_and_hooks",
    "12_error_handling_and_alerts",
    "13_custom_operators_hooks_sensors",
    "15_pools_priority_concurrency",
    "16_setup_teardown",
    "17_generated_ingest_crm",
    "17_generated_ingest_erp",
    "17_generated_ingest_web",
    "18_human_in_the_loop",
    "19_task_loop_fixed",
    "19_task_loop_until",
    "20_capstone_sales_etl",
    "20_capstone_sales_report",
}


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, f"DAG import failures: {dagbag.import_errors}"


def test_expected_dags_are_present(dagbag):
    missing = EXPECTED_DAG_IDS - set(dagbag.dag_ids)
    assert not missing, f"Missing DAGs: {sorted(missing)}"


def test_scheduling_examples_are_generated(dagbag):
    scheduling = [d for d in dagbag.dag_ids if d.startswith("14_schedule_")]
    assert len(scheduling) >= 8


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAG_IDS))
def test_every_dag_has_tutorial_tag(dagbag, dag_id):
    assert "tutorial" in dagbag.get_dag(dag_id).tags


def test_every_dag_has_tasks(dagbag):
    empty = [dag_id for dag_id, dag in dagbag.dags.items() if not dag.tasks]
    assert not empty, f"DAGs without tasks: {empty}"


def test_dag_ids_match_their_file_number(dagbag):
    """Convention in this repo: DAG ids start with the number of the file that defines them."""
    for dag_id, dag in dagbag.dags.items():
        if dag_id == "tutorial_daily_kpis":  # @asset DAGs are named after the function
            continue
        file_number = dag.fileloc.rsplit("/", 1)[-1][:2]
        assert dag_id.startswith(file_number), f"{dag_id} defined in {dag.fileloc}"
