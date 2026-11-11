"""
Structure tests — assert the *shape* of important DAGs so refactors don't silently
change behaviour (dependencies, trigger rules, schedules, retries).
"""

from __future__ import annotations

from datetime import timedelta


def downstream_ids(dag, task_id: str) -> set[str]:
    return set(dag.get_task(task_id).downstream_task_ids)


def test_hello_airflow_is_a_straight_line(dagbag):
    dag = dagbag.get_dag("01_hello_airflow")
    assert downstream_ids(dag, "start") == {"say_hello"}
    assert downstream_ids(dag, "say_hello") == {"print_context"}
    assert downstream_ids(dag, "print_context") == {"end"}
    assert dag.default_args["retries"] == 1


def test_taskflow_transform_logic(dagbag):
    """TaskFlow functions are plain Python — call the underlying function directly."""
    dag = dagbag.get_dag("02_taskflow_api")
    transform = dag.get_task("transform").python_callable
    assert transform({"a": 1.5, "b": 2.25}) == {"total": 3.75, "count": 2}


def test_branch_join_uses_correct_trigger_rule(dagbag):
    dag = dagbag.get_dag("05_branching_and_trigger_rules")
    assert dag.get_task("join").trigger_rule == "none_failed_min_one_success"
    assert dag.get_task("cleanup").trigger_rule == "all_done"
    assert dag.get_task("on_any_failure").trigger_rule == "one_failed"
    assert downstream_ids(dag, "choose_plan") == {"process_small", "process_large"}


def test_error_handling_defaults(dagbag):
    dag = dagbag.get_dag("12_error_handling_and_alerts")
    flaky = dag.get_task("flaky_api_call")
    assert flaky.retries == 3
    assert flaky.retry_delay == timedelta(seconds=5)
    assert dag.get_task("slow_task").execution_timeout == timedelta(seconds=10)
    assert dag.dagrun_timeout == timedelta(minutes=30)


def test_setup_teardown_pairing(dagbag):
    dag = dagbag.get_dag("16_setup_teardown")
    assert dag.get_task("create_workspace").is_setup
    assert dag.get_task("delete_workspace").is_teardown
    assert dag.get_task("stop_fake_service").is_teardown


def test_capstone_has_quality_gate_before_publish(dagbag):
    dag = dagbag.get_dag("20_capstone_sales_etl")
    publish = dag.get_task("publish")
    assert {"quality_checks.column_checks", "quality_checks.table_checks"} <= publish.upstream_task_ids
    assert dag.get_task("process_region.transform").upstream_task_ids == {"process_region.read_csv"}
    assert [o.name for o in publish.outlets] == ["sales_summary"]
    assert dag.max_active_runs == 1


def test_capstone_report_is_asset_scheduled(dagbag):
    dag = dagbag.get_dag("20_capstone_sales_report")
    assert "sales_summary" in str(dag.timetable.asset_condition)


def test_pool_usage(dagbag):
    dag = dagbag.get_dag("15_pools_priority_concurrency")
    assert dag.get_task("call_api").pool == "api_pool"
    assert dag.get_task("urgent_report").priority_weight == 100
    assert dag.get_task("heavy_query").pool_slots == 2
