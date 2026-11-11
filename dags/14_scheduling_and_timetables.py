"""
# 14 · Scheduling — cron, presets, timedelta, timetables, catchup & backfill

**Concepts:** `schedule` values, `start_date`/`end_date`, `catchup`, data intervals vs
trigger-style schedules, `CronTriggerTimetable`, `CronDataIntervalTimetable`,
`DeltaTriggerTimetable`, `MultipleCronTriggerTimetable`,
`@once`, `@continuous`, `max_active_runs`, timezones.

Each DAG here has a single task that prints its timing information, so you can compare
`logical_date`, `data_interval_start/end` and `run_after` across schedule types.

| DAG | Schedule |
|-----|----------|
| `14_schedule_cron_preset`        | `"@daily"` (preset) |
| `14_schedule_cron_expression`    | `"30 6 * * 1-5"` — 06:30 Mon–Fri |
| `14_schedule_timedelta`          | every 4 hours (`timedelta`) |
| `14_schedule_trigger_timetable`  | `CronTriggerTimetable` in Asia/Kathmandu time |
| `14_schedule_catchup_backfill`   | data-interval timetable with `catchup=True` (bounded by end_date) |
| `14_schedule_multiple_crons`     | 08:00 **and** 17:30 on weekdays |
| `14_schedule_delta_trigger`      | `DeltaTriggerTimetable` — every 30 minutes, trigger-style |
| `14_schedule_once`               | `"@once"` — a single run |
| `14_schedule_continuous`         | `"@continuous"` — start the next run as soon as one finishes |

⚠️ `14_schedule_continuous` runs forever while unpaused — pause it after watching.

📖 docs/18-scheduling-and-timetables.md
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import (
    DAG,
    CronDataIntervalTimetable,
    CronTriggerTimetable,
    DeltaTriggerTimetable,
    MultipleCronTriggerTimetable,
    task,
)

START = pendulum.datetime(2026, 1, 1, tz="UTC")


@task
def show_timing(dag_run=None, logical_date=None, data_interval_start=None, data_interval_end=None):
    print(f"run_id              : {dag_run.run_id}")
    print(f"run_type            : {dag_run.run_type}   (scheduled / manual / backfill / asset_triggered)")
    print(f"run_after           : {dag_run.run_after}  (earliest time the run may start)")
    print(f"logical_date        : {logical_date}")
    print(f"data_interval_start : {data_interval_start}")
    print(f"data_interval_end   : {data_interval_end}")


SCHEDULES = {
    # dag_id: (schedule, extra DAG kwargs)
    "14_schedule_cron_preset": ("@daily", {}),
    "14_schedule_cron_expression": ("30 6 * * 1-5", {}),
    "14_schedule_timedelta": (timedelta(hours=4), {}),
    "14_schedule_trigger_timetable": (
        # Trigger timetables: run AT the cron time; no "data interval" (start == end).
        CronTriggerTimetable("0 9 * * *", timezone="Asia/Kathmandu"),
        {},
    ),
    "14_schedule_catchup_backfill": (
        # Data-interval timetables: the run for 2026-11-01 covers [11-01, 11-02) and
        # starts AFTER that interval ends. catchup=True creates a run for every missed
        # interval between start_date and now (bounded here by end_date -> 6 runs).
        CronDataIntervalTimetable("@daily", timezone="UTC"),
        {
            "start_date": pendulum.datetime(2026, 11, 1, tz="UTC"),
            "end_date": pendulum.datetime(2026, 11, 7, tz="UTC"),
            "catchup": True,
            "max_active_runs": 2,  # process at most 2 days in parallel
        },
    ),
    "14_schedule_multiple_crons": (
        MultipleCronTriggerTimetable("0 8 * * 1-5", "30 17 * * 1-5", timezone="UTC"),
        {},
    ),
    # NOTE: EventsTimetable (run on a fixed list of dates) is skipped here because of a
    # serialization bug in Airflow 3.4.0 (KeyError: '_summary'). See docs/18-scheduling-and-timetables.md.
    "14_schedule_delta_trigger": (DeltaTriggerTimetable(timedelta(minutes=30)), {}),
    "14_schedule_once": ("@once", {}),
    "14_schedule_continuous": ("@continuous", {"max_active_runs": 1}),  # max_active_runs=1 is required
}

for dag_id, (schedule, extra_kwargs) in SCHEDULES.items():
    kwargs = {"start_date": START, "catchup": False, **extra_kwargs}
    with DAG(
        dag_id=dag_id,
        schedule=schedule,
        tags=["tutorial", "11-scheduling"],
        doc_md=__doc__,
        **kwargs,
    ):
        show_timing()
