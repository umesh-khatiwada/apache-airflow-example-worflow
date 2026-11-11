"""
# 18 · Human-in-the-loop — pause and ask a person (Airflow 3.1+)

**Concepts:** `HITLEntryOperator` (ask for input), `ApprovalOperator` (approve/reject),
`HITLBranchOperator` (a human picks the branch), `HITLOperator` (pick options),
`response_timeout` + `defaults`, Notifiers with a link to the response page.

```
prepare_report ─► reviewer_notes (form) ─► approve_publish (Approve/Reject) ─► choose_channel ─┬─► publish_to_email
                                                                                                ├─► publish_to_slack
                                                                                                └─► publish_to_website
```

**Try it:** trigger the DAG, then open **Browse ▸ Required Actions** (or click the task
that is waiting — it shows as *awaiting input*) and respond in the UI.

* If nobody answers `approve_publish` within 30 minutes, it uses the default **Reject**,
  which skips everything downstream.

📖 docs/22-human-in-the-loop.md
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.hitl import (
    ApprovalOperator,
    HITLBranchOperator,
    HITLEntryOperator,
    HITLOperator,
)
from airflow.sdk import Param, dag, task

from tutorial_lib.notifiers import LogNotifier

# Logs who must answer and what was asked (swap for SlackNotifier/SmtpNotifier in real life).
ask_notifier = LogNotifier("Human input needed: '{{ task.subject }}' (task {{ ti.task_id }})", level="info")


@dag(
    dag_id="18_human_in_the_loop",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    tags=["tutorial", "14-advanced"],
    doc_md=__doc__,
)
def human_in_the_loop():
    @task
    def prepare_report() -> dict:
        return {"revenue": 12_345.67, "orders": 321, "period": "2026-W45"}

    report = prepare_report()

    reviewer_notes = HITLEntryOperator(
        task_id="reviewer_notes",
        subject="Review the weekly report",
        body="Revenue: {{ ti.xcom_pull(task_ids='prepare_report')['revenue'] }}. Add any notes for readers.",
        params={"notes": Param("Looks good", type="string")},
        notifiers=[ask_notifier],
    )

    approve_publish = ApprovalOperator(
        task_id="approve_publish",
        subject="Publish the weekly report?",
        body="Reviewer notes: {{ ti.xcom_pull(task_ids='reviewer_notes')['params_input']['notes'] }}",
        defaults="Reject",  # used if nobody answers before the timeout
        response_timeout=timedelta(minutes=30),
        notifiers=[ask_notifier],
    )

    choose_channel = HITLBranchOperator(
        task_id="choose_channel",
        subject="Where should the report go?",
        options=["publish_to_email", "publish_to_slack", "publish_to_website"],  # = downstream task ids
        multiple=True,  # allow picking several
        notifiers=[ask_notifier],
    )

    @task
    def publish_to_email():
        print("Sent by e-mail")

    @task
    def publish_to_slack():
        print("Posted to Slack")

    @task
    def publish_to_website():
        print("Published on the website")

    report >> reviewer_notes >> approve_publish >> choose_channel
    choose_channel >> [publish_to_email(), publish_to_slack(), publish_to_website()]

    # Generic HITLOperator: choose an option; the answer is available as XCom.
    rate_this_tutorial = HITLOperator(
        task_id="rate_this_tutorial",
        subject="How useful was this example?",
        options=["⭐", "⭐⭐", "⭐⭐⭐"],
        defaults=["⭐⭐⭐"],
        response_timeout=timedelta(hours=1),
    )

    @task
    def thank_you(answer: dict):
        print(f"You chose {answer['chosen_options']} — thanks!")

    thank_you(rate_this_tutorial.output)


human_in_the_loop()
