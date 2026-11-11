"""
A custom *Notifier*.

Notifiers are reusable, templated callbacks. Providers ship real ones
(e.g. ``SlackNotifier``, ``SmtpNotifier``); this one just writes to the task log so
it works without any external service.

Usage::

    @task(on_failure_callback=LogNotifier("Task {{ ti.task_id }} failed in {{ dag.dag_id }}"))
"""

from __future__ import annotations

from airflow.sdk import BaseNotifier


class LogNotifier(BaseNotifier):
    # Fields listed here are rendered with Jinja before notify() is called.
    template_fields = ("message",)

    def __init__(self, message: str, level: str = "warning") -> None:
        super().__init__()
        self.message = message
        self.level = level

    def notify(self, context) -> None:
        getattr(self.log, self.level)("[LogNotifier] %s", self.message)
