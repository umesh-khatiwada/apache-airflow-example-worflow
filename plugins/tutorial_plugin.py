"""
An **Airflow Plugin**.

Most custom code (operators, hooks, sensors) does NOT need a plugin — a normal
import is enough (see tutorial_lib/). A plugin is needed when Airflow itself must
know about something, for example:

* ``macros``                       – extra functions usable in Jinja: {{ macros.tutorial_plugin.<fn>() }}
* ``global_operator_extra_links`` / ``operator_extra_links`` – buttons on tasks in the UI
* ``listeners``                    – react to events (task started/succeeded/failed, ...)
* ``fastapi_apps`` / ``react_apps`` / ``external_views`` – extend the UI / API
* ``timetables``                   – custom schedules

Check it is loaded:  `airflow plugins`  (or Admin -> Plugins in the UI).
"""

from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin

from tutorial_lib.operators.weather import OpenMeteoDocsLink


def to_upper(value: str) -> str:
    """Example macro: {{ macros.tutorial_plugin.to_upper('hello') }} -> HELLO"""
    return str(value).upper()


def env_banner(environment: str) -> str:
    """Example macro: {{ macros.tutorial_plugin.env_banner(var.value.environment) }}"""
    return f"*** running in {environment.upper()} ***"


class TutorialPlugin(AirflowPlugin):
    name = "tutorial_plugin"
    macros = [to_upper, env_banner]
    operator_extra_links = [OpenMeteoDocsLink()]
