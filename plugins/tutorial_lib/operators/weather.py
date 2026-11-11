"""
A custom **Operator** that uses a custom **Hook**, plus an **operator extra link**.

Anatomy of an operator:

* subclass ``BaseOperator``
* ``template_fields``  – attributes rendered with Jinja (``{{ ds }}`` etc.) before execute()
* ``__init__``         – store arguments only. NO network/database calls here: __init__
                         runs every time the DAG file is parsed (every ~30s)!
* ``execute(context)`` – the real work, runs on a worker. The return value is pushed to XCom.
"""

from __future__ import annotations

from collections.abc import Sequence

from airflow.sdk import BaseOperator, BaseOperatorLink

from tutorial_lib.hooks.open_meteo import OpenMeteoHook


class OpenMeteoDocsLink(BaseOperatorLink):
    """Adds an "Open-Meteo Docs" button on the task in the UI (registered in tutorial_plugin.py)."""

    name = "Open-Meteo Docs"

    def get_link(self, operator, *, ti_key) -> str:
        return "https://open-meteo.com/en/docs"


class FetchWeatherOperator(BaseOperator):
    template_fields: Sequence[str] = ("city",)
    ui_color = "#d6eaff"  # colour of the task box in the Graph view
    operator_extra_links = (OpenMeteoDocsLink(),)

    def __init__(
        self,
        *,
        city: str,
        latitude: float,
        longitude: float,
        open_meteo_conn_id: str = OpenMeteoHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.city = city
        self.latitude = latitude
        self.longitude = longitude
        self.open_meteo_conn_id = open_meteo_conn_id

    def execute(self, context) -> dict:
        hook = OpenMeteoHook(open_meteo_conn_id=self.open_meteo_conn_id)
        weather = hook.get_current_weather(self.latitude, self.longitude)
        self.log.info("Weather in %s: %s", self.city, weather)
        return {"city": self.city, **weather}
