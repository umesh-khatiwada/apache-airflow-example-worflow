"""
A custom **Hook**.

A hook is a Python class that knows *how to talk to one external system*.
It reads credentials from an Airflow **Connection** (so no secrets in code) and
exposes friendly methods. Operators and @task functions then use the hook.

This hook talks to the free Open-Meteo weather API (no API key needed).
The connection `open_meteo_api` is defined in docker-compose.yaml as::

    AIRFLOW_CONN_OPEN_METEO_API='{"conn_type": "http", "host": "https://api.open-meteo.com"}'

If there is no internet access, ``get_current_weather(..., offline_fallback=True)``
returns fake but realistic data so the tutorial still runs.
"""

from __future__ import annotations

import random
from typing import Any

import requests
from airflow.sdk import BaseHook


class OpenMeteoHook(BaseHook):
    # These four class attributes tell Airflow how this hook relates to connections.
    conn_name_attr = "open_meteo_conn_id"
    default_conn_name = "open_meteo_api"
    conn_type = "http"
    hook_name = "Open-Meteo"

    def __init__(self, open_meteo_conn_id: str = default_conn_name, timeout: int = 10) -> None:
        super().__init__()
        self.open_meteo_conn_id = open_meteo_conn_id
        self.timeout = timeout

    def get_base_url(self) -> str:
        conn = self.get_connection(self.open_meteo_conn_id)
        host = conn.host or "https://api.open-meteo.com"
        if "://" not in host:
            host = f"{conn.schema or 'https'}://{host}"
        return host.rstrip("/")

    def get_current_weather(
        self, latitude: float, longitude: float, offline_fallback: bool = True
    ) -> dict[str, Any]:
        """Return {"temperature": °C, "windspeed": km/h, "source": "api"|"fallback"}."""
        url = f"{self.get_base_url()}/v1/forecast"
        params = {"latitude": latitude, "longitude": longitude, "current_weather": "true"}
        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            current = response.json()["current_weather"]
            return {
                "temperature": current["temperature"],
                "windspeed": current["windspeed"],
                "source": "api",
            }
        except (requests.RequestException, KeyError) as err:
            if not offline_fallback:
                raise
            self.log.warning("Open-Meteo not reachable (%s). Using fallback data.", err)
            rng = random.Random(f"{latitude},{longitude}")  # same city -> same numbers
            return {
                "temperature": round(rng.uniform(-5, 35), 1),
                "windspeed": round(rng.uniform(0, 40), 1),
                "source": "fallback",
            }
