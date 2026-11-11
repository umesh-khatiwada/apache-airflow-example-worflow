"""
# 13 · Using custom operators, hooks, sensors & plugins

**Concepts:** importing your own components from `plugins/tutorial_lib/`,
templated fields on custom operators, operator extra links, plugin macros,
mapping a custom operator.

Source code of the components (read these!):

| Component | File |
|-----------|------|
| `OpenMeteoHook`           | plugins/tutorial_lib/hooks/open_meteo.py |
| `FetchWeatherOperator`    | plugins/tutorial_lib/operators/weather.py |
| `CsvQualityCheckOperator` | plugins/tutorial_lib/operators/csv_quality.py |
| `CsvHasRowsSensor`        | plugins/tutorial_lib/sensors/csv_rows.py |
| `LogNotifier`             | plugins/tutorial_lib/notifiers.py |
| `TutorialPlugin` (macros, links) | plugins/tutorial_plugin.py |

Open a `fetch_weather_*` task → you'll see an **Open-Meteo Docs** link button.

📖 docs/17-custom-operators-hooks-plugins.md
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

from tutorial_lib.notifiers import LogNotifier
from tutorial_lib.operators.csv_quality import CsvQualityCheckOperator
from tutorial_lib.operators.weather import FetchWeatherOperator
from tutorial_lib.paths import SAMPLE_DATA_DIR
from tutorial_lib.sensors.csv_rows import CsvHasRowsSensor

REGIONS = ["north", "south", "west"]


@dag(
    dag_id="13_custom_operators_hooks_sensors",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    tags=["tutorial", "10-extending"],
    doc_md=__doc__,
)
def custom_components():
    # Custom operator using a custom hook. `city` is a template field.
    kathmandu = FetchWeatherOperator(
        task_id="fetch_weather_kathmandu",
        city="Kathmandu ({{ ds }})",
        latitude=27.71,
        longitude=85.32,
    )
    oslo = FetchWeatherOperator(task_id="fetch_weather_oslo", city="Oslo", latitude=59.91, longitude=10.75)

    # Custom sensor: the sample file already exists, so this succeeds on the first poke.
    wait_for_sample = CsvHasRowsSensor(
        task_id="wait_for_sample_file",
        file_path=str(SAMPLE_DATA_DIR / "sales_north.csv"),
        min_rows=5,
        poke_interval=5,
        timeout=60,
    )

    # Custom operator, mapped over regions (custom operators support .partial/.expand too).
    quality = CsvQualityCheckOperator.partial(
        task_id="csv_quality_check",
        required_columns=["order_id", "order_date", "region", "product", "quantity", "unit_price"],
        not_null_columns=["order_id", "product"],
        min_rows=5,
        on_failure_callback=LogNotifier("Quality check failed for {{ ti.map_index }}"),
    ).expand(file_path=[str(SAMPLE_DATA_DIR / f"sales_{region}.csv") for region in REGIONS])

    @task
    def compare(weather_a: dict, weather_b: dict, quality_results: list[dict]) -> None:
        warmer = max([weather_a, weather_b], key=lambda w: w["temperature"])
        print(f"Warmer city right now: {warmer['city']} ({warmer['temperature']} °C)")
        for result in quality_results:
            print(f"{result['file']}: {result['rows']} rows OK")

    # Plugin macros in a template (registered by plugins/tutorial_plugin.py)
    use_plugin_macro = BashOperator(
        task_id="use_plugin_macro",
        bash_command="echo '{{ macros.tutorial_plugin.to_upper(\"custom plugins are loaded\") }}'",
    )

    wait_for_sample >> quality
    # `.output` turns a classic operator into its XCom result (a list for mapped operators).
    compare(kathmandu.output, oslo.output, quality.output) >> use_plugin_macro


custom_components()
