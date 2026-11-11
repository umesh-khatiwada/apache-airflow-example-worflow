"""
Unit tests for the custom operator, sensor, hook and plugin macros in plugins/.

Operators are normal Python classes: create one and call .execute(context) directly.
"""

from __future__ import annotations

from unittest import mock

import pytest
import requests
from airflow.sdk.exceptions import AirflowFailException

from tutorial_lib.hooks.open_meteo import OpenMeteoHook
from tutorial_lib.operators.csv_quality import CsvQualityCheckOperator
from tutorial_lib.paths import SAMPLE_DATA_DIR
from tutorial_lib.sensors.csv_rows import CsvHasRowsSensor

COLUMNS = ["order_id", "order_date", "region", "product", "quantity", "unit_price", "customer_id"]


# ---- CsvQualityCheckOperator ---------------------------------------------------------
def test_quality_check_passes_on_sample_file():
    op = CsvQualityCheckOperator(
        task_id="qc",
        file_path=str(SAMPLE_DATA_DIR / "sales_north.csv"),
        required_columns=COLUMNS,
        min_rows=5,
        not_null_columns=["order_id"],
    )
    result = op.execute(context={})
    assert result["rows"] == 12
    assert result["problems"] == []


def test_quality_check_fails_on_missing_column(tmp_path):
    csv_file = tmp_path / "bad.csv"
    csv_file.write_text("order_id,amount\n1,10\n")
    op = CsvQualityCheckOperator(task_id="qc", file_path=str(csv_file), required_columns=["order_id", "region"])
    with pytest.raises(AirflowFailException, match="missing columns"):
        op.execute(context={})


def test_quality_check_fails_on_empty_values(tmp_path):
    csv_file = tmp_path / "nulls.csv"
    csv_file.write_text("order_id,product\n1,\n2,Mouse\n")
    op = CsvQualityCheckOperator(task_id="qc", file_path=str(csv_file), not_null_columns=["product"])
    with pytest.raises(AirflowFailException, match="1 empty values"):
        op.execute(context={})


def test_quality_check_fails_when_file_missing(tmp_path):
    op = CsvQualityCheckOperator(task_id="qc", file_path=str(tmp_path / "nope.csv"))
    with pytest.raises(AirflowFailException, match="File not found"):
        op.execute(context={})


# ---- CsvHasRowsSensor -----------------------------------------------------------------
def test_sensor_waits_then_succeeds(tmp_path):
    csv_file = tmp_path / "incoming.csv"
    sensor = CsvHasRowsSensor(task_id="wait", file_path=str(csv_file), min_rows=2)

    assert sensor.poke(context={}).is_done is False  # file doesn't exist yet

    csv_file.write_text("id\n1\n")
    assert sensor.poke(context={}).is_done is False  # only 1 row

    csv_file.write_text("id\n1\n2\n")
    result = sensor.poke(context={})
    assert result.is_done is True
    assert result.xcom_value == 2


# ---- OpenMeteoHook ---------------------------------------------------------------------
@pytest.fixture
def open_meteo_connection(monkeypatch):
    # Connections can be supplied as env vars — perfect for tests.
    monkeypatch.setenv("AIRFLOW_CONN_OPEN_METEO_API", '{"conn_type": "http", "host": "https://example.test"}')


def test_hook_parses_api_response(open_meteo_connection):
    fake_response = mock.Mock()
    fake_response.json.return_value = {"current_weather": {"temperature": 21.5, "windspeed": 7.0}}
    fake_response.raise_for_status.return_value = None
    with mock.patch("tutorial_lib.hooks.open_meteo.requests.get", return_value=fake_response) as get:
        result = OpenMeteoHook().get_current_weather(1.0, 2.0)
    assert result == {"temperature": 21.5, "windspeed": 7.0, "source": "api"}
    assert get.call_args.args[0] == "https://example.test/v1/forecast"


def test_hook_falls_back_when_offline(open_meteo_connection):
    with mock.patch(
        "tutorial_lib.hooks.open_meteo.requests.get", side_effect=requests.ConnectionError("offline")
    ):
        first = OpenMeteoHook().get_current_weather(27.7, 85.3)
        second = OpenMeteoHook().get_current_weather(27.7, 85.3)
    assert first["source"] == "fallback"
    assert first == second  # deterministic fallback


def test_hook_raises_without_fallback(open_meteo_connection):
    with mock.patch(
        "tutorial_lib.hooks.open_meteo.requests.get", side_effect=requests.ConnectionError("offline")
    ):
        with pytest.raises(requests.ConnectionError):
            OpenMeteoHook().get_current_weather(1, 2, offline_fallback=False)


# ---- Plugin macros ----------------------------------------------------------------------
def test_plugin_macros():
    import tutorial_plugin

    assert tutorial_plugin.to_upper("abc") == "ABC"
    assert tutorial_plugin.env_banner("dev") == "*** running in DEV ***"
    assert tutorial_plugin.TutorialPlugin.name == "tutorial_plugin"
