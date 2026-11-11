"""
Unit tests for the capstone's business logic.

`clean_rows` is a plain function defined at module level in the DAG file, so it can be
tested without Airflow running. DAG file names start with a digit, so we import the
module with importlib instead of a normal `import` statement.
"""

from __future__ import annotations

import csv
import importlib

import pytest

from tutorial_lib.paths import SAMPLE_DATA_DIR

capstone = importlib.import_module("20_capstone_sales_etl")


def row(order_id="1", quantity="2", unit_price="10.50", region=" North ", customer_id="C001"):
    return {
        "order_id": order_id,
        "order_date": "2026-11-01",
        "region": region,
        "product": " Mouse ",
        "quantity": quantity,
        "unit_price": unit_price,
        "customer_id": customer_id,
    }


def test_revenue_and_cleanup():
    result = capstone.clean_rows([row()])
    cleaned = result["rows"][0]
    assert cleaned["revenue"] == "21.00"
    assert cleaned["region"] == "north"  # trimmed + lower-cased
    assert cleaned["product"] == "Mouse"
    assert result["region"] == "north"
    assert result["rejected"] == []


@pytest.mark.parametrize("bad_quantity", ["0", "-2"])
def test_rejects_non_positive_quantity(bad_quantity):
    result = capstone.clean_rows([row(order_id="7", quantity=bad_quantity), row(order_id="8")])
    assert result["rejected"] == ["7"]
    assert [r["order_id"] for r in result["rows"]] == [8]


def test_empty_customer_becomes_none():
    assert capstone.clean_rows([row(customer_id="")])["rows"][0]["customer_id"] is None


@pytest.mark.parametrize(("region", "expected_rejected"), [("north", 0), ("south", 1), ("west", 1)])
def test_sample_files(region, expected_rejected):
    with (SAMPLE_DATA_DIR / f"sales_{region}.csv").open(newline="") as f:
        result = capstone.clean_rows(list(csv.DictReader(f)))
    assert len(result["rejected"]) == expected_rejected
    assert len(result["rows"]) == 12 - expected_rejected
