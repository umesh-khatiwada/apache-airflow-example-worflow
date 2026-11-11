"""
Shared pytest fixtures.

Run the tests with:   make test
(which runs `source scripts/local_env.sh && pytest -q tests`)
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

PROJECT_DIR = Path(__file__).resolve().parents[1]
DAGS_DIR = PROJECT_DIR / "dags"

# Sensible defaults if someone runs plain `pytest` without sourcing scripts/local_env.sh.
os.environ.setdefault("AIRFLOW_HOME", str(PROJECT_DIR / ".airflow_local"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", str(DAGS_DIR))
os.environ.setdefault("AIRFLOW__CORE__PLUGINS_FOLDER", str(PROJECT_DIR / "plugins"))
os.environ.setdefault("TUTORIAL_DATA_DIR", str(PROJECT_DIR / "data"))
os.environ.setdefault("AIRFLOW_VAR_ENVIRONMENT", "test")


@pytest.fixture(scope="session")
def dagbag():
    """Parse every DAG file once per test session (same thing the dag-processor does)."""
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAGS_DIR))
