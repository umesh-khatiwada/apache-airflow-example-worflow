"""
A custom **Sensor**.

A sensor is an operator that *waits* for something. Airflow calls ``poke()``
every ``poke_interval`` seconds until it returns True (or the ``timeout`` is hit).

Returning ``PokeReturnValue(is_done=True, xcom_value=...)`` lets the sensor also
hand a value to downstream tasks.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from airflow.sdk import BaseSensorOperator, PokeReturnValue


class CsvHasRowsSensor(BaseSensorOperator):
    """Wait until a CSV file exists AND contains at least ``min_rows`` data rows."""

    template_fields: Sequence[str] = ("file_path",)

    def __init__(self, *, file_path: str, min_rows: int = 1, **kwargs) -> None:
        super().__init__(**kwargs)
        self.file_path = file_path
        self.min_rows = min_rows

    def poke(self, context) -> PokeReturnValue:
        path = Path(self.file_path)
        if not path.exists():
            self.log.info("Waiting for %s to appear...", path)
            return PokeReturnValue(is_done=False)
        with path.open() as f:
            data_rows = max(sum(1 for _ in f) - 1, 0)  # minus header line
        self.log.info("%s has %s data rows (need %s)", path, data_rows, self.min_rows)
        return PokeReturnValue(is_done=data_rows >= self.min_rows, xcom_value=data_rows)
