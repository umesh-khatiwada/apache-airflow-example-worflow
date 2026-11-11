"""
A custom **Operator** that validates a CSV file.

Pure Python, no external services, so it is easy to unit-test
(see tests/test_custom_components.py).
"""

from __future__ import annotations

import csv
from collections.abc import Sequence
from pathlib import Path

from airflow.sdk import BaseOperator
from airflow.sdk.exceptions import AirflowFailException


class CsvQualityCheckOperator(BaseOperator):
    """
    Check that a CSV file exists, has the required columns and enough rows.

    :param file_path: path to the CSV file (templated, so ``{{ ds }}`` works)
    :param required_columns: column names that must be present in the header
    :param min_rows: minimum number of data rows (header excluded)
    :param not_null_columns: columns that may not contain empty values
    """

    template_fields: Sequence[str] = ("file_path",)
    ui_color = "#e8f7e4"

    def __init__(
        self,
        *,
        file_path: str,
        required_columns: Sequence[str] = (),
        min_rows: int = 1,
        not_null_columns: Sequence[str] = (),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.file_path = file_path
        self.required_columns = list(required_columns)
        self.min_rows = min_rows
        self.not_null_columns = list(not_null_columns)

    def execute(self, context) -> dict:
        path = Path(self.file_path)
        if not path.exists():
            # AirflowFailException = fail immediately, do NOT retry (retrying won't create the file).
            raise AirflowFailException(f"File not found: {path}")

        with path.open(newline="") as f:
            reader = csv.DictReader(f)
            header = reader.fieldnames or []
            rows = list(reader)

        problems: list[str] = []
        missing = [c for c in self.required_columns if c not in header]
        if missing:
            problems.append(f"missing columns {missing}")
        if len(rows) < self.min_rows:
            problems.append(f"only {len(rows)} rows, expected at least {self.min_rows}")
        for column in self.not_null_columns:
            empty = sum(1 for r in rows if not (r.get(column) or "").strip())
            if empty:
                problems.append(f"{empty} empty values in column '{column}'")

        summary = {"file": str(path), "rows": len(rows), "columns": header, "problems": problems}
        if problems:
            raise AirflowFailException(f"Quality check failed for {path.name}: {'; '.join(problems)}")
        self.log.info("Quality check passed: %s", summary)
        return summary
