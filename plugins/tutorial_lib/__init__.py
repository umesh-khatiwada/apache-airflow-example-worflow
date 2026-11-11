"""
Shared code for the tutorial DAGs.

Why does this live in `plugins/`?
    Airflow adds three folders to Python's import path on every component:
    `dags/`, `plugins/` and `config/`. Anything placed here can therefore be
    imported from any DAG file with a normal import, e.g.::

        from tutorial_lib.operators.csv_quality import CsvQualityCheckOperator

Keep DAG files thin (just the workflow) and put reusable logic here.
"""
