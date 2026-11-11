"""Where the tutorial DAGs read and write files."""

from __future__ import annotations

import os
from pathlib import Path

# In Docker this is /opt/airflow/data (mounted from ./data in the project).
# When running locally (scripts/local_env.sh) it is <project>/data.
DATA_DIR = Path(os.environ.get("TUTORIAL_DATA_DIR", "/opt/airflow/data"))

# Folder with SQL files and sample CSVs that ship with the project.
INCLUDE_DIR = Path(__file__).resolve().parents[2] / "dags" / "include"
if not INCLUDE_DIR.exists():  # inside Docker the layout is /opt/airflow/{dags,plugins}
    INCLUDE_DIR = Path("/opt/airflow/dags/include")

SQL_DIR = INCLUDE_DIR / "sql"
SAMPLE_DATA_DIR = INCLUDE_DIR / "data"
