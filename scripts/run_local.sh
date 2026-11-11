#!/usr/bin/env bash
# Run ALL Airflow components in one process with SQLite — no Docker needed.
# Good for quick experiments. DAGs that use the `demo_postgres` connection also
# need the Postgres container:  docker compose up -d postgres
set -euo pipefail
source "$(dirname "$0")/local_env.sh"
mkdir -p "${AIRFLOW_HOME}" "${TUTORIAL_DATA_DIR}"
airflow db migrate
airflow pools import "${PROJECT_DIR}/config/pools.json" || true
echo "Starting Airflow standalone at http://localhost:8080 (Ctrl+C to stop)"
exec airflow standalone
