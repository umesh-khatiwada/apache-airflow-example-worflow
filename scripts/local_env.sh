#!/usr/bin/env bash
# Source this file to point the `airflow` CLI at this project WITHOUT Docker:
#     source scripts/local_env.sh
# It is used by scripts/run_local.sh and by `make test`.

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")/.." && pwd)"
export PROJECT_DIR

export AIRFLOW_HOME="${PROJECT_DIR}/.airflow_local"           # sqlite DB + generated config live here
export AIRFLOW__CORE__DAGS_FOLDER="${PROJECT_DIR}/dags"
export AIRFLOW__CORE__PLUGINS_FOLDER="${PROJECT_DIR}/plugins"
export AIRFLOW__CORE__LOAD_EXAMPLES="false"
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION="true"
export AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL="30"
export AIRFLOW__CORE__TEST_CONNECTION="Enabled"
# Local login: every user is admin, no password needed (learning only!)
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS="true"
# Folder that the file/sensor examples use (in Docker it is /opt/airflow/data)
export TUTORIAL_DATA_DIR="${PROJECT_DIR}/data"

# Same tutorial connections/variables as docker-compose.yaml.
# demo_postgres points at the Postgres container exposed on localhost:5432.
export AIRFLOW_CONN_DEMO_POSTGRES='{"conn_type": "postgres", "host": "localhost", "port": 5432, "login": "airflow", "password": "airflow", "schema": "demo"}'
export AIRFLOW_CONN_FS_DEFAULT='{"conn_type": "fs", "extra": {"path": "/"}}'
export AIRFLOW_CONN_OPEN_METEO_API='{"conn_type": "http", "host": "https://api.open-meteo.com"}'
export AIRFLOW_VAR_ENVIRONMENT="local"
export AIRFLOW_VAR_TUTORIAL_SETTINGS='{"team": "data-platform", "row_limit": 100, "notify": false}'

if [ -d "${PROJECT_DIR}/.venv" ] && [ -z "${VIRTUAL_ENV:-}" ]; then
  # shellcheck disable=SC1091
  source "${PROJECT_DIR}/.venv/bin/activate"
fi
