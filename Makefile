# Shortcuts. Run `make help` to list them.
.DEFAULT_GOAL := help
SHELL := /bin/bash
CONSTRAINTS := https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.12.txt

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

env: ## Create .env from .env.example (sets AIRFLOW_UID to your user id)
	@test -f .env || (sed "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$$(id -u)/" .env.example > .env && echo ".env created")

init: env ## One-time setup: migrate the metadata DB, create admin user + pools
	docker compose up airflow-init

up: env ## Start Airflow in the background (UI: http://localhost:8080, airflow/airflow)
	docker compose up -d

down: ## Stop all containers (keeps the database)
	docker compose down

reset: ## Stop and DELETE everything, including the database volume
	docker compose down --volumes --remove-orphans

ps: ## Show container status
	docker compose ps

logs: ## Follow scheduler logs
	docker compose logs -f airflow-scheduler

cli: ## Open a bash shell inside an Airflow container
	docker compose run --rm airflow-cli bash

psql: ## Open psql on the demo database
	docker compose exec postgres psql -U airflow -d demo

venv: ## Create local .venv with Airflow 3.4 (tests / IDE autocomplete)
	uv venv --python 3.12 .venv
	uv pip install --python .venv/bin/python -r requirements-dev.txt --constraint $(CONSTRAINTS)

test: ## Fast tests: DAG integrity, structure, unit tests (needs `make venv`)
	source scripts/local_env.sh && pytest -q tests -m "not integration"

test-all: ## All tests incl. running whole DAGs with dag.test() (creates the local SQLite DB)
	source scripts/local_env.sh && mkdir -p "$$AIRFLOW_HOME" && airflow db migrate && pytest -q tests

lint: ## Lint DAGs/plugins/tests with ruff (incl. Airflow-specific AIR rules)
	source scripts/local_env.sh && ruff check dags plugins tests

standalone: ## Run Airflow without Docker (SQLite, single process)
	./scripts/run_local.sh

.PHONY: help env init up down reset ps logs cli psql venv test test-all lint standalone
