# Shortcuts. Run `make help` to list them.
.DEFAULT_GOAL := help
SHELL := /bin/bash
CONSTRAINTS := https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.12.txt

# ---- Cloud (SkyPilot + DigitalOcean) settings, see docs/34-deploy-digitalocean-skypilot.md ----
# Put machine-specific overrides (e.g. SKY := /path/to/sky) in .cloud.mk (git-ignored).
-include .cloud.mk
SKY ?= sky
CLUSTER ?= airflow-do
CLOUD_TTL ?= 45# minutes until the VM deletes itself
TUNNEL_PORT ?= 8080
CLOUD_YAML := deploy/skypilot/airflow-do.yaml

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

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

cloud-check: ## Cloud: check SkyPilot can use your DigitalOcean credentials
	$(SKY) check do

cloud-up: ## Cloud: launch the VM; it deletes itself after CLOUD_TTL minutes (default 45)
	$(SKY) launch -c $(CLUSTER) $(CLOUD_YAML) -y -i $(CLOUD_TTL) --down --wait-for none

cloud-tunnel: ## Cloud: SSH tunnel so the UI is at http://localhost:8080 (keep it running)
	@echo "Airflow UI: http://localhost:$(TUNNEL_PORT)   (user: airflow, password: make cloud-password)"
	ssh -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -N -L $(TUNNEL_PORT):localhost:8080 $(CLUSTER)

cloud-password: ## Cloud: print the admin password generated on the VM
	@ssh -o BatchMode=yes -o ConnectTimeout=10 -o LogLevel=ERROR $(CLUSTER) "grep '^_AIRFLOW_WWW_USER_PASSWORD=' ~/.airflow-secrets.env | cut -d= -f2"

cloud-expiry: ## Cloud: show when the VM will be deleted
	@./scripts/cloud_expiry.sh $(CLUSTER)

cloud-extend: ## Cloud: restart the countdown: CLOUD_TTL minutes from NOW
	$(SKY) autostop $(CLUSTER) -i $(CLOUD_TTL) --down --wait-for none -y

cloud-sync: ## Cloud: push local DAG/plugin changes to the VM
	$(SKY) exec $(CLUSTER) --workdir . 'echo "synced to ~/sky_workdir"'

cloud-status: ## Cloud: list clusters and their auto-teardown setting
	$(SKY) status

cloud-down: ## Cloud: delete the VM now (stops billing)
	$(SKY) down $(CLUSTER) -y

.PHONY: help env init up down reset ps logs cli psql venv test test-all lint standalone cloud-check cloud-up cloud-tunnel cloud-password cloud-expiry cloud-extend cloud-sync cloud-status cloud-down
