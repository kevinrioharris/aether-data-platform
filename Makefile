PY := .venv/bin/python

.PHONY: help install up down clean ps logs cdc-register cdc-status generate consume consume-docker silver reconcile airflow-trigger use-case-1 report psql test lint

help:
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-16s %s\n", $$1, $$2}'

.env:
	cp .env.example .env

install: .env ## Create the Python venv and install the platform + dev tools
	python3 -m venv .venv
	$(PY) -m pip install -U pip
	$(PY) -m pip install -e ".[dev]"

up: .env ## Start source DB, Kafka, Debezium, Kafka UI, object storage, Airflow
	docker compose up -d --build

down: ## Stop everything (keeps data)
	docker compose --profile pipeline down

clean: ## Stop everything and delete all data volumes
	docker compose --profile pipeline down -v

ps: ## Show service status
	docker compose ps

logs: ## Tail logs (e.g. make logs s=kafka-connect)
	docker compose logs -f $(s)

cdc-register: ## Register/update the Debezium Postgres connector
	$(PY) scripts/register_connector.py

cdc-status: ## Show the Debezium connector status
	$(PY) scripts/register_connector.py status

generate: ## Simulate live shop traffic (inserts/updates/deletes)
	$(PY) scripts/generate_activity.py --events-per-sec 2

consume: ## Run the CDC consumer locally (Kafka → bronze Delta)
	$(PY) -m dataplatform.bronze.cdc_consumer

consume-docker: ## Run the CDC consumer as a container
	docker compose --profile pipeline up -d --build cdc-consumer

silver: ## Build silver locally (CDC MERGE + SCD2 + quality checks)
	$(PY) -m dataplatform.silver.run

reconcile: ## Compare silver with the source DB (stop the generator first)
	$(PY) -m dataplatform.silver.run --reconcile

airflow-trigger: ## Trigger the build_silver DAG now
	docker compose exec airflow airflow dags trigger build_silver

use-case-1: ## Run use case 1 end to end: Excel targets + FX rates → silver → gold → report
	$(PY) scripts/make_sample_excel.py
	$(PY) -m dataplatform.cli upload sample_data/sales_targets_2026.xlsx excel/sales_targets/
	$(PY) -m dataplatform.cli ingest-files sales_targets
	$(PY) -m dataplatform.cli ingest-api fx_rates
	$(PY) -m dataplatform.cli build-reference sales_targets
	$(PY) -m dataplatform.cli build-reference fx_rates
	$(PY) -m dataplatform.silver.run
	$(PY) -m dataplatform.cli build-gold
	$(PY) -m dataplatform.gold.report

report: ## Print the sales-vs-target report from gold
	$(PY) -m dataplatform.gold.report

psql: ## Open psql on the source shop DB
	docker compose exec postgres-source psql -U shop -d shop

test: ## Run unit tests
	$(PY) -m pytest -q

lint: ## Lint with ruff
	$(PY) -m ruff check .
