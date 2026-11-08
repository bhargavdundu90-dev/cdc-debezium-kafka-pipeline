# Common local tasks. Spark targets need Java 11 or 17 on PATH.
PY ?= python

.PHONY: help install sample test unit integration demo glue-zip tf-fmt tf-validate up down clean

help:
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

install: ## install Python dependencies
	$(PY) -m pip install -r requirements.txt

sample: ## regenerate data/sample/customers_cdc.jsonl (seeded)
	$(PY) -m generator.cdc_events --snapshot 20 --changes 100 --duplicate-rate 0.05 --late-rate 0.05 \
		--out data/sample/customers_cdc.jsonl

test: ## all tests
	$(PY) -m pytest -q

unit: ## unit tests only
	$(PY) -m pytest -q -m "not integration"

integration: ## generator -> bronze -> SCD2 -> DQ -> promote
	$(PY) -m pytest -q -m integration

demo: ## two simulated days through the daily run, then show one customer's history
	./scripts/demo.sh

glue-zip: ## dist/cdc_pipeline.zip for the Glue job
	./scripts/package_glue.sh

tf-fmt: ## terraform fmt check
	terraform -chdir=terraform fmt -check -recursive

tf-validate: ## terraform init (no backend) + validate
	terraform -chdir=terraform init -backend=false -input=false
	terraform -chdir=terraform validate

up: ## start Postgres + Kafka + Debezium and register the connector
	docker compose -f docker/docker-compose.yml up -d
	./docker/connect/register-connector.sh

down: ## stop the local stack and drop its volumes
	docker compose -f docker/docker-compose.yml down -v

clean: ## remove local outputs
	rm -rf data/output dist spark-warehouse metastore_db derby.log .pytest_cache
