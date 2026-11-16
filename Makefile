.DEFAULT_GOAL := help
.PHONY: help install test lint fmt hooks up down reset psql topics reconcile api

help: ## Show available commands
	@grep -E '^[a-z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

install: ## Install dependencies into .venv
	uv sync

test: ## Run the test suite
	uv run pytest -q

lint: ## Check style and common bugs
	uv run ruff check .
	uv run ruff format --check .

fmt: ## Auto-fix lint issues and format code
	uv run ruff check --fix .
	uv run ruff format .

hooks: ## Install git hooks (pre-commit, commit-msg, pre-push)
	uv run pre-commit install

KAFKA_TOPICS = docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092

up: ## Start Kafka, Postgres and Redis
	docker compose up -d --wait

down: ## Stop the stack (data is kept)
	docker compose down

reset: ## Stop the stack and delete all data
	docker compose down -v

psql: ## Open a psql shell
	docker compose exec postgres psql -U finpulse -d finpulse

topics: ## Create the Kafka topics
	$(KAFKA_TOPICS) --create --if-not-exists --topic fin.events --partitions 6 --replication-factor 1
	$(KAFKA_TOPICS) --create --if-not-exists --topic fin.events.retry --partitions 6 --replication-factor 1
	$(KAFKA_TOPICS) --create --if-not-exists --topic fin.events.dlq --partitions 3 --replication-factor 1

reconcile: ## Ledger check, must print 0 rows
	docker compose exec postgres psql -U finpulse -d finpulse -c "SELECT * FROM account_reconciliation WHERE balance <> expected;"

api: ## Run the API on port 8010
	uv run uvicorn fin_pulse.api.main:app --port 8010
