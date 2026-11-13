.PHONY: up down reset psql topics api worker install test lint fmt hooks reconcile

up:
	docker compose up -d

down:
	docker compose down

# wipes volumes: Postgres re-runs db/*.sql on next `up`
reset:
	docker compose down -v

psql:
	docker compose exec postgres psql -U finpulse -d finpulse

# must return 0 rows if the ledger and balances agree
reconcile:
	docker compose exec postgres psql -U finpulse -d finpulse \
	  -c "SELECT * FROM account_reconciliation WHERE balance <> expected;"

install:
	uv sync

test:
	uv run pytest -q

lint:
	uv run ruff check .

# explicit topics: 6 partitions = max consumer parallelism per group
topics:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic fin.events --partitions 6 --replication-factor 1
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic fin.events.retry --partitions 6 --replication-factor 1
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic fin.events.dlq --partitions 3 --replication-factor 1

# ports 8000/8001 are taken by other projects on this machine
api:
	uv run uvicorn fin_pulse.api.main:app --port 8010

# format + autofix lint issues
fmt:
	uv run ruff check --fix .
	uv run ruff format .

# install git hooks (pre-commit, commit-msg, pre-push)
hooks:
	uv run pre-commit install

# consumer group "fin-pulse-workers"; run several in separate terminals to scale
worker:
	uv run python -m fin_pulse.worker.main
