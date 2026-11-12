.PHONY: up down reset psql install test lint reconcile

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
