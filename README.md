# fin-pulse

**Distributed Financial Event Processing Engine**

Accepts deposit / withdrawal / transfer events, queues them in Kafka, processes them in
parallel, and guarantees each event takes effect exactly once. Educational simulation:
no real money, no compliance claims.

Flow: client -> FastAPI -> Kafka -> workers -> PostgreSQL (ledger)

## Status
Phase 1 (core): in progress.
Done: schema, event contracts, dev stack. Next: ingestion API, worker.

## Run the dev stack
Requires [uv](https://docs.astral.sh/uv/) and Docker.

```bash
cp .env.example .env
make up            # Kafka (KRaft), Postgres (schema + 1000 seeded accounts), Redis
uv sync            # creates .venv and installs everything from pyproject.toml
make test          # runs: uv run pytest
make reconcile     # expect 0 rows
```

## Targets (not yet measured)
Throughput and latency numbers will be added from reproducible benchmarks only.
