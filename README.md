# fin-pulse

Distributed event processing engine for financial transactions.

Accepts deposit, withdrawal and transfer events over HTTP, queues them in Kafka,
applies them in parallel workers, and guarantees each event takes effect exactly
once under duplicates, crashes and load.

## Architecture

    client -> FastAPI -> Kafka -> workers -> PostgreSQL (ledger)

Redis speeds up duplicate detection and rate limiting. Idempotency is enforced in
PostgreSQL, never in Redis.

## Stack

Python, FastAPI, Kafka, PostgreSQL, Redis, Prometheus, Grafana, k6, Docker Compose

## Status

Under construction.
