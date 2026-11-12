# ADR 0001: At-least-once delivery + idempotency enforced in Postgres

Status: accepted

## Context
Kafka consumers can see the same event twice (crash after DB commit but before
offset commit, rebalance, producer retry). In a financial system a duplicate
means money applied twice.

## Decision
- Delivery is at-least-once. Offsets are committed only after the DB transaction commits.
- Idempotency is enforced by `processed_events.event_id PRIMARY KEY`, inserted in the
  SAME transaction as the ledger entries and balance updates.
- Redis is only a fast path to reject obvious duplicates early. It is not trusted:
  losing Redis must not cause double application.

## Consequences
- Every event costs one extra insert. Accepted, because correctness outranks throughput here.
- Rejected events (e.g. insufficient funds) are also recorded, so a replay is a no-op with the same outcome.
- We do not claim exactly-once delivery; we get exactly-once *effect*.
