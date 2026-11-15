# ADR 0001: Idempotency is enforced in PostgreSQL

Status: accepted

## Context

Kafka delivers at least once. A consumer can see the same event twice after a crash
between the database commit and the offset commit, after a rebalance, or after a
producer retry. For a ledger, a duplicate means money applied twice.

## Decision

- Delivery is at-least-once. Offsets are committed only after the database
  transaction commits.
- `processed_events.event_id` is a primary key, inserted in the same transaction as
  the ledger entries and balance updates. A replayed event fails the claim and is a
  no-op.
- Redis only rejects obvious duplicates early. It is never trusted: losing Redis must
  not cause an event to be applied twice.

## Consequences

- Every event costs one extra insert. Correctness is worth more than that throughput.
- Rejected events (for example insufficient funds) are recorded as well, so a replay
  returns the same outcome.
- The system provides exactly-once effect, not exactly-once delivery.
