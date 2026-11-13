"""Integration tests against the REAL Postgres from docker compose (`make up`).
They skip automatically if Postgres is not reachable."""

import asyncio
import uuid

import asyncpg
import pytest
import pytest_asyncio

from fin_pulse.config import settings
from fin_pulse.models import parse_event
from fin_pulse.processing import process_event


@pytest_asyncio.fixture
async def pool():
    try:
        p = await asyncpg.create_pool(
            settings.postgres_dsn, min_size=1, max_size=10, timeout=3
        )
    except (TimeoutError, OSError, asyncpg.PostgresError):
        pytest.skip("postgres is not reachable (run `make up`)")
    yield p
    await p.close()


def evt(kind, event_id=None, **payload):
    return parse_event(
        {
            "event_id": event_id or f"evt_{uuid.uuid4().hex}",
            "type": f"{kind}.requested",
            "timestamp": "2026-11-12T10:00:00Z",
            "payload": {"currency": "IRR", **payload},
        }
    )


async def new_account(pool, balance):
    acc = f"t_{uuid.uuid4().hex[:12]}"
    await pool.execute(
        "INSERT INTO accounts (account_id, currency, opening_balance, balance) "
        "VALUES ($1, 'IRR', $2, $2)",
        acc,
        balance,
    )
    return acc


async def balance(pool, acc):
    return await pool.fetchval(
        "SELECT balance FROM accounts WHERE account_id = $1", acc
    )


async def ledger_rows(pool, event_id):
    return await pool.fetchval(
        "SELECT count(*) FROM ledger_entries WHERE event_id = $1", event_id
    )


async def test_deposit_is_applied(pool):
    acc = await new_account(pool, 1000)
    e = evt("deposit", account_id=acc, amount=500)
    assert (await process_event(pool, e)).status == "applied"
    assert await balance(pool, acc) == 1500
    assert await ledger_rows(pool, e.event_id) == 1


async def test_withdrawal_with_insufficient_funds_is_rejected_and_recorded(pool):
    acc = await new_account(pool, 100)
    e = evt("withdrawal", account_id=acc, amount=500)
    out = await process_event(pool, e)
    assert (out.status, out.reason) == ("rejected", "insufficient_funds")
    assert await balance(pool, acc) == 100
    assert await ledger_rows(pool, e.event_id) == 0
    status = await pool.fetchval(
        "SELECT status FROM processed_events WHERE event_id = $1", e.event_id
    )
    assert status == "rejected"


async def test_transfer_moves_money_with_two_ledger_entries(pool):
    a, b = await new_account(pool, 1000), await new_account(pool, 0)
    e = evt("transfer", from_account=a, to_account=b, amount=300)
    assert (await process_event(pool, e)).status == "applied"
    assert (await balance(pool, a), await balance(pool, b)) == (700, 300)
    assert await ledger_rows(pool, e.event_id) == 2


async def test_unknown_account_is_rejected_and_nothing_changes(pool):
    a = await new_account(pool, 1000)
    e = evt("transfer", from_account=a, to_account="t_does_not_exist", amount=10)
    out = await process_event(pool, e)
    assert (out.status, out.reason) == ("rejected", "account_not_found")
    assert await balance(pool, a) == 1000


async def test_replayed_event_is_applied_only_once(pool):
    acc = await new_account(pool, 1000)
    e = evt("deposit", account_id=acc, amount=100)
    assert (await process_event(pool, e)).status == "applied"
    assert (await process_event(pool, e)).status == "duplicate"
    assert await balance(pool, acc) == 1100


async def test_same_event_delivered_concurrently_is_applied_exactly_once(pool):
    acc = await new_account(pool, 1000)
    e = evt("deposit", account_id=acc, amount=100)
    results = await asyncio.gather(*(process_event(pool, e) for _ in range(20)))
    statuses = sorted(r.status for r in results)
    assert statuses.count("applied") == 1 and statuses.count("duplicate") == 19
    assert await balance(pool, acc) == 1100


async def test_concurrent_withdrawals_never_overdraw(pool):
    acc = await new_account(pool, 1000)
    events = [evt("withdrawal", account_id=acc, amount=100) for _ in range(50)]
    results = await asyncio.gather(*(process_event(pool, e) for e in events))
    applied = sum(r.status == "applied" for r in results)
    rejected = sum(r.status == "rejected" for r in results)
    assert (applied, rejected) == (10, 40)
    assert await balance(pool, acc) == 0


async def test_opposite_transfers_do_not_deadlock_and_conserve_money(pool):
    a, b = await new_account(pool, 1_000_000), await new_account(pool, 1_000_000)
    events = []
    for _ in range(100):
        events.append(evt("transfer", from_account=a, to_account=b, amount=10))
        events.append(evt("transfer", from_account=b, to_account=a, amount=10))
    results = await asyncio.wait_for(
        asyncio.gather(*(process_event(pool, e) for e in events)), timeout=30
    )
    assert all(r.status == "applied" for r in results)
    assert await balance(pool, a) + await balance(pool, b) == 2_000_000
    bad = await pool.fetch(
        "SELECT account_id FROM account_reconciliation "
        "WHERE account_id = ANY($1::text[]) AND balance <> expected",
        [a, b],
    )
    assert bad == []
