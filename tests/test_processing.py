"""Integration tests against the real PostgreSQL from `make up`.

They skip when the database is unreachable. Every row they create uses the `tst_`
prefix and is deleted afterwards, so the seeded accounts stay untouched.
"""

import asyncio
import uuid

import asyncpg
import pytest
import pytest_asyncio

from fin_pulse.config import settings
from fin_pulse.models import parse_event
from fin_pulse.processing import Outcome, process_event


async def _cleanup(pool):
    await pool.execute(r"DELETE FROM ledger_entries WHERE event_id LIKE 'tst\_%'")
    await pool.execute(r"DELETE FROM processed_events WHERE event_id LIKE 'tst\_%'")
    await pool.execute(r"DELETE FROM accounts WHERE account_id LIKE 'tst\_%'")


@pytest_asyncio.fixture
async def pool():
    try:
        p = await asyncpg.create_pool(
            settings.postgres_dsn, min_size=1, max_size=10, timeout=3
        )
    except (OSError, asyncpg.PostgresError, TimeoutError):
        pytest.skip("postgres is not reachable (run `make up`)")
    yield p
    await _cleanup(p)
    await p.close()


def evt(kind, event_id=None, **payload):
    return parse_event(
        {
            "event_id": event_id or f"tst_{uuid.uuid4().hex}",
            "type": f"{kind}.requested",
            "timestamp": "2026-11-12T10:00:00Z",
            "payload": {"currency": "IRR", **payload},
        }
    )


async def new_account(pool, balance):
    account_id = f"tst_{uuid.uuid4().hex[:12]}"
    await pool.execute(
        "INSERT INTO accounts (account_id, currency, opening_balance, balance) "
        "VALUES ($1, 'IRR', $2, $2)",
        account_id,
        balance,
    )
    return account_id


async def balance(pool, account_id):
    return await pool.fetchval(
        "SELECT balance FROM accounts WHERE account_id = $1", account_id
    )


async def ledger_rows(pool, event_id):
    return await pool.fetchval(
        "SELECT count(*) FROM ledger_entries WHERE event_id = $1", event_id
    )


async def test_deposit_is_applied(pool):
    account = await new_account(pool, 1000)
    event = evt("deposit", account_id=account, amount=500)
    assert await process_event(pool, event) == Outcome("applied")
    assert await balance(pool, account) == 1500
    assert await ledger_rows(pool, event.event_id) == 1


async def test_withdrawal_over_balance_is_rejected_and_recorded(pool):
    account = await new_account(pool, 100)
    event = evt("withdrawal", account_id=account, amount=500)
    assert await process_event(pool, event) == Outcome("rejected", "insufficient_funds")
    assert await balance(pool, account) == 100
    assert await ledger_rows(pool, event.event_id) == 0
    status = await pool.fetchval(
        "SELECT status FROM processed_events WHERE event_id = $1", event.event_id
    )
    assert status == "rejected"


async def test_transfer_moves_money_with_two_ledger_entries(pool):
    source, target = await new_account(pool, 1000), await new_account(pool, 0)
    event = evt("transfer", from_account=source, to_account=target, amount=300)
    assert await process_event(pool, event) == Outcome("applied")
    assert (await balance(pool, source), await balance(pool, target)) == (700, 300)
    assert await ledger_rows(pool, event.event_id) == 2


async def test_unknown_account_is_rejected_and_nothing_changes(pool):
    source = await new_account(pool, 1000)
    event = evt("transfer", from_account=source, to_account="tst_missing", amount=10)
    assert await process_event(pool, event) == Outcome("rejected", "account_not_found")
    assert await balance(pool, source) == 1000


async def test_replayed_event_is_applied_only_once(pool):
    account = await new_account(pool, 1000)
    event = evt("deposit", account_id=account, amount=100)
    assert await process_event(pool, event) == Outcome("applied")
    assert await process_event(pool, event) == Outcome("duplicate")
    assert await balance(pool, account) == 1100


async def test_same_event_delivered_concurrently_is_applied_exactly_once(pool):
    account = await new_account(pool, 1000)
    event = evt("deposit", account_id=account, amount=100)
    results = await asyncio.gather(*(process_event(pool, event) for _ in range(20)))
    statuses = [r.status for r in results]
    assert statuses.count("applied") == 1
    assert statuses.count("duplicate") == 19
    assert await balance(pool, account) == 1100


async def test_concurrent_withdrawals_never_overdraw(pool):
    account = await new_account(pool, 1000)
    events = [evt("withdrawal", account_id=account, amount=100) for _ in range(50)]
    results = await asyncio.gather(*(process_event(pool, e) for e in events))
    statuses = [r.status for r in results]
    assert statuses.count("applied") == 10
    assert statuses.count("rejected") == 40
    assert await balance(pool, account) == 0


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
    mismatched = await pool.fetch(
        "SELECT account_id FROM account_reconciliation "
        "WHERE account_id = ANY($1::text[]) AND balance <> expected",
        [a, b],
    )
    assert mismatched == []
