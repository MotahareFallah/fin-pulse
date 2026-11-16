"""Apply one event to PostgreSQL in a single transaction.

Order inside the transaction matters:
1. Claim the event_id in processed_events. If another transaction already claimed it,
   the event is a duplicate and nothing else happens (ADR 0001).
2. Lock the affected accounts in a fixed order (sorted by account_id), so opposite
   transfers (A to B and B to A) cannot deadlock.
3. Validate, then write the ledger entries and update the balances.

Any exception rolls back the whole transaction, including the claim, so the event can
be retried safely.
"""

from dataclasses import dataclass

import asyncpg

from fin_pulse.models import AnyEvent, DepositEvent, WithdrawalEvent


@dataclass(frozen=True)
class Outcome:
    status: str  # "applied", "rejected" or "duplicate"
    reason: str | None = None


def _legs(event: AnyEvent) -> list[tuple[str, int]]:
    """The signed ledger movements this event makes, as (account_id, delta)."""
    p = event.payload
    if isinstance(event, DepositEvent):
        return [(p.account_id, p.amount)]
    if isinstance(event, WithdrawalEvent):
        return [(p.account_id, -p.amount)]
    return [(p.from_account, -p.amount), (p.to_account, p.amount)]


async def _claim(conn: asyncpg.Connection, event: AnyEvent) -> bool:
    claimed = await conn.fetchval(
        "INSERT INTO processed_events (event_id, event_type, status) "
        "VALUES ($1, $2, 'applied') "
        "ON CONFLICT (event_id) DO NOTHING RETURNING event_id",
        event.event_id,
        event.type,
    )
    return claimed is not None


async def _reject(conn: asyncpg.Connection, event_id: str, reason: str) -> Outcome:
    # Recorded, so a replay of a rejected event gets the same answer.
    await conn.execute(
        "UPDATE processed_events SET status = 'rejected', reject_reason = $2 "
        "WHERE event_id = $1",
        event_id,
        reason,
    )
    return Outcome("rejected", reason)


async def _lock_accounts(
    conn: asyncpg.Connection, account_ids: list[str]
) -> dict[str, asyncpg.Record]:
    rows = await conn.fetch(
        "SELECT account_id, currency, balance FROM accounts "
        "WHERE account_id = ANY($1::text[]) ORDER BY account_id FOR UPDATE",
        account_ids,
    )
    return {row["account_id"]: row for row in rows}


async def _post(
    conn: asyncpg.Connection, event_id: str, account_id: str, delta: int
) -> None:
    await conn.execute(
        "UPDATE accounts SET balance = balance + $2 WHERE account_id = $1",
        account_id,
        delta,
    )
    await conn.execute(
        "INSERT INTO ledger_entries (event_id, account_id, amount) VALUES ($1, $2, $3)",
        event_id,
        account_id,
        delta,
    )


async def process_event(pool: asyncpg.Pool, event: AnyEvent) -> Outcome:
    legs = _legs(event)

    async with pool.acquire() as conn, conn.transaction():
        if not await _claim(conn, event):
            return Outcome("duplicate")

        accounts = await _lock_accounts(conn, [account for account, _ in legs])
        if any(account not in accounts for account, _ in legs):
            return await _reject(conn, event.event_id, "account_not_found")
        if any(row["currency"] != event.payload.currency for row in accounts.values()):
            return await _reject(conn, event.event_id, "currency_mismatch")
        if any(accounts[account]["balance"] + delta < 0 for account, delta in legs):
            return await _reject(conn, event.event_id, "insufficient_funds")

        for account, delta in legs:
            await _post(conn, event.event_id, account, delta)
        return Outcome("applied")
