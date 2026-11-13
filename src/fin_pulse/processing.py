"""Apply one event to Postgres in ONE transaction.

Order inside the transaction matters:
1. Claim event_id in processed_events (idempotency guard, ADR 0001).
   If someone else already claimed it, we do nothing.
2. Lock the affected accounts in a fixed order (sorted by account_id), so two
   opposite transfers (A->B and B->A) can never deadlock.
3. Validate, then write ledger entries and update balances.

If anything raises, the whole transaction (including the claim) rolls back,
so the event can be safely retried.
"""

from dataclasses import dataclass

import asyncpg

from fin_pulse.models import DepositEvent, TransferEvent, WithdrawalEvent


@dataclass(frozen=True)
class Outcome:
    status: str  # "applied" | "rejected" | "duplicate"
    reason: str | None = None


async def _claim(conn: asyncpg.Connection, event) -> bool:
    row = await conn.fetchval(
        "INSERT INTO processed_events (event_id, event_type, status) "
        "VALUES ($1, $2, 'applied') "
        "ON CONFLICT (event_id) DO NOTHING RETURNING event_id",
        event.event_id,
        event.type,
    )
    return row is not None


async def _reject(conn: asyncpg.Connection, event_id: str, reason: str) -> Outcome:
    # Recorded, so a replay of this event gets the same answer.
    await conn.execute(
        "UPDATE processed_events SET status = 'rejected', reject_reason = $2 "
        "WHERE event_id = $1",
        event_id,
        reason,
    )
    return Outcome("rejected", reason)


async def _lock_accounts(conn: asyncpg.Connection, account_ids: list[str]) -> dict:
    rows = await conn.fetch(
        "SELECT account_id, currency, balance FROM accounts "
        "WHERE account_id = ANY($1::text[]) ORDER BY account_id FOR UPDATE",
        account_ids,
    )
    return {r["account_id"]: r for r in rows}


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


async def process_event(pool: asyncpg.Pool, event) -> Outcome:
    p = event.payload
    ids = (
        [p.from_account, p.to_account]
        if isinstance(event, TransferEvent)
        else [p.account_id]
    )

    async with pool.acquire() as conn:
        async with conn.transaction():
            if not await _claim(conn, event):
                return Outcome("duplicate")

            accounts = await _lock_accounts(conn, ids)
            if any(i not in accounts for i in ids):
                return await _reject(conn, event.event_id, "account_not_found")
            if any(a["currency"] != p.currency for a in accounts.values()):
                return await _reject(conn, event.event_id, "currency_mismatch")

            if isinstance(event, DepositEvent):
                await _post(conn, event.event_id, p.account_id, p.amount)
            elif isinstance(event, WithdrawalEvent):
                if accounts[p.account_id]["balance"] < p.amount:
                    return await _reject(conn, event.event_id, "insufficient_funds")
                await _post(conn, event.event_id, p.account_id, -p.amount)
            else:  # TransferEvent
                if accounts[p.from_account]["balance"] < p.amount:
                    return await _reject(conn, event.event_id, "insufficient_funds")
                await _post(conn, event.event_id, p.from_account, -p.amount)
                await _post(conn, event.event_id, p.to_account, p.amount)

            return Outcome("applied")
