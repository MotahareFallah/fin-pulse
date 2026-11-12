-- fin-pulse schema (Phase 1)
-- Money is always BIGINT in the smallest currency unit. Never float/numeric.

CREATE TABLE accounts (
    account_id      TEXT PRIMARY KEY,
    currency        TEXT   NOT NULL CHECK (char_length(currency) = 3),
    opening_balance BIGINT NOT NULL DEFAULT 0 CHECK (opening_balance >= 0),
    balance         BIGINT NOT NULL DEFAULT 0 CHECK (balance >= 0),  -- last line of defence
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Source of truth for idempotency. The PRIMARY KEY is the guard: a replayed
-- event_id can never be applied twice, even if Redis lost its keys.
-- Rejected events are recorded too, so a replay gets the same outcome.
CREATE TABLE processed_events (
    event_id      TEXT PRIMARY KEY,
    event_type    TEXT NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('applied', 'rejected')),
    reject_reason TEXT,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Append-only. amount is signed: + credit, - debit.
-- A transfer writes two rows with the same event_id (one per account).
CREATE TABLE ledger_entries (
    entry_id   BIGSERIAL PRIMARY KEY,
    event_id   TEXT   NOT NULL REFERENCES processed_events(event_id),
    account_id TEXT   NOT NULL REFERENCES accounts(account_id),
    amount     BIGINT NOT NULL CHECK (amount <> 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (event_id, account_id)
);
CREATE INDEX ledger_entries_account_idx ON ledger_entries (account_id, entry_id);

-- Reconciliation: balance must equal opening_balance + sum(ledger).
-- `SELECT * FROM account_reconciliation WHERE balance <> expected` must return 0 rows.
CREATE VIEW account_reconciliation AS
SELECT a.account_id,
       a.balance,
       a.opening_balance + COALESCE(SUM(l.amount), 0) AS expected
FROM accounts a
LEFT JOIN ledger_entries l USING (account_id)
GROUP BY a.account_id, a.balance, a.opening_balance;
