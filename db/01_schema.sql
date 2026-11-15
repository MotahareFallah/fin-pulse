-- Ledger schema. Money is always BIGINT in the smallest currency unit, never float.

CREATE TABLE accounts (
    account_id      TEXT PRIMARY KEY,
    currency        TEXT   NOT NULL CHECK (char_length(currency) = 3),
    opening_balance BIGINT NOT NULL DEFAULT 0 CHECK (opening_balance >= 0),
    balance         BIGINT NOT NULL DEFAULT 0 CHECK (balance >= 0),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The primary key is the idempotency guard: an event_id can be applied only once.
-- Rejected events are recorded too, so a replay gets the same outcome.
CREATE TABLE processed_events (
    event_id      TEXT PRIMARY KEY,
    event_type    TEXT NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('applied', 'rejected')),
    reject_reason TEXT,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Append-only. amount is signed: positive credits, negative debits.
-- A transfer writes two rows with the same event_id, one per account.
CREATE TABLE ledger_entries (
    entry_id   BIGSERIAL PRIMARY KEY,
    event_id   TEXT   NOT NULL REFERENCES processed_events (event_id),
    account_id TEXT   NOT NULL REFERENCES accounts (account_id),
    amount     BIGINT NOT NULL CHECK (amount <> 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (event_id, account_id)
);

CREATE INDEX ledger_entries_account_idx ON ledger_entries (account_id, entry_id);

-- Reconciliation: every balance must equal opening_balance plus the sum of its ledger
-- entries. `SELECT * FROM account_reconciliation WHERE balance <> expected` must be empty.
CREATE VIEW account_reconciliation AS
SELECT a.account_id,
       a.balance,
       a.opening_balance + COALESCE(SUM(l.amount), 0) AS expected
FROM accounts a
LEFT JOIN ledger_entries l USING (account_id)
GROUP BY a.account_id, a.balance, a.opening_balance;
