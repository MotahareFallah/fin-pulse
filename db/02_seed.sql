-- 1000 test accounts, acc_00001 to acc_01000, each opening with 1,000,000.
INSERT INTO accounts (account_id, currency, opening_balance, balance)
SELECT 'acc_' || lpad(i::text, 5, '0'), 'IRR', 1000000, 1000000
FROM generate_series(1, 1000) AS i;
