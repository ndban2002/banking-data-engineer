-- Read-only examples. Replace core_sim if using a different simulator schema.

-- Recent writes: compare business time with database write time.
SELECT id, kind, status, amount, channel, occurred_at, created_at, updated_at, error_code
FROM core_sim.transactions
ORDER BY created_at DESC, id DESC LIMIT 20;

-- Status and failure distribution (unique transactions, not request attempts).
SELECT kind, status, error_code, count(*) AS transactions, sum(amount) AS amount_vnd
FROM core_sim.transactions
GROUP BY kind, status, error_code
ORDER BY kind, status, error_code;

-- Merchant net revenue per business minute in Vietnam.
WITH cashflows AS (
    SELECT CASE WHEN kind = 'PAYMENT' THEN destination_account_id ELSE source_account_id END AS account_id,
           occurred_at,
           CASE WHEN kind = 'PAYMENT' THEN amount::numeric ELSE -amount::numeric END AS net_amount
    FROM core_sim.transactions WHERE status = 'SUCCEEDED'
)
SELECT m.id, m.name, m.mcc, m.region,
       date_trunc('minute', f.occurred_at AT TIME ZONE 'Asia/Ho_Chi_Minh') AS local_minute,
       sum(f.net_amount) AS net_revenue_vnd
FROM cashflows f
JOIN core_sim.accounts a ON a.id = f.account_id
JOIN core_sim.merchants m ON m.id = a.merchant_id
GROUP BY m.id, m.name, m.mcc, m.region, local_minute
ORDER BY local_minute DESC, m.id LIMIT 50;

-- Per-account reconciliation: should return zero rows after a healthy workload.
WITH movements AS (
    SELECT source_account_id AS account_id, -amount::numeric AS delta
    FROM core_sim.transactions WHERE status = 'SUCCEEDED'
    UNION ALL
    SELECT destination_account_id, amount::numeric
    FROM core_sim.transactions WHERE status = 'SUCCEEDED'
), net AS (
    SELECT account_id, sum(delta) AS delta FROM movements GROUP BY account_id
)
SELECT a.id, a.balance, a.opening_balance + coalesce(net.delta, 0) AS expected_balance
FROM core_sim.accounts a LEFT JOIN net ON net.account_id = a.id
WHERE a.balance <> a.opening_balance + coalesce(net.delta, 0);

-- System-wide conservation of money.
SELECT sum(opening_balance) AS opening_total, sum(balance) AS current_total,
       sum(balance) - sum(opening_balance) AS difference
FROM core_sim.accounts;
