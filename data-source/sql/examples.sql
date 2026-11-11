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

-- Payment activity by local hour. Include failures, since they are also attempts
-- to pay; idempotent retries do not create additional transaction rows.
SELECT extract(hour FROM occurred_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::int AS local_hour,
       count(*) AS payments,
       count(*) FILTER (WHERE status = 'SUCCEEDED') AS successful_payments
FROM core_sim.transactions
WHERE kind = 'PAYMENT'
GROUP BY local_hour
ORDER BY local_hour;

-- Ticket sizes by MCC. Successful payments avoid the deliberate oversized
-- insufficient-funds scenario distorting the retail amount distribution.
SELECT m.mcc, count(*) AS payments,
       round(avg(t.amount)) AS average_vnd,
       percentile_disc(0.5) WITHIN GROUP (ORDER BY t.amount) AS median_vnd,
       percentile_disc(0.9) WITHIN GROUP (ORDER BY t.amount) AS p90_vnd,
       min(t.amount) AS minimum_vnd, max(t.amount) AS maximum_vnd
FROM core_sim.transactions t
JOIN core_sim.accounts a ON a.id = t.destination_account_id
JOIN core_sim.merchants m ON m.id = a.merchant_id
WHERE t.kind = 'PAYMENT' AND t.status = 'SUCCEEDED'
GROUP BY m.mcc
ORDER BY m.mcc;

-- Local shopping by customer region. The result need not equal the configured
-- local preference: the global merchant pool can also select a local merchant.
SELECT c.region AS customer_region, count(*) AS payments,
       count(*) FILTER (WHERE c.region = m.region) AS same_region_payments,
       round(100.0 * count(*) FILTER (WHERE c.region = m.region)
             / nullif(count(*), 0), 2) AS same_region_percent
FROM core_sim.transactions t
JOIN core_sim.accounts ca ON ca.id = t.source_account_id
JOIN core_sim.customers c ON c.id = ca.customer_id
JOIN core_sim.accounts ma ON ma.id = t.destination_account_id
JOIN core_sim.merchants m ON m.id = ma.merchant_id
WHERE t.kind = 'PAYMENT'
GROUP BY c.region
ORDER BY c.region;

-- Refund lag uses business time. Historic rows and direct service calls can use
-- other lags; generator age settings govern newly generated refund candidates.
SELECT count(*) AS refunds,
       min(r.occurred_at - p.occurred_at) AS minimum_lag,
       percentile_disc(0.5) WITHIN GROUP (ORDER BY r.occurred_at - p.occurred_at) AS median_lag,
       max(r.occurred_at - p.occurred_at) AS maximum_lag
FROM core_sim.transactions r
JOIN core_sim.transactions p ON p.id = r.original_transaction_id
WHERE r.kind = 'REFUND' AND r.status = 'SUCCEEDED';
