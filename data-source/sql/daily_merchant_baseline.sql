-- Read-only PostgreSQL baseline for Gold. Replace core_sim if necessary.
-- Pause the simulator and fully reconcile Bronze/Silver before comparing.
-- The current source and the Gold input must describe the same dataset/cutoff.
-- Grain: merchant x occurred_at date in Vietnam. Refunds use their own date.
WITH attributed AS (
    SELECT (t.occurred_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date AS business_date,
           coalesce(a.merchant_id::text, '__unattributed__') AS merchant_id,
           t.kind, t.status, t.amount::numeric AS amount_vnd
    FROM core_sim.transactions t
    LEFT JOIN core_sim.accounts a ON a.id = CASE
        WHEN t.kind = 'PAYMENT' THEN t.destination_account_id
        ELSE t.source_account_id END
)
SELECT business_date, merchant_id,
       count(*) FILTER (WHERE kind = 'PAYMENT' AND status = 'SUCCEEDED') AS payment_count,
       count(*) FILTER (WHERE kind = 'REFUND' AND status = 'SUCCEEDED') AS refund_count,
       count(*) FILTER (WHERE kind = 'PAYMENT' AND status = 'FAILED') AS failed_payment_count,
       count(*) FILTER (WHERE kind = 'REFUND' AND status = 'FAILED') AS failed_refund_count,
       count(*) FILTER (WHERE kind = 'PAYMENT' AND status = 'PENDING') AS pending_payment_count,
       count(*) FILTER (WHERE kind = 'REFUND' AND status = 'PENDING') AS pending_refund_count,
       count(*) AS transaction_count,
       coalesce(sum(amount_vnd) FILTER (WHERE kind = 'PAYMENT' AND status = 'SUCCEEDED'), 0) AS payment_amount_vnd,
       coalesce(sum(amount_vnd) FILTER (WHERE kind = 'REFUND' AND status = 'SUCCEEDED'), 0) AS refund_amount_vnd,
       coalesce(sum(CASE WHEN kind = 'PAYMENT' THEN amount_vnd ELSE -amount_vnd END)
                FILTER (WHERE status = 'SUCCEEDED'), 0) AS net_amount_vnd
FROM attributed
GROUP BY business_date, merchant_id
ORDER BY business_date, merchant_id;
