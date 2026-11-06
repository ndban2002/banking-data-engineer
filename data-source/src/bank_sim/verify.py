from .db import Database


def verify(db: Database) -> dict:
    # A single consistent snapshot avoids false mismatches while generation runs.
    with db.connect() as conn, conn.transaction():
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        totals = conn.execute(db.query("""
            SELECT count(*) AS accounts, coalesce(sum(opening_balance), 0) AS opening_total,
                coalesce(sum(balance), 0) AS current_total
            FROM {schema}.accounts
        """)).fetchone()
        totals = {key: int(value) for key, value in totals.items()}
        mismatches = conn.execute(db.query("""
            WITH movements AS (
                SELECT source_account_id AS account_id, -amount::numeric AS delta
                FROM {schema}.transactions WHERE status = 'SUCCEEDED'
                UNION ALL
                SELECT destination_account_id, amount::numeric
                FROM {schema}.transactions WHERE status = 'SUCCEEDED'
            ), net AS (SELECT account_id, sum(delta) AS delta FROM movements GROUP BY account_id)
            SELECT a.id, a.balance, a.opening_balance + coalesce(net.delta, 0) AS expected_balance
            FROM {schema}.accounts a LEFT JOIN net ON net.account_id = a.id
            WHERE a.balance < 0 OR a.balance <> a.opening_balance + coalesce(net.delta, 0)
            ORDER BY a.id
        """)).fetchall()
        statuses = conn.execute(db.query("""
            SELECT kind, status, count(*) AS count, sum(amount) AS amount
            FROM {schema}.transactions GROUP BY kind, status ORDER BY kind, status
        """)).fetchall()
        failures = conn.execute(db.query("""
            SELECT error_code, count(*) AS count FROM {schema}.transactions
            WHERE status = 'FAILED' GROUP BY error_code ORDER BY error_code
        """)).fetchall()
        bad_refunds = conn.execute(db.query("""
            SELECT count(*) AS count FROM {schema}.transactions r
            LEFT JOIN {schema}.transactions p ON p.id = r.original_transaction_id
            WHERE r.kind = 'REFUND' AND r.status = 'SUCCEEDED' AND (
                p.id IS NULL OR p.kind <> 'PAYMENT' OR p.status <> 'SUCCEEDED'
                OR r.source_account_id <> p.destination_account_id
                OR r.destination_account_id <> p.source_account_id OR r.amount <> p.amount
                OR r.channel <> p.channel OR r.occurred_at < p.occurred_at)
        """)).fetchone()["count"]
        duplicate_refunds = conn.execute(db.query("""
            SELECT count(*) AS count FROM (
                SELECT original_transaction_id FROM {schema}.transactions
                WHERE kind = 'REFUND' AND status = 'SUCCEEDED'
                GROUP BY original_transaction_id HAVING count(*) > 1
            ) duplicates
        """)).fetchone()["count"]
        pending = sum(row["count"] for row in statuses if row["status"] == "PENDING")
        return {
            "ok": (totals["opening_total"] == totals["current_total"] and not mismatches
                   and not bad_refunds and not duplicate_refunds and not pending),
            **totals,
            "pending": pending,
            "account_mismatch_count": len(mismatches),
            "account_mismatches": [
                {"id": str(row["id"]), "balance": row["balance"], "expected_balance": int(row["expected_balance"])}
                for row in mismatches[:100]
            ],
            "invalid_refunds": bad_refunds,
            "duplicate_refunds": duplicate_refunds,
            "transactions": [dict(row, amount=int(row["amount"])) for row in statuses],
            "failures": failures,
        }
