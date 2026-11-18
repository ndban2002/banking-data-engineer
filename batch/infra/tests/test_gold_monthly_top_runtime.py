"""Exercise monthly top transactions in Spark with a synthetic Silver fixture."""
from datetime import date, datetime, timezone
import os
from pathlib import Path
import sys
import unittest


for path in (Path(__file__).resolve().parents[1] / "jobs", Path("/opt/spark/work-dir/jobs")):
    if path.is_dir():
        sys.path.insert(0, str(path))


RUN_RUNTIME = __name__ == "__main__" or os.environ.get("RUN_GOLD_MONTHLY_TOP_RUNTIME") == "1"
JANUARY = date(2026, 1, 1)
FEBRUARY = date(2026, 2, 1)


def transaction(tx_id, amount, occurred_at, *, status="SUCCEEDED", kind="PAYMENT"):
    accounts = ("merchant-b-account", "customer-account") if kind == "REFUND" else (
        "customer-account", "merchant-a-account"
    )
    return tx_id, kind, status, amount, occurred_at, *accounts


@unittest.skipUnless(RUN_RUNTIME, "requires Spark runtime; execute with spark-submit")
class GoldMonthlyTopRuntimeTests(unittest.TestCase):
    def test_month_boundary_strict_limit_and_deterministic_ties(self):
        from pyspark.sql import SparkSession

        from gold_daily_merchant import merchant_events
        from gold_monthly_top_transactions import build_mart

        spark = (SparkSession.builder.master("local[2]").appName("gold-monthly-top-runtime-test")
                 .config("spark.sql.session.timeZone", "UTC")
                 .config("spark.sql.shuffle.partitions", "2")
                 .getOrCreate())
        spark.sparkContext.setLogLevel("WARN")
        self.addCleanup(spark.stop)

        # Midnight February 1 in Vietnam falls at 17:00 UTC on January 31.
        january_last_second = datetime(2026, 1, 31, 16, 59, 59, tzinfo=timezone.utc)
        february_first_second = datetime(2026, 1, 31, 17, 0, 0, tzinfo=timezone.utc)
        early_tie = datetime(2026, 1, 31, 16, 59, 57, tzinfo=timezone.utc)
        rows = [
            transaction("february-payment", 700, february_first_second),
            transaction("tie-0-later", 100, january_last_second),
            transaction("huge-pending", 5000, january_last_second, status="PENDING"),
            transaction("tie-b", 100, early_tie),
            transaction("huge-failed", 6000, january_last_second, status="FAILED"),
            transaction("tie-a", 100, early_tie),
        ]
        rows.extend(transaction(f"january-{amount}", amount, january_last_second,
                                kind="REFUND" if amount == 900 else "PAYMENT")
                    for amount in range(200, 1100, 100))
        transactions = spark.createDataFrame(
            rows, "transaction_id STRING, transaction_type STRING, status STRING, "
                  "amount_vnd LONG, occurred_at TIMESTAMP, source_account_id STRING, "
                  "destination_account_id STRING",
        )
        accounts = spark.createDataFrame(
            [("customer-account", None), ("merchant-a-account", "merchant-a"),
             ("merchant-b-account", "merchant-b")],
            "account_id STRING, merchant_id STRING",
        )
        events = merchant_events(spark, {"transactions": transactions, "accounts": accounts})
        top = build_mart(events).collect()

        january = sorted((row for row in top if row.business_month == JANUARY),
                         key=lambda row: row.transaction_rank)
        self.assertEqual([row.transaction_rank for row in january], list(range(1, 11)))
        self.assertEqual([row.transaction_id for row in january],
                         [f"january-{amount}" for amount in range(1000, 100, -100)] + ["tie-a"])
        refund = january[1]
        self.assertEqual((refund.transaction_type, refund.merchant_id, refund.business_date),
                         ("REFUND", "merchant-b", date(2026, 1, 31)))
        self.assertEqual((january[0].merchant_id, january[0].amount_vnd), ("merchant-a", 1000))

        february = [row for row in top if row.business_month == FEBRUARY]
        self.assertEqual([(row.transaction_rank, row.transaction_id, row.business_date) for row in february],
                         [(1, "february-payment", FEBRUARY)])
        self.assertEqual(len(top), 11)


if __name__ == "__main__":
    unittest.main(verbosity=2)
