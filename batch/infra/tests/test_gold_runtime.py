"""Real Spark/Iceberg Gold tests, using disposable source namespaces only.

Run with spark-submit after copying this file AND test_silver_runtime.py to
the same container directory. Regular pytest skips the runtime tests.
"""
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
import uuid

for path in (Path(__file__).resolve().parents[1] / "jobs", Path("/opt/spark/work-dir/jobs")):
    if path.is_dir():
        sys.path.insert(0, str(path))

import test_silver_runtime as fixtures

RUN_RUNTIME = __name__ == "__main__" or os.environ.get("RUN_GOLD_RUNTIME") == "1"
UTC = timezone.utc
D15 = datetime(2026, 11, 15, 16, 59, 59, tzinfo=UTC)
D16 = D15 + timedelta(seconds=1)  # Midnight in Vietnam, still Nov 15 UTC.
D17 = D16 + timedelta(days=1)
D18 = D17 + timedelta(days=1)


@unittest.skipUnless(RUN_RUNTIME, "requires Spark/Iceberg; execute with spark-submit")
class GoldRuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        import bronze_loader
        import silver_loader
        import gold_daily_merchant

        cls.gold = gold_daily_merchant
        cls.bronze = bronze_loader
        cls.silver = silver_loader
        cls.spark = (SparkSession.builder.master("local[2]").appName("gold-runtime-tests")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("WARN")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.fixture = fixtures.SilverRuntimeTests(methodName="runTest")
        self.fixture.spark, self.fixture.bronze, self.fixture.loader = self.spark, self.bronze, self.silver
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.gold_ns = "gold_test_" + uuid.uuid4().hex
        self.target = f"lakehouse.{self.gold_ns}.daily_merchant_performance"
        self.addCleanup(self.cleanup_gold)

    def cleanup_gold(self):
        self.assertRegex(self.gold_ns, r"^gold_test_[a-f0-9]{32}$")
        if any(r["namespace"] == self.gold_ns for r in self.spark.sql("SHOW NAMESPACES IN lakehouse").collect()):
            for row in self.spark.sql(f"SHOW TABLES IN lakehouse.{self.gold_ns}").collect():
                self.spark.sql(f"DROP TABLE lakehouse.{self.gold_ns}.{row['tableName']} PURGE")
            self.spark.sql(f"DROP NAMESPACE lakehouse.{self.gold_ns}")

    def load(self, table, rows):
        batch = self.fixture.append_batch(table, rows)
        return self.fixture.process(table, batch)

    def seed(self):
        self.load("customers", [fixtures.customer()])
        self.load("merchants", [fixtures.merchant()])
        self.load("accounts", [fixtures.account(balance=970), fixtures.account("merchant", balance=30)])
        self.payment = fixtures.transaction(occurred_at=D16)
        self.pending = fixtures.transaction("pending", status="PENDING", occurred_at=D18)
        self.load("transactions", [
            fixtures.transaction("boundary", amount=30, occurred_at=D15), self.payment,
            fixtures.transaction("refund", kind="REFUND", occurred_at=D17,
                                 original_transaction_id=fixtures.uid("payment"),
                                 source_account_id=fixtures.uid("merchant-account"),
                                 destination_account_id=fixtures.uid("customer-account")),
            fixtures.transaction("failed-refund", kind="REFUND", status="FAILED", error_code="INVALID_REFUND",
                                 amount=55, occurred_at=D17, original_transaction_id=fixtures.uid("payment"),
                                 source_account_id=fixtures.uid("merchant-account"),
                                 destination_account_id=fixtures.uid("customer-account")), self.pending,
            fixtures.transaction("unattributed", status="FAILED", error_code="INVALID_ACCOUNTS", occurred_at=D17,
                                 source_account_id=fixtures.uid("merchant-account"),
                                 destination_account_id=fixtures.uid("customer-account")),
        ])

    def build(self):
        return self.gold.materialize(self.spark, silver_namespace=self.fixture.silver_ns, gold_namespace=self.gold_ns)

    def rows(self):
        return {(str(r["business_date"]), r["merchant_id"]): r.asDict() for r in self.spark.table(self.target).collect()}

    def test_daily_metrics_replay_late_correction_and_disappearing_day(self):
        self.seed()
        first = self.build()
        merchant = fixtures.uid("merchant")
        rows = self.rows()
        self.assertEqual(len(rows), 5)  # Four merchant days + explicit unknown bucket.
        for day, count, payment, refund, net in [
            ("2026-11-15", 1, 30, 0, 30), ("2026-11-16", 1, 100, 0, 100),
            ("2026-11-17", 2, 0, 100, -100), ("2026-11-18", 1, 0, 0, 0),
        ]:
            row = rows[(day, merchant)]
            self.assertEqual((row["transaction_count"], row["payment_amount_vnd"],
                              row["refund_amount_vnd"], row["net_amount_vnd"]), (count, payment, refund, net))
        self.assertEqual(rows[("2026-11-17", merchant)]["failed_refund_count"], 1)
        self.assertEqual(rows[("2026-11-18", merchant)]["pending_payment_count"], 1)
        unknown = rows[("2026-11-17", self.gold.UNKNOWN_MERCHANT)]
        self.assertEqual(unknown["failed_payment_count"], 1)
        self.assertEqual(first["reconciliation"]["input_transactions"], 6)
        second = self.build()
        self.assertEqual(first["input_id"], second["input_id"])
        self.assertEqual(rows, self.rows())
        self.assertEqual(self.spark.table(self.target).schema["payment_amount_vnd"].dataType.simpleString(), "decimal(38,0)")

        # A late change moves the sole event away from D18; that group must vanish.
        changed = {**self.pending, "status": "FAILED", "error_code": "SIMULATED_DECLINE", "occurred_at": D16,
                   "updated_at": fixtures.BASE + timedelta(hours=1)}
        self.load("transactions", [changed])
        third = self.build()
        self.assertNotEqual(third["input_id"], first["input_id"])
        rows = self.rows()
        self.assertNotIn(("2026-11-18", merchant), rows)
        self.assertEqual(rows[("2026-11-16", merchant)]["failed_payment_count"], 1)
        self.assertEqual(rows[("2026-11-15", merchant)]["payment_amount_vnd"], 30)

        self.load("transactions", [fixtures.transaction("late", amount=20, occurred_at=D15)])
        self.load("accounts", [fixtures.account(balance=950, updated_at=fixtures.BASE + timedelta(hours=2)),
                               fixtures.account("merchant", balance=50, updated_at=fixtures.BASE + timedelta(hours=2))])
        self.build()
        self.assertEqual(self.rows()[("2026-11-15", merchant)]["payment_amount_vnd"], 50)

    def test_quality_and_quarantine_block_publication(self):
        self.seed()
        self.build()
        before = self.rows()
        accounts = f"lakehouse.{self.fixture.silver_ns}.dim_accounts"
        self.spark.sql(f"UPDATE {accounts} SET balance_vnd = balance_vnd + 1 WHERE customer_id IS NOT NULL")
        with self.assertRaisesRegex(ValueError, "quality gate failed"):
            self.build()
        self.assertEqual(before, self.rows())
        self.spark.sql(f"UPDATE {accounts} SET balance_vnd = balance_vnd - 1 WHERE customer_id IS NOT NULL")
        self.load("transactions", [fixtures.transaction("bad-amount", amount=-1)])
        with self.assertRaisesRegex(ValueError, "quarantine"):
            self.build()
        self.assertEqual(before, self.rows())

    def test_crash_before_run_ledger_and_pinned_input(self):
        self.seed()
        with patch.object(self.gold, "write_run", side_effect=RuntimeError("injected ledger failure")):
            with self.assertRaisesRegex(RuntimeError, "injected ledger"):
                self.build()
        before = self.rows()
        self.assertEqual(self.spark.table(f"lakehouse.{self.gold_ns}._materializations").count(), 0)
        self.build()
        self.assertEqual(before, self.rows())
        # Modify live input after the gate. Gold still uses precisely the checked snapshots.
        real_check = self.gold.check_silver
        def checked_then_changed(*args, **kwargs):
            report = real_check(*args, **kwargs)
            table = f"lakehouse.{self.fixture.silver_ns}.fact_transactions"
            self.spark.sql(f"UPDATE {table} SET occurred_at = occurred_at + INTERVAL 7 DAYS")
            return report
        with patch.object(self.gold, "check_silver", side_effect=checked_then_changed):
            self.build()
        self.assertEqual(before, self.rows())
        # Another dataset cannot reuse the same Gold destination.
        target = f"lakehouse.{self.fixture.silver_ns}.dim_accounts"
        self.spark.sql(f"ALTER TABLE {target} SET TBLPROPERTIES ('silver.source_dataset'='[\"wrong\",\"db\",\"schema\"]')")
        with self.assertRaises(ValueError):
            self.build()
        self.assertEqual(before, self.rows())

    def test_empty_input_replaces_old_groups_and_uncheckpointed_rows_block(self):
        self.seed()
        self.build()
        tx_table = f"lakehouse.{self.fixture.silver_ns}.fact_transactions"
        self.spark.sql(f"UPDATE {tx_table} SET _batch_id = 'unfinished'")
        with self.assertRaisesRegex(ValueError, "uncheckpointed"):
            self.build()
        self.spark.sql(f"DELETE FROM {tx_table}")
        accounts = f"lakehouse.{self.fixture.silver_ns}.dim_accounts"
        self.spark.sql(f"UPDATE {accounts} SET balance_vnd = opening_balance_vnd")
        report = self.build()
        self.assertEqual(report["row_count"], 0)
        self.assertEqual(self.rows(), {})
        self.assertEqual(report["reconciliation"]["input_transactions"], 0)

    def test_aggregation_can_exceed_bigint_without_losing_vnd(self):
        from pyspark.sql import functions as F

        maximum = 9223372036854775807
        events = self.spark.createDataFrame(
            [("one", "PAYMENT", "SUCCEEDED", maximum, "merchant"),
             ("two", "PAYMENT", "SUCCEEDED", maximum, "merchant")],
            "transaction_id STRING, transaction_type STRING, status STRING, amount_vnd LONG, merchant_id STRING"
        ).withColumn("business_date", F.lit("2026-11-17").cast("date"))
        mart = self.gold.build_mart(events)
        row = mart.first()
        self.assertEqual(row["payment_amount_vnd"], maximum * 2)
        self.assertEqual(row["net_amount_vnd"], maximum * 2)
        self.assertEqual(row["payment_count"], 2)
        self.assertTrue(self.gold.reconcile(self.spark, events, mart)["ok"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
