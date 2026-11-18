"""Verify the Gold rollups against real Spark and Iceberg in an isolated namespace.

Run this file with spark-submit inside the configured Spark container. The
normal pytest run skips these cases unless RUN_GOLD_ROLLUPS_RUNTIME=1.
"""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import sys
import unittest
import uuid


for path in (Path(__file__).resolve().parents[1] / "jobs", Path("/opt/spark/work-dir/jobs")):
    if path.is_dir():
        sys.path.insert(0, str(path))

import gold_daily_merchant as daily
from bronze_loader import current_snapshot_id
from silver_loader import ensure_table


RUN_RUNTIME = __name__ == "__main__" or os.environ.get("RUN_GOLD_ROLLUPS_RUNTIME") == "1"
DAY_1 = date(2026, 1, 5)
DAY_2 = date(2026, 1, 6)
DAY_3 = date(2026, 2, 1)
JAN = date(2026, 1, 1)
FEB = DAY_3
MERCHANT_A = "10000000-0000-4000-8000-000000000001"
MERCHANT_B = "10000000-0000-4000-8000-000000000002"
MERCHANT_C = "10000000-0000-4000-8000-000000000003"
MERCHANT_D = "10000000-0000-4000-8000-000000000004"
PLATFORM = "daily_platform_performance"
MONTHLY = "monthly_merchant_performance"
MAX_LONG = 9223372036854775807


def merchant_day(day, merchant, *, payments=0, refunds=0, failed=0, pending=0,
                 payment_amount=0, refund_amount=0):
    return {
        "business_date": day,
        "merchant_id": merchant,
        "payment_count": payments,
        "refund_count": refunds,
        "failed_payment_count": failed,
        "failed_refund_count": 0,
        "pending_payment_count": pending,
        "pending_refund_count": 0,
        "transaction_count": payments + refunds + failed + pending,
        "payment_amount_vnd": Decimal(payment_amount),
        "refund_amount_vnd": Decimal(refund_amount),
        "net_amount_vnd": Decimal(payment_amount - refund_amount),
    }


@unittest.skipUnless(RUN_RUNTIME, "requires Spark/Iceberg; execute with spark-submit")
class GoldRollupRuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        import gold_rollups

        cls.rollups = gold_rollups
        cls.spark = (SparkSession.builder.master("local[2]").appName("gold-rollup-runtime-tests")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.shuffle.partitions", "2")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("WARN")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.namespace = f"gold_rollup_test_{uuid.uuid4().hex}"
        self.source = self.table(daily.MART)
        self.ledger = self.table("_materializations")
        self.properties = {
            "gold.source_namespace": "silver_fixture",
            "gold.source_dataset": json.dumps(["banking-simulator", "fixture", "core_sim"]),
            "gold.pipeline_version": daily.VERSION,
        }
        self.completed_at = datetime(2026, 3, 1, tzinfo=timezone.utc)
        self.addCleanup(self.cleanup_namespace)
        self.spark.sql(f"CREATE NAMESPACE lakehouse.{self.namespace}")
        ensure_table(self.spark, self.ledger, daily.RUN_SCHEMA, self.properties)

    def table(self, name):
        return f"lakehouse.{self.namespace}.{name}"

    def cleanup_namespace(self):
        self.assertRegex(self.namespace, r"^gold_rollup_test_[a-f0-9]{32}$")
        for name in (PLATFORM, MONTHLY, daily.MART, "_materializations"):
            self.spark.sql(f"DROP TABLE IF EXISTS {self.table(name)} PURGE")
        self.spark.sql(f"DROP NAMESPACE IF EXISTS lakehouse.{self.namespace}")

    def write_daily(self, rows):
        from pyspark.sql import functions as F

        # Give each fixture upstream lineage like the real daily merchant job.
        self.completed_at += timedelta(seconds=1)
        lineage = {f"lakehouse.silver_fixture.{name}": int(self.completed_at.timestamp()) + index
                   for index, (name, _) in enumerate(daily.TABLES.values())}
        input_id = daily.input_id(lineage)
        source_snapshots = json.dumps(lineage, sort_keys=True)
        schema = ", ".join(["business_date DATE", "merchant_id STRING"]
                           + [f"{name} LONG" for name in daily.COUNTS]
                           + [f"{name} DECIMAL(38,0)" for name in daily.AMOUNTS])
        frame = (self.spark.createDataFrame(rows, schema)
                 .withColumn("_input_id", F.lit(input_id))
                 .withColumn("_source_snapshots", F.lit(source_snapshots)))
        writer = frame.writeTo(self.source)
        if self.spark.catalog.tableExists(self.source):
            writer.overwrite(F.lit(True))
        else:
            writer = writer.using("iceberg").partitionedBy("business_date")
            for key, value in self.properties.items():
                writer = writer.tableProperty(key, value)
            writer.create()
        totals = {name: sum(row[name] for row in rows) for name in daily.METRICS}
        report = {
            "ok": True,
            "input_transactions": totals["transaction_count"],
            "totals": {name: str(value) if name in daily.AMOUNTS else value for name, value in totals.items()},
        }
        return {
            "run_id": str(uuid.uuid4()),
            "target_table": self.source,
            "input_id": input_id,
            "source_snapshots": source_snapshots,
            "output_snapshot_id": current_snapshot_id(self.spark, self.source),
            "row_count": len(rows),
            "reconciliation": json.dumps(report),
            "completed_at": self.completed_at,
            "pipeline_version": daily.VERSION,
        }

    def build(self):
        return self.rollups.materialize(self.spark, gold_namespace=self.namespace)

    def platform_rows(self):
        return {row["business_date"]: row.asDict() for row in self.spark.table(self.table(PLATFORM)).collect()}

    def monthly_rows(self):
        return {(row["business_month"], row["merchant_id"]): row.asDict()
                for row in self.spark.table(self.table(MONTHLY)).collect()}

    def test_rollup_amounts_rates_and_dense_rank(self):
        source_rows = [
            merchant_day(DAY_1, MERCHANT_A, payments=3, failed=1, payment_amount=MAX_LONG),
            merchant_day(DAY_1, MERCHANT_B, payments=1, payment_amount=MAX_LONG),
            merchant_day(DAY_1, daily.UNKNOWN_MERCHANT, failed=2, pending=1),
            merchant_day(DAY_2, MERCHANT_A, payments=1, refunds=1, failed=5,
                         payment_amount=100, refund_amount=100),
            merchant_day(DAY_2, MERCHANT_C, refunds=1, refund_amount=450),
            merchant_day(DAY_2, MERCHANT_D, pending=1),
            merchant_day(DAY_3, MERCHANT_C, payments=1, payment_amount=500),
        ]
        source = self.write_daily(source_rows)
        daily.write_run(self.spark, self.ledger, source)
        report = self.build()
        self.assertEqual(report["source_snapshot_id"], source["output_snapshot_id"])
        self.assertEqual(report["source_run_id"], source["run_id"])

        platform = self.platform_rows()
        self.assertEqual(set(platform), {DAY_1, DAY_2, DAY_3})
        first = platform[DAY_1]
        self.assertEqual(first["payment_amount_vnd"], Decimal(MAX_LONG) * 2)
        self.assertEqual(first["transaction_count"], 8)
        self.assertEqual(first["active_merchant_count"], 2)
        self.assertEqual(first["unattributed_transaction_count"], 3)
        self.assertEqual(first["payment_success_rate_pct"], Decimal("57.1429"))
        self.assertEqual(platform[DAY_2]["net_amount_vnd"], Decimal(-450))
        self.assertEqual(platform[DAY_2]["active_merchant_count"], 1)

        monthly = self.monthly_rows()
        self.assertEqual(set(monthly), {
            (JAN, MERCHANT_A), (JAN, MERCHANT_B), (JAN, MERCHANT_C),
            (JAN, MERCHANT_D), (FEB, MERCHANT_C),
        })
        merchant_a = monthly[JAN, MERCHANT_A]
        self.assertEqual(merchant_a["payment_days"], 2)
        self.assertEqual(merchant_a["payment_amount_vnd"], Decimal(MAX_LONG) + 100)
        self.assertEqual(merchant_a["payment_success_rate_pct"], Decimal("40.0000"))
        self.assertEqual(merchant_a["net_amount_vnd"], Decimal(MAX_LONG))
        self.assertEqual(merchant_a["net_rank"], 1)
        self.assertEqual(monthly[JAN, MERCHANT_B]["net_rank"], 1)
        self.assertEqual(monthly[JAN, MERCHANT_C]["net_rank"], 2)
        self.assertEqual(monthly[JAN, MERCHANT_C]["net_amount_vnd"], Decimal(-450))
        self.assertIsNone(monthly[JAN, MERCHANT_D]["payment_success_rate_pct"])
        self.assertIsNone(monthly[JAN, MERCHANT_D]["net_rank"])
        self.assertEqual(monthly[FEB, MERCHANT_C]["net_rank"], 1)

    def test_replay_pins_completed_snapshot_and_bad_ledger_preserves_outputs(self):
        completed = self.write_daily([
            merchant_day(DAY_1, MERCHANT_A, payments=1, payment_amount=170),
        ])
        daily.write_run(self.spark, self.ledger, completed)
        unrecorded = self.write_daily([
            merchant_day(DAY_3, MERCHANT_B, payments=1, payment_amount=980),
        ])
        self.assertNotEqual(unrecorded["output_snapshot_id"], completed["output_snapshot_id"])

        first = self.build()
        before = self.platform_rows(), self.monthly_rows()
        self.assertEqual(first["source_snapshot_id"], completed["output_snapshot_id"])
        self.assertEqual(set(before[0]), {DAY_1})
        self.assertEqual(before[0][DAY_1]["net_amount_vnd"], Decimal(170))
        replay = self.build()
        self.assertEqual(first["source_run_id"], replay["source_run_id"])
        self.assertEqual(before, (self.platform_rows(), self.monthly_rows()))

        # A completed run with a conflicting ledger total must block both writes.
        invalid_report = json.loads(unrecorded["reconciliation"])
        invalid_report["totals"]["payment_amount_vnd"] = "981"
        daily.write_run(self.spark, self.ledger, {
            **unrecorded, "reconciliation": json.dumps(invalid_report),
        })
        output_snapshots = {name: current_snapshot_id(self.spark, self.table(name))
                            for name in (PLATFORM, MONTHLY)}
        with self.assertRaises(ValueError):
            self.build()
        self.assertEqual(output_snapshots, {name: current_snapshot_id(self.spark, self.table(name))
                                           for name in output_snapshots})
        self.assertEqual(before, (self.platform_rows(), self.monthly_rows()))

        daily.write_run(self.spark, self.ledger, {
            **unrecorded, "run_id": str(uuid.uuid4()),
            "completed_at": unrecorded["completed_at"] + timedelta(seconds=1),
        })
        refreshed = self.build()
        self.assertEqual(refreshed["source_snapshot_id"], unrecorded["output_snapshot_id"])
        self.assertEqual(set(self.platform_rows()), {DAY_3})
        self.assertEqual(set(self.monthly_rows()), {(FEB, MERCHANT_B)})


if __name__ == "__main__":
    unittest.main(verbosity=2)
