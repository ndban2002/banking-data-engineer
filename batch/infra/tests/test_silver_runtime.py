"""Integration checks against real Spark and Iceberg, without a PostgreSQL source.

The regular pytest suite skips these checks. Run this file with spark-submit in
the existing Spark container, or set RUN_SILVER_RUNTIME=1 in a configured Spark
environment. Each test writes only to fresh, UUID-named namespaces and deletes
only those namespaces afterwards. No existing Bronze or Silver tables are read.
"""

from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
import uuid


for jobs_dir in (Path(__file__).resolve().parents[1] / "jobs",
                 Path("/opt/spark/work-dir/jobs")):
    if jobs_dir.is_dir():
        sys.path.insert(0, str(jobs_dir))

RUN_RUNTIME = __name__ == "__main__" or os.environ.get("RUN_SILVER_RUNTIME") == "1"
BASE = datetime(2026, 11, 16, tzinfo=timezone.utc)
SCHEMAS = {
    "customers": "id STRING, name STRING, region STRING, created_at TIMESTAMP",
    "merchants": ("id STRING, name STRING, mcc STRING, region STRING, channel STRING, "
                  "created_at TIMESTAMP"),
    "accounts": ("id STRING, customer_id STRING, merchant_id STRING, currency STRING, "
                 "opening_balance LONG, balance LONG, created_at TIMESTAMP, updated_at TIMESTAMP"),
    "transactions": (
        "id STRING, request_id STRING, kind STRING, source_account_id STRING, "
        "destination_account_id STRING, amount LONG, currency STRING, channel STRING, "
        "status STRING, error_code STRING, original_transaction_id STRING, "
        "requested_decline BOOLEAN, occurred_at TIMESTAMP, created_at TIMESTAMP, updated_at TIMESTAMP"
    ),
}
TARGETS = {
    "customers": "dim_customers", "merchants": "dim_merchants",
    "accounts": "dim_accounts", "transactions": "fact_transactions",
}


def uid(label):
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, "silver-runtime-test/" + label))


def customer(**changes):
    row = dict(id=uid("customer"), name="  Test Customer  ", region=" hcm ", created_at=BASE)
    return {**row, **changes}


def merchant(**changes):
    row = dict(id=uid("merchant"), name="  Test Merchant  ", mcc="5411", region=" hcm ",
               channel="QR", created_at=BASE)
    return {**row, **changes}


def account(owner="customer", **changes):
    row = dict(id=uid(owner + "-account"),
               customer_id=uid("customer") if owner == "customer" else None,
               merchant_id=uid("merchant") if owner == "merchant" else None,
               currency="VND", opening_balance=1000 if owner == "customer" else 0,
               balance=900 if owner == "customer" else 100,
               created_at=BASE, updated_at=BASE + timedelta(seconds=1))
    return {**row, **changes}


def transaction(label="payment", **changes):
    row = dict(id=uid(label), request_id=uid(label + "-request"), kind="PAYMENT",
               source_account_id=uid("customer-account"),
               destination_account_id=uid("merchant-account"), amount=100,
               currency="VND", channel="QR", status="SUCCEEDED", error_code=None,
               original_transaction_id=None, requested_decline=False,
               occurred_at=BASE - timedelta(days=1), created_at=BASE,
               updated_at=BASE + timedelta(seconds=1))
    return {**row, **changes}


@unittest.skipUnless(RUN_RUNTIME, "requires configured Spark/Iceberg runtime; execute with spark-submit")
class SilverRuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        import bronze_loader
        import silver_loader

        cls.bronze = bronze_loader
        cls.loader = silver_loader
        cls.spark = (SparkSession.builder
                     .master(os.environ.get("SILVER_TEST_MASTER", "local[2]"))
                     .appName("banking-silver-runtime-tests")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.adaptive.enabled", "false")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("WARN")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        suffix = uuid.uuid4().hex
        self.bronze_ns = "silver_test_b_" + suffix
        self.silver_ns = "silver_test_s_" + suffix
        self.owned_namespaces = [self.bronze_ns, self.silver_ns]
        self.settings = self.bronze.SourceSettings(
            host="unused.test", port="5432", database="silver_runtime_fixture",
            schema="core_sim", user="unused", password="unused")
        self.tick = 0
        self.addCleanup(self._cleanup_namespaces)
        self.spark.sql(f"CREATE NAMESPACE lakehouse.{self.bronze_ns}")
        self._create_manifest_table(self.bronze_ns)

    def _create_manifest_table(self, namespace):
        self.spark.sql(f"""CREATE TABLE lakehouse.{namespace}._ingestion_batches (
            batch_id STRING, source_key STRING, target_table STRING, mode STRING,
            row_count BIGINT, observed_at TIMESTAMP, completed_at TIMESTAMP,
            snapshot_id BIGINT, watermark TIMESTAMP
        ) USING iceberg""")

    def _cleanup_namespaces(self):
        # Deliberately avoid a generic 'drop namespace' helper that could be
        # called with a user-supplied production namespace.
        for namespace in self.owned_namespaces:
            self.assertRegex(namespace, r"^silver_test_[bs]_[0-9a-f]{32}$")
            exists = any(row["namespace"] == namespace
                         for row in self.spark.sql("SHOW NAMESPACES IN lakehouse").collect())
            if not exists:
                continue
            for row in self.spark.sql(f"SHOW TABLES IN lakehouse.{namespace}").collect():
                table = row["tableName"]
                self.assertRegex(table, r"^[A-Za-z_][A-Za-z0-9_]*$")
                self.spark.sql(f"DROP TABLE lakehouse.{namespace}.{table} PURGE")
            self.spark.sql(f"DROP NAMESPACE lakehouse.{namespace}")

    def append_batch(self, table, rows, *, observed_at=None, accepted=True,
                     settings=None, namespace=None, snapshot_override=None):
        source_settings = settings or self.settings
        namespace = namespace or self.bronze_ns
        self.tick += 1
        observed_at = observed_at or BASE + timedelta(minutes=self.tick)
        batch_id = str(uuid.uuid4())
        source = self.spark.createDataFrame(rows, SCHEMAS[table])
        spec = self.bronze.TABLES[table]
        target = f"lakehouse.{namespace}.{table}"
        bronze = self.bronze.add_ingestion_metadata(source, source_settings, spec,
                                                    batch_id, observed_at)
        self.bronze.append_bronze(bronze, target, source_settings.source_key(table))
        snapshot_id = self.bronze.current_snapshot_id(self.spark, target)
        if snapshot_override is not None:
            snapshot_id = snapshot_override
        watermark = max((row["updated_at"] for row in rows), default=None) if spec.watermark_column else None
        manifest = dict(batch_id=batch_id, source_key=source_settings.source_key(table),
                        target_table=target, mode="full", row_count=len(rows),
                        observed_at=observed_at, completed_at=observed_at + timedelta(seconds=1),
                        snapshot_id=snapshot_id, watermark=watermark)
        if accepted:
            self.bronze.write_manifest(self.spark, f"lakehouse.{namespace}._ingestion_batches", manifest)
        return manifest

    def process(self, table, batch=None, *, replay=False, bronze_namespace=None):
        return self.loader.process_table(
            self.spark, table, bronze_namespace=bronze_namespace or self.bronze_ns,
            silver_namespace=self.silver_ns,
            batch_id=batch["batch_id"] if batch else None, replay=replay)

    def target(self, table):
        return self.spark.table(f"lakehouse.{self.silver_ns}.{TARGETS[table]}")

    def control_count(self, table):
        name = f"lakehouse.{self.silver_ns}.{table}"
        return self.spark.table(name).count() if self.spark.catalog.tableExists(name) else 0

    def test_four_tables_preserve_types_normalize_dimensions_and_pass_quality_gate(self):
        fixtures = dict(customers=[customer()], merchants=[merchant()],
                        accounts=[account(), account("merchant")], transactions=[transaction()])
        for table, rows in fixtures.items():
            self.process(table, self.append_batch(table, rows))
        self.assertEqual(self.target("customers").first()["customer_name"], "Test Customer")
        self.assertEqual(self.target("customers").first()["region"], "HCM")
        self.assertEqual(self.target("merchants").first()["merchant_name"], "Test Merchant")
        self.assertEqual(self.target("accounts").schema["balance_vnd"].dataType.simpleString(), "bigint")
        self.assertEqual(self.target("transactions").schema["amount_vnd"].dataType.simpleString(), "bigint")
        self.assertEqual(self.target("transactions").first()["amount_vnd"], 100)
        self.assertEqual(self.target("accounts").where("customer_id IS NULL").count(), 1)
        self.assertEqual(self.control_count("_processed_batches"), 4)
        self.assertEqual(self.control_count("_quarantine"), 0)

        from silver_quality import check_silver
        report = check_silver(self.spark, namespace=self.silver_ns)
        self.assertTrue(report["ok"], report)
        self.assertEqual(len(report["snapshots"]), 4)
        # The read-only gate must expose a broken reference, without repairing
        # it silently or modifying the previously written Silver data.
        self.spark.sql(f"""UPDATE lakehouse.{self.silver_ns}.dim_accounts
            SET customer_id = '{uid('missing-customer')}' WHERE customer_id IS NOT NULL""")
        bad = check_silver(self.spark, namespace=self.silver_ns)
        self.assertFalse(bad["ok"], bad)
        self.assertEqual(self.target("accounts").where(
            f"customer_id = '{uid('missing-customer')}'").count(), 1)

    def test_pending_then_success_and_replaying_old_or_current_batch_never_regresses(self):
        pending = self.append_batch("transactions", [transaction(status="PENDING", updated_at=BASE)])
        self.process("transactions", pending)
        success = self.append_batch("transactions", [transaction()])
        self.process("transactions", success)
        before = self.target("transactions").first().asDict()
        self.process("transactions", pending, replay=True)
        self.process("transactions", success, replay=True)
        self.process("transactions", success)
        self.assertEqual(self.target("transactions").count(), 1)
        self.assertEqual(self.target("transactions").first().asDict(), before)
        self.assertEqual(before["status"], "SUCCEEDED")
        self.assertEqual(self.control_count("_processed_batches"), 2)
        self.assertEqual(self.control_count("_quarantine"), 0)

    def test_only_manifest_accepted_snapshot_is_consumed(self):
        accepted = self.append_batch("customers", [customer()])
        orphan = self.append_batch("customers", [customer(id=uid("orphan"), name="Unaccepted")],
                                   accepted=False)
        self.process("customers")
        self.assertEqual(self.target("customers").count(), 1)
        self.assertEqual(self.target("customers").first()["customer_id"], uid("customer"))
        self.assertEqual(self.control_count("_processed_batches"), 1)
        with self.assertRaises(Exception):
            self.process("customers", orphan)
        self.assertEqual(self.control_count("_processed_batches"), 1)
        self.assertEqual(accepted["row_count"], 1)
        # A processed batch ID must continue to name the exact accepted input.
        # Mutate only our disposable ledger to simulate manifest corruption.
        self.spark.sql(f"""UPDATE lakehouse.{self.bronze_ns}._ingestion_batches
            SET row_count = 2 WHERE batch_id = '{accepted['batch_id']}'""")
        with self.assertRaisesRegex(ValueError, "manifest changed"):
            self.process("customers", accepted)
        self.assertEqual(self.target("customers").count(), 1)
        self.assertEqual(self.control_count("_processed_batches"), 1)

    def test_dimension_versions_use_observation_time_and_empty_snapshot_does_not_delete(self):
        old = self.append_batch("merchants", [merchant()], observed_at=BASE + timedelta(minutes=1))
        new = self.append_batch("merchants", [merchant(name="New name", region=" hn ")],
                                observed_at=BASE + timedelta(minutes=2))
        self.process("merchants", new)
        self.process("merchants", old)
        empty = self.append_batch("merchants", [], observed_at=BASE + timedelta(minutes=3))
        self.process("merchants", empty)
        self.process("merchants", empty, replay=True)
        result = self.target("merchants").first()
        self.assertEqual(self.target("merchants").count(), 1)
        self.assertEqual(result["merchant_name"], "New name")
        self.assertEqual(result["region"], "HN")
        self.assertEqual(self.control_count("_processed_batches"), 3)

    def test_failed_invalid_refund_is_retained_and_bad_amount_is_quarantined_once(self):
        rejected_refund = transaction(
            "failed-refund", kind="REFUND", status="FAILED", error_code="INVALID_REFUND",
            original_transaction_id=uid("payment"), amount=55,
            source_account_id=uid("merchant-account"), destination_account_id=uid("customer-account"))
        bad_amount = transaction("bad-amount", amount=-100)
        batch = self.append_batch("transactions", [transaction(), rejected_refund, bad_amount])
        self.process("transactions", batch)
        self.process("transactions", batch, replay=True)
        self.assertEqual(self.target("transactions").count(), 2)
        failed = self.target("transactions").where("status = 'FAILED'").first()
        self.assertEqual(failed["error_code"], "INVALID_REFUND")
        self.assertEqual(self.control_count("_quarantine"), 1)
        row = self.spark.table(f"lakehouse.{self.silver_ns}._quarantine").first()
        self.assertEqual(row["record_id"], bad_amount["id"])
        self.assertEqual(row["batch_id"], batch["batch_id"])
        self.assertTrue(row["reason_codes"])
        self.assertEqual(self.control_count("_processed_batches"), 1)

    def test_equal_version_conflicts_never_choose_a_payload_arbitrarily(self):
        initial = self.append_batch("transactions", [transaction()])
        self.process("transactions", initial)
        conflicting = self.append_batch("transactions", [transaction(amount=200)])
        self.process("transactions", conflicting)
        self.process("transactions", conflicting, replay=True)
        self.assertEqual(self.target("transactions").first()["amount_vnd"], 100)
        self.assertEqual(self.control_count("_quarantine"), 1)

        within_batch = self.append_batch("transactions", [
            transaction("conflict", amount=300), transaction("conflict", amount=301)])
        self.process("transactions", within_batch)
        self.assertEqual(self.target("transactions").count(), 1)
        self.assertEqual(self.control_count("_quarantine"), 3)

    def test_duplicate_delivery_is_deduplicated_and_request_key_conflicts_are_rejected(self):
        first = transaction()
        batch = self.append_batch("transactions", [first, dict(first)])
        self.process("transactions", batch)
        self.assertEqual(self.target("transactions").count(), 1)
        other = transaction("other-payment", request_id=first["request_id"])
        self.process("transactions", self.append_batch("transactions", [other]))
        self.assertEqual(self.target("transactions").count(), 1)
        self.assertEqual(self.control_count("_quarantine"), 1)

    def test_crash_after_outputs_before_checkpoint_can_retry_without_duplicates(self):
        batch = self.append_batch("customers", [customer(), customer(id=uid("bad-customer"), name=" ")])
        with patch.object(self.loader, "write_checkpoint", side_effect=RuntimeError("injected checkpoint failure")):
            with self.assertRaisesRegex(RuntimeError, "injected checkpoint failure"):
                self.process("customers", batch)
        self.assertEqual(self.control_count("_processed_batches"), 0)
        self.assertEqual(self.target("customers").count(), 1)
        self.assertEqual(self.control_count("_quarantine"), 1)
        self.process("customers", batch)
        self.assertEqual(self.target("customers").count(), 1)
        self.assertEqual(self.control_count("_quarantine"), 1)
        self.assertEqual(self.control_count("_processed_batches"), 1)

    def test_unavailable_input_snapshot_does_not_advance_checkpoint(self):
        batch = self.append_batch("customers", [customer()], snapshot_override=9223372036854775000)
        with self.assertRaises(Exception):
            self.process("customers", batch)
        self.assertEqual(self.control_count("_processed_batches"), 0)
        self.assertEqual(self.control_count("_quarantine"), 0)

    def test_silver_namespace_cannot_be_rebound_to_another_bronze_dataset(self):
        self.process("customers", self.append_batch("customers", [customer()]))
        other_namespace = "silver_test_b_" + uuid.uuid4().hex
        self.owned_namespaces.append(other_namespace)
        self.spark.sql(f"CREATE NAMESPACE lakehouse.{other_namespace}")
        self._create_manifest_table(other_namespace)
        other_settings = self.bronze.SourceSettings(
            host="unused.test", port="5432", database="other_dataset", schema="core_sim",
            user="unused", password="unused")
        other_batch = self.append_batch("customers", [customer(name="Wrong source")],
                                        settings=other_settings, namespace=other_namespace)
        with self.assertRaises(Exception):
            self.process("customers", other_batch, bronze_namespace=other_namespace)
        self.assertEqual(self.target("customers").first()["customer_name"], "Test Customer")
        self.assertEqual(self.control_count("_processed_batches"), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
