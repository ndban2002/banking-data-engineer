"""Read-only cross-table checks over pinned Silver snapshots.

Run after loading all four tables from a quiescent source. Pinning each Iceberg
table makes this report repeatable; it does not create a shared PostgreSQL
cutoff. Use --require-processed to include Silver quarantine and checkpoints.
"""

from __future__ import annotations

import argparse
import json
import uuid

from bronze_loader import current_snapshot_id, identifier
from job_reporting import add_report_argument, emit_report
from silver_loader import read_snapshot, table_properties


TABLES = {
    "customers": ("dim_customers", "customer_id"),
    "merchants": ("dim_merchants", "merchant_id"),
    "accounts": ("dim_accounts", "account_id"),
    "transactions": ("fact_transactions", "transaction_id"),
}
BINDING_KEYS = ("silver.source_dataset", "silver.source_namespace", "silver.pipeline_version")


def quality_queries(views: dict[str, str]) -> dict[str, str]:
    """Return queries whose rows each represent a violated key or record."""
    c, m, a, t = (views[name] for name in TABLES)
    queries = {
        f"{name}_key_unique": f"SELECT {key} FROM {views[name]} GROUP BY {key} "
                               f"HAVING {key} IS NULL OR COUNT(*) > 1"
        for name, (_, key) in TABLES.items()
    }
    queries.update({
        "transaction_request_unique": f"""
            SELECT request_id FROM {t} GROUP BY request_id
            HAVING request_id IS NULL OR COUNT(*) > 1""",
        "account_owner_xor": f"""
            SELECT account_id FROM {a}
            WHERE (customer_id IS NULL AND merchant_id IS NULL)
               OR (customer_id IS NOT NULL AND merchant_id IS NOT NULL)""",
        "account_customer_fk": f"""
            SELECT a.account_id FROM {a} a LEFT JOIN {c} c ON a.customer_id = c.customer_id
            WHERE a.customer_id IS NOT NULL AND c.customer_id IS NULL""",
        "account_merchant_fk": f"""
            SELECT a.account_id FROM {a} a LEFT JOIN {m} m ON a.merchant_id = m.merchant_id
            WHERE a.merchant_id IS NOT NULL AND m.merchant_id IS NULL""",
        "account_customer_unique": f"""
            SELECT customer_id FROM {a} WHERE customer_id IS NOT NULL
            GROUP BY customer_id HAVING COUNT(*) > 1""",
        "account_merchant_unique": f"""
            SELECT merchant_id FROM {a} WHERE merchant_id IS NOT NULL
            GROUP BY merchant_id HAVING COUNT(*) > 1""",
        "transaction_account_fk": f"""
            SELECT t.transaction_id FROM {t} t
            LEFT JOIN {a} s ON t.source_account_id = s.account_id
            LEFT JOIN {a} d ON t.destination_account_id = d.account_id
            WHERE s.account_id IS NULL OR d.account_id IS NULL
               OR t.source_account_id = t.destination_account_id""",
        "refund_reference": f"""
            SELECT r.transaction_id FROM {t} r
            LEFT JOIN {t} p ON r.original_transaction_id = p.transaction_id
            WHERE (r.transaction_type = 'REFUND' AND
                   (p.transaction_id IS NULL OR r.original_transaction_id = r.transaction_id))
               OR (r.transaction_type = 'PAYMENT' AND r.original_transaction_id IS NOT NULL)""",
        "successful_payment_direction": f"""
            SELECT t.transaction_id FROM {t} t
            LEFT JOIN {a} s ON t.source_account_id = s.account_id
            LEFT JOIN {a} d ON t.destination_account_id = d.account_id
            WHERE t.transaction_type = 'PAYMENT' AND t.status = 'SUCCEEDED'
              AND (s.customer_id IS NULL OR d.merchant_id IS NULL)""",
        "successful_refund_consistency": f"""
            SELECT r.transaction_id FROM {t} r
            LEFT JOIN {t} p ON r.original_transaction_id = p.transaction_id
            WHERE r.transaction_type = 'REFUND' AND r.status = 'SUCCEEDED' AND (
                p.transaction_id IS NULL OR p.transaction_type <> 'PAYMENT'
                OR p.status <> 'SUCCEEDED'
                OR r.source_account_id <> p.destination_account_id
                OR r.destination_account_id <> p.source_account_id
                OR r.amount_vnd <> p.amount_vnd OR r.channel <> p.channel
                OR r.occurred_at < p.occurred_at)""",
        "successful_refund_limit": f"""
            SELECT original_transaction_id FROM {t}
            WHERE transaction_type = 'REFUND' AND status = 'SUCCEEDED'
            GROUP BY original_transaction_id HAVING COUNT(*) > 1""",
        "account_balance_reconciliation": f"""
            WITH movements AS (
                SELECT source_account_id AS account_id,
                       -CAST(amount_vnd AS DECIMAL(38, 0)) AS delta
                FROM {t} WHERE status = 'SUCCEEDED'
                UNION ALL
                SELECT destination_account_id, CAST(amount_vnd AS DECIMAL(38, 0))
                FROM {t} WHERE status = 'SUCCEEDED'
            ), net AS (SELECT account_id, SUM(delta) AS delta FROM movements GROUP BY account_id)
            SELECT a.account_id FROM {a} a LEFT JOIN net n ON a.account_id = n.account_id
            WHERE a.balance_vnd IS NULL OR a.opening_balance_vnd IS NULL
               OR a.balance_vnd < 0 OR a.opening_balance_vnd < 0
               OR CAST(a.balance_vnd AS DECIMAL(38, 0)) <>
                  CAST(a.opening_balance_vnd AS DECIMAL(38, 0)) + COALESCE(n.delta, 0)""",
    })
    return queries


def processing_violations(spark, frames: dict, namespace: str, binding: dict) -> dict:
    """Count quarantined rows and per-table batch references without checkpoints."""
    from pyspark.sql import functions as F

    controls = {}
    for name in ("_processed_batches", "_quarantine"):
        target = f"lakehouse.{namespace}.{name}"
        if not spark.catalog.tableExists(target):
            raise ValueError(f"missing Silver control table: {target}")
        actual = table_properties(spark, target)
        if any(actual.get(key) != value for key, value in binding.items()):
            raise ValueError("Silver control tables have different source bindings")
        controls[name] = read_snapshot(spark, target)

    completed = controls["_processed_batches"]
    uncheckpointed = 0
    for source, frame in frames.items():
        accepted = completed.where(F.col("silver_table") == f"lakehouse.{namespace}.{TABLES[source][0]}")
        rows = frame.select(F.col("_batch_id").alias("batch_id"),
                            F.col("_source_key").alias("source_key")).distinct()
        uncheckpointed += rows.join(accepted.select("batch_id", "source_key"),
                                   ["batch_id", "source_key"], "left_anti").count()
    return {"quarantined_rows": controls["_quarantine"].count(), "uncheckpointed_batches": uncheckpointed}


def check_silver(spark, namespace: str = "silver", *, snapshots: dict | None = None,
                 require_processed: bool = False) -> dict:
    """Check one dataset, returning counts and the exact snapshots inspected.

    Missing tables, absent source bindings and unreadable snapshots raise rather
    than returning a misleading successful partial report. Gold supplies an
    explicit snapshot map to validate exactly the same inputs it will consume.
    """
    identifier(namespace)
    requested = snapshots
    if requested is not None and set(requested) != {
        f"lakehouse.{namespace}.{name}" for name, _ in TABLES.values()
    }:
        raise ValueError("snapshot map must identify exactly the four Silver tables")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    # Overflow must fail the report instead of producing NULL in monetary sums.
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    prefix = "silver_check_" + uuid.uuid4().hex
    views, frames, snapshots = {}, {}, {}
    binding = None
    try:
        for source, (name, _) in TABLES.items():
            target = f"lakehouse.{namespace}.{name}"
            if not spark.catalog.tableExists(target):
                raise ValueError(f"required Silver table is missing: {target}")
            properties = table_properties(spark, target)
            current_binding = {key: properties.get(key) for key in BINDING_KEYS}
            if not all(current_binding.values()):
                raise ValueError(f"Silver table has no complete source binding: {target}")
            if binding is None:
                binding = current_binding
            elif binding != current_binding:
                raise ValueError("Silver tables are bound to different datasets or pipeline versions")
            dataset = json.loads(current_binding["silver.source_dataset"])
            if not isinstance(dataset, list) or len(dataset) != 3:
                raise ValueError("invalid Silver source dataset binding")
            expected_key = json.dumps([*dataset, source], separators=(",", ":"))
            if properties.get("silver.source_key") != expected_key:
                raise ValueError(f"Silver source table binding does not match: {target}")
            snapshot = (requested[target] if requested is not None
                        else current_snapshot_id(spark, target))
            snapshots[target] = snapshot
            frame = (spark.read.option("snapshot-id", str(snapshot)).table(target)
                     if snapshot is not None else spark.table(target).limit(0))
            view = f"{prefix}_{source}"
            frame.createOrReplaceTempView(view)
            views[source] = view
            frames[source] = frame
        # Counts only: the report does not expose names, IDs or rejected payloads.
        violations = {name: spark.sql(query).count()
                      for name, query in quality_queries(views).items()}
        pending = spark.sql(f"SELECT COUNT(*) AS n FROM {views['transactions']} "
                            "WHERE status = 'PENDING'").first()["n"]
        report = {
            "ok": not any(violations.values()),
            "namespace": namespace,
            "snapshots": snapshots,
            "violations": violations,
            "pending_transactions": pending,
            "not_checked": {
                "historical_merchant_channel": "Current dimensions do not prove channel at settlement.",
                "shared_source_cutoff": "Run after ingesting a quiescent source; table snapshots are separate.",
            },
        }
        if require_processed:
            controls = processing_violations(spark, frames, namespace, binding)
            report["processing_violations"] = controls
            report["ok"] = report["ok"] and not any(controls.values())
        else:
            report["not_checked"]["batch_quarantine"] = "Review loader batch rejection counts and quarantine separately."
        return report
    finally:
        for view in views.values():
            spark.catalog.dropTempView(view)
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", default="silver")
    parser.add_argument("--require-processed", action="store_true",
                        help="Also require empty quarantine and checkpoints for the Silver output")
    add_report_argument(parser)
    args = parser.parse_args(argv)
    try:
        identifier(args.namespace)
    except ValueError as exc:
        parser.error(str(exc))
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("bank-sim-silver-quality").getOrCreate()
    try:
        report = check_silver(spark, args.namespace, require_processed=args.require_processed)
        emit_report(report, orchestration=args.orchestration_report)
        if not report["ok"]:
            raise SystemExit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
