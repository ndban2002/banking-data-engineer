"""Build the full merchant x Vietnam business-date mart from checked Silver.

One writer per Gold namespace; pause Silver writers during materialization.
Full replacement intentionally recomputes old dates, including disappearing
groups. A single Iceberg overwrite publishes the whole mart. The run ledger
is a separate commit; a retry rebuilds rather than adding monetary deltas.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import uuid

from bronze_loader import current_snapshot_id, identifier
from job_reporting import add_report_argument, emit_report
from silver_loader import ensure_table, table_properties
from silver_quality import TABLES, check_silver, processing_violations


MART = "daily_merchant_performance"
VERSION = "daily_merchant_v1"
BUSINESS_TIMEZONE = "Asia/Ho_Chi_Minh"
UNKNOWN_MERCHANT = "__unattributed__"
KEYS = ["business_date", "merchant_id"]
COUNTS = ["payment_count", "refund_count", "failed_payment_count", "failed_refund_count",
          "pending_payment_count", "pending_refund_count", "transaction_count"]
AMOUNTS = ["payment_amount_vnd", "refund_amount_vnd", "net_amount_vnd"]
METRICS = COUNTS + AMOUNTS
RUN_SCHEMA = """run_id STRING, target_table STRING, input_id STRING,
    source_snapshots STRING, output_snapshot_id LONG, row_count LONG,
    reconciliation STRING, completed_at TIMESTAMP, pipeline_version STRING"""


def input_id(snapshots: dict) -> str:
    encoded = json.dumps({"version": VERSION, "timezone": BUSINESS_TIMEZONE,
                          "snapshots": snapshots}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def merchant_events(spark, frames: dict):
    """Attribute every transaction, including unsuccessful invalid requests.

    Unknown merchant directions on FAILED/PENDING intents get an explicit
    bucket. They must never disappear through an inner join.
    """
    from pyspark.sql import functions as F

    transactions = frames["transactions"]
    required = ["transaction_id", "transaction_type", "status", "amount_vnd", "occurred_at"]
    invalid = (F.col("transaction_id").isNull() | F.col("occurred_at").isNull()
               | F.col("amount_vnd").isNull() | (F.col("amount_vnd") <= 0)
               | ~F.col("status").isin("PENDING", "SUCCEEDED", "FAILED")
               | F.col("status").isNull()
               | ~F.col("transaction_type").isin("PAYMENT", "REFUND")
               | F.col("transaction_type").isNull())
    if transactions.where(invalid).limit(1).count():
        raise ValueError("Gold input has invalid transaction values")
    if transactions.schema["amount_vnd"].dataType.simpleString() != "bigint":
        raise ValueError("Gold expects BIGINT VND in Silver")
    owner_account = F.when(F.col("transaction_type") == "PAYMENT",
                           F.col("destination_account_id")).otherwise(F.col("source_account_id"))
    facts = transactions.select(*required, owner_account.alias("__merchant_account"))
    accounts = frames["accounts"].select(F.col("account_id").alias("__merchant_account"), "merchant_id")
    events = facts.join(accounts, "__merchant_account", "left")
    if events.where((F.col("status") == "SUCCEEDED") & F.col("merchant_id").isNull()).limit(1).count():
        raise ValueError("successful transaction has no merchant attribution")
    return events.select(
        *required,
        F.coalesce("merchant_id", F.lit(UNKNOWN_MERCHANT)).alias("merchant_id"),
        F.to_date(F.from_utc_timestamp("occurred_at", BUSINESS_TIMEZONE)).alias("business_date"),
    )


def build_mart(events):
    from pyspark.sql import functions as F

    expressions = []
    for status, prefix in (("SUCCEEDED", ""), ("FAILED", "failed_"), ("PENDING", "pending_")):
        for kind in ("PAYMENT", "REFUND"):
            selected = (F.col("status") == status) & (F.col("transaction_type") == kind)
            expressions.append(F.sum(F.when(selected, 1).otherwise(0)).cast("long").alias(
                f"{prefix}{kind.lower()}_count"))
            if status == "SUCCEEDED":
                expressions.append(F.sum(F.when(selected, F.col("amount_vnd").cast("decimal(38,0)"))
                                         .otherwise(F.lit(0).cast("decimal(38,0)"))).alias(
                                             f"{kind.lower()}_amount_vnd"))
    return (events.groupBy(*KEYS).agg(*expressions, F.count("*").alias("transaction_count"))
            .withColumn("net_amount_vnd", (F.col("payment_amount_vnd") - F.col("refund_amount_vnd"))
                        .cast("decimal(38,0)"))
            .select(*KEYS, *METRICS))


def reconcile(spark, events, mart) -> dict:
    """Independent SQL grouping checks every merchant/day, not just grand totals."""
    prefix = "gold_reconcile_" + uuid.uuid4().hex
    events.createOrReplaceTempView(prefix)
    try:
        expressions = []
        for status, label in (("SUCCEEDED", ""), ("FAILED", "failed_"), ("PENDING", "pending_")):
            for kind in ("PAYMENT", "REFUND"):
                condition = f"status = '{status}' AND transaction_type = '{kind}'"
                expressions.append(f"SUM(CASE WHEN {condition} THEN 1 ELSE 0 END) AS {label}{kind.lower()}_count")
        for kind in ("PAYMENT", "REFUND"):
            expressions.append(f"SUM(CASE WHEN status = 'SUCCEEDED' AND transaction_type = '{kind}' "
                               f"THEN CAST(amount_vnd AS DECIMAL(38,0)) ELSE 0 END) AS {kind.lower()}_amount_vnd")
        baseline = spark.sql(f"SELECT business_date, merchant_id, {', '.join(expressions)}, "
                             f"COUNT(*) AS transaction_count FROM {prefix} GROUP BY business_date, merchant_id")
        from pyspark.sql import functions as F

        baseline = baseline.withColumn("net_amount_vnd", F.col("payment_amount_vnd") - F.col("refund_amount_vnd"))
        columns = KEYS + METRICS
        mismatch = (mart.select(*columns).exceptAll(baseline.select(*columns)).limit(1).count()
                    + baseline.select(*columns).exceptAll(mart.select(*columns)).limit(1).count())
        if mismatch:
            raise ValueError("Gold merchant/day metrics differ from SQL baseline")
        sums = mart.agg(*[F.coalesce(F.sum(name), F.lit(0)).alias(name) for name in METRICS]).first().asDict()
        source_count = events.count()
        if sums["transaction_count"] != source_count:
            raise ValueError("Gold attribution lost or duplicated transactions")
        return {"ok": True, "input_transactions": source_count,
                "totals": {key: str(value) if key in AMOUNTS else value for key, value in sums.items()}}
    finally:
        spark.catalog.dropTempView(prefix)


def write_run(spark, target: str, record: dict) -> None:
    spark.createDataFrame([record], RUN_SCHEMA).writeTo(target).append()


def materialize(spark, *, silver_namespace="silver", gold_namespace="gold",
                source_snapshots: dict | None = None) -> dict:
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    identifier(silver_namespace)
    identifier(gold_namespace)
    if silver_namespace == gold_namespace:
        raise ValueError("Silver and Gold namespaces must differ")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    events = mart = None
    try:
        snapshots = {f"lakehouse.{silver_namespace}.{name}": current_snapshot_id(
            spark, f"lakehouse.{silver_namespace}.{name}") for name, _ in TABLES.values()}
        if source_snapshots is not None:
            if (not isinstance(source_snapshots, dict) or source_snapshots.keys() != snapshots.keys()
                    or any(value is not None and type(value) is not int for value in source_snapshots.values())):
                raise ValueError("expected Silver snapshots must map all four tables to integer snapshot IDs or null")
            if source_snapshots != snapshots:
                raise ValueError("current Silver snapshots differ from the quality gate; rerun Silver quality")
        quality = check_silver(spark, silver_namespace, snapshots=snapshots)
        if not quality["ok"]:
            raise ValueError(f"Silver quality gate failed: {quality['violations']}")
        frames = {source: (spark.read.option("snapshot-id", str(snapshots[target])).table(target)
                           if snapshots[target] is not None else spark.table(target).limit(0))
                  for source, (name, _) in TABLES.items()
                  for target in [f"lakehouse.{silver_namespace}.{name}"]}
        source_properties = table_properties(spark, f"lakehouse.{silver_namespace}.dim_accounts")
        binding = {key: source_properties[key] for key in
                   ("silver.source_namespace", "silver.source_dataset", "silver.pipeline_version")}
        processing = processing_violations(spark, frames, silver_namespace, binding)
        if processing["quarantined_rows"]:
            raise ValueError("Silver quarantine is not empty; resolve/rebuild Silver before Gold")
        if processing["uncheckpointed_batches"]:
            raise ValueError("Silver has uncheckpointed output; retry Silver before Gold")
        events = merchant_events(spark, frames).persist(StorageLevel.MEMORY_AND_DISK)
        mart = build_mart(events).persist(StorageLevel.MEMORY_AND_DISK)
        row_count = mart.count()
        reconciliation = reconcile(spark, events, mart)
        fingerprint = input_id(snapshots)
        output = mart.withColumn("_input_id", F.lit(fingerprint)).withColumn(
            "_source_snapshots", F.lit(json.dumps(snapshots, sort_keys=True)))
        target = f"lakehouse.{gold_namespace}.{MART}"
        ledger = f"lakehouse.{gold_namespace}._materializations"
        properties = {"gold.source_namespace": silver_namespace,
                      "gold.source_dataset": binding["silver.source_dataset"],
                      "gold.pipeline_version": VERSION}
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{gold_namespace}")
        ensure_table(spark, ledger, RUN_SCHEMA, properties)
        if spark.catalog.tableExists(target):
            actual = table_properties(spark, target)
            if any(actual.get(k) != v for k, v in properties.items()):
                raise ValueError("Gold target belongs to another dataset or version; use a new namespace")
            output.writeTo(target).overwrite(F.lit(True))
        else:
            writer = output.writeTo(target).using("iceberg").partitionedBy("business_date")
            for key, value in properties.items():
                writer = writer.tableProperty(key, value)
            writer.create()
        snapshot = current_snapshot_id(spark, target)
        # Verify the published snapshot before marking this run complete.
        published = (spark.read.option("snapshot-id", str(snapshot)).table(target)
                     if snapshot is not None else spark.table(target).limit(0))
        if (published.exceptAll(output).limit(1).count()
                or output.exceptAll(published).limit(1).count()):
            raise ValueError("published Gold snapshot differs from validated output")
        record = dict(run_id=str(uuid.uuid4()), target_table=target, input_id=fingerprint,
                      source_snapshots=json.dumps(snapshots, sort_keys=True), output_snapshot_id=snapshot,
                      row_count=row_count, reconciliation=json.dumps(reconciliation),
                      completed_at=datetime.now(timezone.utc), pipeline_version=VERSION)
        write_run(spark, ledger, record)
        return {**record, "reconciliation": reconciliation, "pending_transactions": quality["pending_transactions"],
                "unattributed_merchant": UNKNOWN_MERCHANT, "silver_not_checked": quality["not_checked"]}
    finally:
        if mart is not None:
            mart.unpersist()
        if events is not None:
            events.unpersist()
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-namespace", default="silver")
    parser.add_argument("--gold-namespace", default="gold")
    parser.add_argument("--source-snapshots", type=json.loads, default=argparse.SUPPRESS,
                        help="JSON object of the four Silver snapshots expected from the quality gate")
    add_report_argument(parser)
    args = parser.parse_args(argv)
    if hasattr(args, "source_snapshots") and not isinstance(args.source_snapshots, dict):
        parser.error("--source-snapshots must be a JSON object")
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("bank-sim-daily-merchant-gold").getOrCreate()
    try:
        report = materialize(spark, silver_namespace=args.silver_namespace, gold_namespace=args.gold_namespace,
                             source_snapshots=getattr(args, "source_snapshots", None))
        emit_report(report, orchestration=args.orchestration_report)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
