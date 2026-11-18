"""Build platform KPIs and monthly merchant ranks from a completed daily Gold run."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import uuid

from bronze_loader import current_snapshot_id, identifier
from gold_daily_merchant import AMOUNTS, MART, METRICS, UNKNOWN_MERCHANT, VERSION as DAILY_VERSION, write_run
from silver_loader import read_snapshot, table_properties


VERSION = "gold_rollups_v1"
DAILY_PLATFORM = "daily_platform_performance"
MONTHLY_MERCHANT = "monthly_merchant_performance"
BINDING_KEYS = ("gold.source_namespace", "gold.source_dataset", "gold.pipeline_version")


def completed_daily_run(spark, source: str, ledger: str) -> dict:
    from pyspark.sql import functions as F

    if not spark.catalog.tableExists(source) or not spark.catalog.tableExists(ledger):
        raise ValueError("run gold_daily_merchant.py first")
    binding = table_properties(spark, source)
    ledger_binding = table_properties(spark, ledger)
    if (binding.get("gold.pipeline_version") != DAILY_VERSION
            or any(not binding.get(key) or binding[key] != ledger_binding.get(key) for key in BINDING_KEYS)):
        raise ValueError("daily Gold and its ledger must have the same source binding and pipeline version")
    run = (read_snapshot(spark, ledger).where(F.col("target_table") == source)
           .orderBy(F.col("completed_at").desc_nulls_last(), F.col("run_id").desc())
           .first())
    if run is None:
        raise ValueError("no completed daily Gold run")
    if run["pipeline_version"] != DAILY_VERSION or not run["input_id"] or run["completed_at"] is None:
        raise ValueError("latest daily Gold run has invalid lineage")
    if run["output_snapshot_id"] is None and run["row_count"] != 0:
        raise ValueError("nonempty daily Gold run requires an output snapshot")
    return run.asDict()


def metric_sums():
    from pyspark.sql import functions as F

    return [F.sum(name).alias(name) for name in METRICS]


def total_metrics(frame) -> dict:
    row = frame.agg(*metric_sums()).first().asDict()
    return {name: (str(row[name] or 0) if name in AMOUNTS else row[name] or 0) for name in METRICS}


def with_payment_success_rate(frame):
    from pyspark.sql import functions as F

    succeeded = F.col("payment_count").cast("decimal(20,0)")
    settled = succeeded + F.col("failed_payment_count").cast("decimal(20,0)")
    percentage = F.lit(100).cast("decimal(3,0)")
    rate = (succeeded * percentage / settled).cast("decimal(7,4)")
    return frame.withColumn("payment_success_rate_pct", F.when(settled > 0, rate))


def daily_platform(daily):
    from pyspark.sql import functions as F

    known = F.col("merchant_id") != UNKNOWN_MERCHANT
    overview = daily.groupBy("business_date").agg(
        *metric_sums(),
        F.count(F.when(known & (F.col("payment_count") > 0), 1)).alias("active_merchant_count"),
        F.sum(F.when(~known, F.col("transaction_count")).otherwise(0)).alias("unattributed_transaction_count"),
    )
    return with_payment_success_rate(overview)


def monthly_merchant(known_daily):
    from pyspark.sql import Window, functions as F

    monthly = (known_daily.withColumn("business_month", F.trunc("business_date", "month"))
               .groupBy("business_month", "merchant_id")
               .agg(*metric_sums(), F.count(F.when(F.col("payment_count") > 0, 1)).alias("payment_days")))
    eligible = (F.col("payment_count") > 0) | (F.col("refund_count") > 0)
    order = Window.partitionBy("business_month").orderBy(F.when(eligible, F.col("net_amount_vnd")).desc_nulls_last())
    return (with_payment_success_rate(monthly)
            .withColumn("net_rank", F.when(eligible, F.dense_rank().over(order))))


def write_mart(spark, output, target: str, properties: dict) -> int | None:
    from pyspark.sql import functions as F

    if spark.catalog.tableExists(target):
        output.writeTo(target).overwrite(F.lit(True))
    else:
        writer = output.writeTo(target).using("iceberg")
        for key, value in properties.items():
            writer = writer.tableProperty(key, value)
        writer.create()
    snapshot = current_snapshot_id(spark, target)
    published = (spark.read.option("snapshot-id", str(snapshot)).table(target)
                 if snapshot is not None else spark.table(target).limit(0))
    if (published.select(*output.columns).exceptAll(output).limit(1).count()
            or output.exceptAll(published.select(*output.columns)).limit(1).count()):
        raise ValueError(f"{target} snapshot differs from verified output")
    return snapshot


def materialize(spark, *, gold_namespace="gold") -> dict:
    """Each mart is an independent Iceberg commit with the same pinned input."""
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    namespace = identifier(gold_namespace)
    source = f"lakehouse.{namespace}.{MART}"
    ledger = f"lakehouse.{namespace}._materializations"
    run = completed_daily_run(spark, source, ledger)
    source_snapshot = run["output_snapshot_id"]
    lineage = json.dumps({source: source_snapshot}, sort_keys=True)
    fingerprint = hashlib.sha256(json.dumps(
        [VERSION, lineage, run["input_id"]], separators=(",", ":")).encode()).hexdigest()
    properties = {key: value for key, value in table_properties(spark, source).items() if key in BINDING_KEYS}
    properties.update({"gold.pipeline_version": VERSION, "gold.source_table": source})
    targets = {name: f"lakehouse.{namespace}.{name}" for name in (DAILY_PLATFORM, MONTHLY_MERCHANT)}
    for target in targets.values():
        if spark.catalog.tableExists(target):
            actual = table_properties(spark, target)
            if any(actual.get(key) != value for key, value in properties.items()):
                raise ValueError(f"{target} belongs to another source or pipeline")

    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    daily = None
    outputs = {}
    try:
        daily = (spark.read.option("snapshot-id", str(source_snapshot)).table(source)
                 if source_snapshot is not None else spark.table(source).limit(0))
        daily = daily.persist(StorageLevel.MEMORY_AND_DISK)
        if (daily.count() != run["row_count"]
                or daily.where(~F.col("_input_id").eqNullSafe(F.lit(run["input_id"]))).limit(1).count()):
            raise ValueError("daily Gold snapshot does not match its completed run")
        invalid_keys = F.col("business_date").isNull() | F.col("merchant_id").isNull()
        if (daily.where(invalid_keys).limit(1).count()
                or daily.groupBy("business_date", "merchant_id").count().where("count > 1").limit(1).count()):
            raise ValueError("daily Gold snapshot violates merchant/date grain")
        daily_totals = total_metrics(daily)
        reconciliation = json.loads(run["reconciliation"])
        if not reconciliation.get("ok") or reconciliation.get("totals") != daily_totals:
            raise ValueError("daily Gold ledger totals do not match the recorded snapshot")

        known_daily = daily.where(F.col("merchant_id") != UNKNOWN_MERCHANT)
        expected_totals = {DAILY_PLATFORM: daily_totals, MONTHLY_MERCHANT: total_metrics(known_daily)}
        mart_summaries = {}
        for name, mart in ((DAILY_PLATFORM, daily_platform(daily)),
                           (MONTHLY_MERCHANT, monthly_merchant(known_daily))):
            output = (mart.withColumn("_input_id", F.lit(fingerprint))
                      .withColumn("_source_snapshot_id", F.lit(source_snapshot).cast("long"))
                      .persist(StorageLevel.MEMORY_AND_DISK))
            outputs[name] = output
            row_count = output.count()
            totals = total_metrics(output)
            if totals != expected_totals[name]:
                raise ValueError(f"{name} totals do not reconcile to daily Gold")
            mart_summaries[name] = {"row_count": row_count, "totals": totals}

        records = {}
        for name, output in outputs.items():
            target = targets[name]
            snapshot = write_mart(spark, output, target, properties)
            report = {"ok": True, "source_run_id": run["run_id"], "totals": mart_summaries[name]["totals"]}
            record = dict(run_id=str(uuid.uuid4()), target_table=target, input_id=fingerprint,
                          source_snapshots=lineage, output_snapshot_id=snapshot,
                          row_count=mart_summaries[name]["row_count"], reconciliation=json.dumps(report),
                          completed_at=datetime.now(timezone.utc), pipeline_version=VERSION)
            write_run(spark, ledger, record)
            records[name] = {**record, "reconciliation": report}
        return {"source_snapshot_id": source_snapshot, "source_run_id": run["run_id"], "tables": records}
    finally:
        for output in outputs.values():
            output.unpersist()
        if daily is not None:
            daily.unpersist()
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold-namespace", default="gold")
    args = parser.parse_args(argv)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("bank-sim-gold-rollups").getOrCreate()
    try:
        print(json.dumps(materialize(spark, gold_namespace=args.gold_namespace), default=str))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
