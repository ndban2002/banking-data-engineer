"""Rank the ten largest successful transactions in each Vietnam business month."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import uuid

from bronze_loader import identifier
from gold_daily_merchant import MART as DAILY_MART, input_id, merchant_events, write_run
from gold_rollups import completed_daily_run, write_mart
from silver_loader import table_properties
from silver_quality import TABLES


MART = "monthly_top_transactions"
VERSION = "monthly_top_transactions_v1"
TOP_N = 10


def build_mart(events):
    from pyspark.sql import Window, functions as F

    order = Window.partitionBy("business_month").orderBy(
        F.col("amount_vnd").desc(), F.col("occurred_at").asc(), F.col("transaction_id").asc()
    )
    return (events.where(F.col("status") == "SUCCEEDED")
            .withColumn("business_month", F.trunc("business_date", "month"))
            .withColumn("transaction_rank", F.row_number().over(order))
            .where(F.col("transaction_rank") <= TOP_N)
            .select("business_month", "transaction_rank", "transaction_id", "transaction_type",
                    "merchant_id", "business_date", "occurred_at", "amount_vnd"))


def check_success_totals(events, run: dict) -> dict:
    """Verify detail covers the successful rows from the same completed run."""
    from pyspark.sql import functions as F

    aggregates = (events.where(F.col("status") == "SUCCEEDED").groupBy("transaction_type")
                  .agg(F.count("*").alias("count"), F.sum(F.col("amount_vnd").cast("decimal(38,0)")).alias("amount")))
    by_type = {row["transaction_type"]: row for row in aggregates.collect()}
    totals = {}
    for kind in ("PAYMENT", "REFUND"):
        row = by_type.get(kind)
        totals[f"{kind.lower()}_count"] = row["count"] if row else 0
        totals[f"{kind.lower()}_amount_vnd"] = str(row["amount"]) if row else "0"
    report = json.loads(run["reconciliation"])
    if not report.get("ok") or any(report.get("totals", {}).get(key) != value for key, value in totals.items()):
        raise ValueError("successful Silver totals do not match the completed daily Gold run")
    return totals


def materialize(spark, *, gold_namespace="gold") -> dict:
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    namespace = identifier(gold_namespace)
    daily_table = f"lakehouse.{namespace}.{DAILY_MART}"
    ledger = f"lakehouse.{namespace}._materializations"
    target = f"lakehouse.{namespace}.{MART}"
    run = completed_daily_run(spark, daily_table, ledger)
    daily_binding = table_properties(spark, daily_table)
    silver_namespace = identifier(daily_binding["gold.source_namespace"])
    dataset = daily_binding["gold.source_dataset"]
    snapshots = json.loads(run["source_snapshots"])
    tables = {source: f"lakehouse.{silver_namespace}.{name}" for source, (name, _) in TABLES.items()}
    if set(snapshots) != set(tables.values()) or input_id(snapshots) != run["input_id"]:
        raise ValueError("completed daily Gold run has inconsistent Silver snapshots")

    frames, detail_snapshots = {}, {}
    for source in ("accounts", "transactions"):
        table = tables[source]
        binding = table_properties(spark, table)
        if binding.get("silver.source_dataset") != dataset:
            raise ValueError(f"{table} belongs to another source dataset")
        snapshot = snapshots[table]
        detail_snapshots[table] = snapshot
        frames[source] = (spark.read.option("snapshot-id", str(snapshot)).table(table)
                          if snapshot is not None else spark.table(table).limit(0))

    properties = {"gold.source_namespace": silver_namespace, "gold.source_dataset": dataset,
                  "gold.pipeline_version": VERSION, "gold.source_table": tables["transactions"]}
    if spark.catalog.tableExists(target):
        actual = table_properties(spark, target)
        if any(actual.get(key) != value for key, value in properties.items()):
            raise ValueError(f"{target} belongs to another source or pipeline")

    previous_timezone = spark.conf.get("spark.sql.session.timeZone")
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    events = output = None
    try:
        frames["transactions"] = frames["transactions"].where(F.col("status") == "SUCCEEDED")
        events = merchant_events(spark, frames).persist(StorageLevel.MEMORY_AND_DISK)
        totals = check_success_totals(events, run)
        lineage = json.dumps(detail_snapshots, sort_keys=True)
        fingerprint = hashlib.sha256(json.dumps([VERSION, run["input_id"]]).encode()).hexdigest()
        output = (build_mart(events).withColumn("_input_id", F.lit(fingerprint))
                  .withColumn("_source_snapshots", F.lit(lineage)).persist(StorageLevel.MEMORY_AND_DISK))
        summary = output.agg(F.count("*").alias("row_count"),
                             F.countDistinct("business_month").alias("month_count")).first()
        snapshot = write_mart(spark, output, target, properties)
        report = {"ok": True, "source_run_id": run["run_id"], "month_count": summary["month_count"],
                  "eligible_transaction_totals": totals}
        record = dict(run_id=str(uuid.uuid4()), target_table=target, input_id=fingerprint,
                      source_snapshots=lineage, output_snapshot_id=snapshot,
                      row_count=summary["row_count"], reconciliation=json.dumps(report),
                      completed_at=datetime.now(timezone.utc), pipeline_version=VERSION)
        write_run(spark, ledger, record)
        return {**record, "reconciliation": report}
    finally:
        if output is not None:
            output.unpersist()
        if events is not None:
            events.unpersist()
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)
        spark.conf.set("spark.sql.session.timeZone", previous_timezone)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold-namespace", default="gold")
    args = parser.parse_args(argv)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("bank-sim-gold-monthly-top-transactions").getOrCreate()
    try:
        print(json.dumps(materialize(spark, gold_namespace=args.gold_namespace), default=str))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
