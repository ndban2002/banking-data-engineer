import json
import os

import dagster as dg

from .spark import SparkJobs, silver_batches, snapshot_id, snapshot_map

BRONZE_TABLES = ("customers", "merchants", "accounts", "transactions")
SILVER_TABLES = ("dim_customers", "dim_merchants", "dim_accounts", "fact_transactions")
BRONZE_ASSETS = [dg.AssetKey(["bronze", table]) for table in BRONZE_TABLES]
SILVER_ASSETS = [dg.AssetKey(["silver", table]) for table in SILVER_TABLES]
DAILY_MERCHANT = dg.AssetKey(["gold", "daily_merchant_performance"])
ROLLUP_ASSETS = [dg.AssetKey(["gold", table])
                 for table in ("daily_platform_performance", "monthly_merchant_performance")]
MONTHLY_TOP = dg.AssetKey(["gold", "monthly_top_transactions"])


@dg.multi_asset(
    specs=[dg.AssetSpec(key, group_name="bronze", kinds={"spark", "iceberg"}) for key in BRONZE_ASSETS],
    description="Load four PostgreSQL tables sequentially through the shared Bronze manifest.",
)
def load_bronze(context: dg.AssetExecutionContext, spark: SparkJobs):
    for key in BRONZE_ASSETS:
        table = key.path[-1]
        mode = "full" if table in ("customers", "merchants") else spark.load_mode
        report = spark.submit(
            context, f"load_{table}_bronze.py", "--namespace", spark.namespace("bronze"),
            "--mode", mode, "--overlap-minutes", str(spark.overlap_minutes),
        )
        yield dg.MaterializeResult(asset_key=key, metadata={
            "table": spark.table("bronze", table),
            "batch_id": report["batch_id"],
            "mode": mode,
            "row_count": report["row_count"],
            "snapshot_id": snapshot_id(report["snapshot_id"]),
            "watermark": report["watermark"],
        })


@dg.multi_asset(
    specs=[
        dg.AssetSpec(silver, deps=[bronze], group_name="silver", kinds={"spark", "iceberg"})
        for bronze, silver in zip(BRONZE_ASSETS, SILVER_ASSETS, strict=True)
    ],
    description="Process accepted Bronze batches in one Spark job with a shared Silver checkpoint.",
)
def load_silver(context: dg.AssetExecutionContext, spark: SparkJobs):
    report = spark.submit(
        context, "silver_loader.py", "--table", "all", "--bronze-namespace", spark.namespace("bronze"),
        "--silver-namespace", spark.namespace("silver"),
    )
    batches = silver_batches(report["batches"])
    for key in SILVER_ASSETS:
        target = spark.table("silver", key.path[-1])
        table_batches = [batch for batch in batches if batch["silver_table"] == target]
        processed = [batch for batch in table_batches if not batch["skipped"]]
        yield dg.MaterializeResult(asset_key=key, metadata={
            "table": target,
            "snapshot_id": snapshot_id(report["tables"][target]["snapshot_id"]),
            "input_rows": sum(batch["input_rows"] for batch in processed),
            "upsert_rows": sum(batch["upsert_rows"] for batch in processed),
            "quarantined_rows": sum(batch["quarantined_rows"] for batch in table_batches),
            "skipped_batches": len(table_batches) - len(processed),
            "processed_batches": dg.MetadataValue.json(processed),
        })


@dg.asset_check(
    asset=SILVER_ASSETS[-1], additional_deps=SILVER_ASSETS[:-1], blocking=True,
    description="Check the four Silver snapshots, quarantine, and batch checkpoints before Gold.",
)
def silver_quality(context: dg.AssetCheckExecutionContext, spark: SparkJobs) -> dg.AssetCheckResult:
    report = spark.submit(
        context, "silver_quality.py", "--namespace", spark.namespace("silver"),
        "--require-processed", allow_quality_failure=True,
    )
    processing = report["processing_violations"]
    violations = report["violations"]
    snapshots = report["snapshots"]
    if (report["namespace"] != spark.namespace("silver")
            or set(snapshots) != {spark.table("silver", table) for table in SILVER_TABLES}
            or len(violations) != 16
            or set(processing) != {"quarantined_rows", "uncheckpointed_batches"}):
        raise dg.Failure("Silver quality returned an incomplete report or a different namespace.")
    return dg.AssetCheckResult(
        passed=report["ok"] is True and not any(violations.values()) and not any(processing.values()),
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={
            "snapshots": dg.MetadataValue.json(snapshot_map(snapshots)),
            "violations": dg.MetadataValue.json(violations),
            "pending_transactions": report["pending_transactions"],
            **processing,
        },
    )


def gold_metadata(record: dict) -> dict:
    sources = json.loads(record["source_snapshots"])
    return {
        "table": record["target_table"],
        "run_id": record["run_id"],
        "input_id": record["input_id"],
        "source_snapshots": dg.MetadataValue.json(snapshot_map(sources)),
        "snapshot_id": snapshot_id(record["output_snapshot_id"]),
        "row_count": record["row_count"],
        "reconciliation": dg.MetadataValue.json(record["reconciliation"]),
    }


def checked_snapshot_args(context: dg.AssetExecutionContext) -> list[str]:
    # A from-failure retry or a direct asset run may have no evaluation in this run.
    # Spark always pins and validates its own inputs before committing Gold.
    events = context.instance.get_records_for_run(
        context.run.run_id, of_type=dg.DagsterEventType.ASSET_CHECK_EVALUATION,
    )
    for event in events.records:
        check = event.event_log_entry.asset_check_evaluation
        if check.asset_key == SILVER_ASSETS[-1] and check.check_name == "silver_quality":
            snapshots = {table: None if value is None else int(value)
                         for table, value in check.metadata["snapshots"].value.items()}
            return ["--source-snapshots", json.dumps(snapshots, sort_keys=True)]
    return []


@dg.asset(key=DAILY_MERCHANT, deps=SILVER_ASSETS, group_name="gold", kinds={"spark", "iceberg"})
def daily_merchant_performance(context: dg.AssetExecutionContext, spark: SparkJobs) -> dg.MaterializeResult:
    """Reconcile and publish the merchant × Vietnam business day mart on Iceberg."""
    record = spark.submit(
        context, "gold_daily_merchant.py", "--silver-namespace", spark.namespace("silver"),
        "--gold-namespace", spark.namespace("gold"), *checked_snapshot_args(context),
    )
    return dg.MaterializeResult(metadata=gold_metadata(record))


@dg.multi_asset(
    specs=[dg.AssetSpec(key, deps=[DAILY_MERCHANT], group_name="gold", kinds={"spark", "iceberg"})
           for key in ROLLUP_ASSETS],
    description="Publish two rollups from one completed daily merchant snapshot.",
)
def gold_rollups(context: dg.AssetExecutionContext, spark: SparkJobs):
    report = spark.submit(context, "gold_rollups.py", "--gold-namespace", spark.namespace("gold"))
    for key in ROLLUP_ASSETS:
        yield dg.MaterializeResult(asset_key=key, metadata=gold_metadata(report["tables"][key.path[-1]]))


@dg.asset(key=MONTHLY_TOP, deps=[DAILY_MERCHANT, *SILVER_ASSETS[-2:]],
          group_name="gold", kinds={"spark", "iceberg"})
def monthly_top_transactions(context: dg.AssetExecutionContext, spark: SparkJobs) -> dg.MaterializeResult:
    """Rank transactions from the Silver snapshots recorded by daily Gold."""
    record = spark.submit(context, "gold_monthly_top_transactions.py", "--gold-namespace", spark.namespace("gold"))
    return dg.MaterializeResult(metadata=gold_metadata(record))


banking_daily = dg.define_asset_job(
    "banking_daily", selection=dg.AssetSelection.assets(load_bronze, load_silver, daily_merchant_performance),
    description="Read PostgreSQL, load Bronze and Silver, then validate Silver and publish daily Gold.",
)
banking_rebuild = dg.define_asset_job(
    "banking_rebuild", selection=dg.AssetSelection.assets(load_silver, daily_merchant_performance),
    description="Resume using accepted Bronze batches already in Iceberg.",
)
banking_refresh_gold = dg.define_asset_job(
    "banking_refresh_gold",
    selection=dg.AssetSelection.assets(daily_merchant_performance) | dg.AssetSelection.checks(silver_quality),
    description="Validate current Silver snapshots and rebuild daily Gold using the same saved inputs.",
)
banking_gold_marts = dg.define_asset_job(
    "banking_gold_marts", selection=dg.AssetSelection.assets(gold_rollups, monthly_top_transactions),
    description="Build the remaining Gold marts using the latest completed daily Gold ledger entry.",
)
banking_batch = dg.define_asset_job(
    "banking_batch", selection=dg.AssetSelection.all(),
    description="Run PostgreSQL extraction, Silver validation and all four Gold marts.",
)
banking_schedule = dg.ScheduleDefinition(
    name="banking_batch_daily", job=banking_batch, cron_schedule="0 2 * * *",
    execution_timezone="Asia/Ho_Chi_Minh", default_status=dg.DefaultScheduleStatus.STOPPED,
)

defs = dg.Definitions(
    assets=[load_bronze, load_silver, daily_merchant_performance, gold_rollups, monthly_top_transactions],
    asset_checks=[silver_quality],
    jobs=[banking_daily, banking_rebuild, banking_refresh_gold, banking_gold_marts, banking_batch],
    schedules=[banking_schedule],
    resources={"spark": SparkJobs(
        namespace_suffix=os.environ.get("BANKING_NAMESPACE_SUFFIX", "m4"),
        load_mode=os.environ.get("BANKING_LOAD_MODE", "full"),
        overlap_minutes=int(os.environ.get("BANKING_OVERLAP_MINUTES", "5")),
    )},
    executor=dg.in_process_executor,
)
