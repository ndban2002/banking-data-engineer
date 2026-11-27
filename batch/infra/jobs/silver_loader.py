"""Consume accepted Bronze batches into Iceberg Silver current-state tables.

Run one writer per Silver namespace. Output, quarantine and checkpoint are
separate commits; replay uses immutable Bronze snapshots and guarded MERGEs.
No source credentials or live PostgreSQL reads are needed.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json

from bronze_loader import SOURCE_PROPERTY, current_snapshot_id, identifier, parse_utc_timestamp
from job_reporting import add_report_argument, emit_report
from silver_transform import SILVER_SPECS, prepare_batch


PIPELINE_VERSION = "current_state_v1"
MANIFEST_FIELDS = (
    "batch_id", "source_key", "target_table", "mode", "row_count",
    "observed_at", "completed_at", "snapshot_id", "watermark",
)
CHECKPOINT_SCHEMA = """bronze_table STRING, batch_id STRING, source_key STRING,
    manifest_fingerprint STRING, bronze_snapshot_id LONG, silver_table STRING,
    input_rows LONG, upsert_rows LONG, quarantined_rows LONG,
    processed_at TIMESTAMP, pipeline_version STRING"""
QUARANTINE_SCHEMA = """rejection_id STRING, source_table STRING, source_key STRING,
    batch_id STRING, bronze_snapshot_id LONG, record_id STRING,
    reason_codes ARRAY<STRING>, raw_record STRING, observed_at TIMESTAMP"""


def manifest_fingerprint(manifest: dict) -> str:
    """Detect reuse of a batch ID for a different supposedly immutable input."""
    values = {key: manifest[key] for key in MANIFEST_FIELDS}
    encoded = json.dumps(values, default=lambda x: x.isoformat(), sort_keys=True,
                         separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def validate_manifest(manifest: dict, table: str, bronze_namespace: str) -> list[str]:
    expected_table = f"lakehouse.{bronze_namespace}.{table}"
    if manifest["target_table"] != expected_table:
        raise ValueError("manifest target does not match requested Bronze table")
    try:
        source = json.loads(manifest["source_key"])
    except (TypeError, ValueError) as exc:
        raise ValueError("manifest has an invalid source identity") from exc
    if (not isinstance(source, list) or len(source) != 4
            or not all(isinstance(x, str) and x for x in source) or source[3] != table):
        raise ValueError("manifest source identity does not match requested table")
    if not isinstance(manifest["batch_id"], str) or not manifest["batch_id"]:
        raise ValueError("manifest requires a batch ID")
    if not isinstance(manifest["row_count"], int) or manifest["row_count"] < 0:
        raise ValueError("manifest has an invalid row count")
    if manifest["mode"] not in ("full", "incremental"):
        raise ValueError("manifest has an invalid extraction mode")
    if table in ("customers", "merchants") and manifest["mode"] != "full":
        raise ValueError("dimensions require full-snapshot manifests")
    if manifest["observed_at"] is None or manifest["completed_at"] is None:
        raise ValueError("manifest requires observation and completion timestamps")
    if manifest["completed_at"] < manifest["observed_at"]:
        raise ValueError("manifest completion predates observation")
    if manifest["row_count"] and manifest["snapshot_id"] is None:
        raise ValueError("nonempty accepted batch requires a Bronze snapshot ID")
    return source


def table_properties(spark, table: str) -> dict:
    return {row["key"]: row["value"]
            for row in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}


def ensure_table(spark, table: str, schema, properties: dict) -> None:
    if spark.catalog.tableExists(table):
        actual = table_properties(spark, table)
        if any(actual.get(key) != value for key, value in properties.items()):
            raise ValueError(f"{table} belongs to another source or pipeline; use a new Silver namespace")
        return
    writer = spark.createDataFrame([], schema).writeTo(table).using("iceberg")
    for key, value in properties.items():
        writer = writer.tableProperty(key, value)
    writer.create()


def read_snapshot(spark, table: str):
    """Freeze an existing Silver table before computing this batch's changes."""
    snapshot = current_snapshot_id(spark, table)
    if snapshot is None:
        return spark.table(table).limit(0)
    return spark.read.option("snapshot-id", str(snapshot)).table(table)


def read_accepted_batch(spark, manifest: dict, source: list[str]):
    from pyspark.sql import functions as F

    table = manifest["target_table"]
    if not spark.catalog.tableExists(table):
        raise ValueError("accepted Bronze table is missing; restore its snapshot before retrying")
    if table_properties(spark, table).get(SOURCE_PROPERTY) != manifest["source_key"]:
        raise ValueError("Bronze table source binding differs from accepted manifest")
    if manifest["snapshot_id"] is None:
        # Empty initial CTAS may not create a snapshot; never read current rows.
        frame = spark.table(table).limit(0)
    else:
        frame = spark.read.option("snapshot-id", str(manifest["snapshot_id"])).table(table)
        frame = frame.where(F.col("_batch_id") == manifest["batch_id"])
    required = {"_source_system", "_source_schema", "_source_table", "_batch_id",
                "_ingested_at", "_ingestion_date"}
    if not required.issubset(frame.columns):
        raise ValueError("Bronze batch has incomplete provenance metadata")
    provenance = (F.col("_source_system").eqNullSafe(source[0])
                  & F.col("_source_schema").eqNullSafe(source[2])
                  & F.col("_source_table").eqNullSafe(source[3])
                  & F.col("_ingested_at").eqNullSafe(F.lit(manifest["observed_at"]))
                  & F.col("_ingestion_date").eqNullSafe(F.to_date("_ingested_at")))
    if frame.where(~provenance).limit(1).count():
        raise ValueError("Bronze row provenance does not match its accepted manifest")
    if frame.count() != manifest["row_count"]:
        raise ValueError("Bronze snapshot row count differs from its accepted manifest")
    return frame


def merge_updates(spark, target: str, updates, key: str) -> None:
    # Target and column names come only from validated namespaces/fixed specs.
    updates.createOrReplaceTempView("silver_updates")
    spark.sql(f"""MERGE INTO {target} t USING silver_updates s ON t.{key} = s.{key}
        WHEN MATCHED AND s._version_at > t._version_at THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")


def merge_quarantine(spark, target: str, rejected) -> None:
    rejected.createOrReplaceTempView("silver_rejections")
    spark.sql(f"""MERGE INTO {target} t USING silver_rejections s
        ON t.rejection_id = s.rejection_id
        WHEN NOT MATCHED THEN INSERT *""")


def write_checkpoint(spark, checkpoint_table: str, record: dict) -> None:
    fields = ("bronze_table", "batch_id", "source_key", "manifest_fingerprint",
              "bronze_snapshot_id", "silver_table", "input_rows", "upsert_rows",
              "quarantined_rows", "processed_at", "pipeline_version")
    spark.createDataFrame([tuple(record[key] for key in fields)], CHECKPOINT_SCHEMA
                         ).createOrReplaceTempView("silver_checkpoint")
    spark.sql(f"""MERGE INTO {checkpoint_table} t USING silver_checkpoint s
        ON t.bronze_table = s.bronze_table AND t.batch_id = s.batch_id
        WHEN NOT MATCHED THEN INSERT *""")


def process_table(spark, table: str, *, bronze_namespace: str = "bronze",
                  silver_namespace: str = "silver", batch_id: str | None = None,
                  replay: bool = False) -> list[dict]:
    """Process each accepted batch once, or explicitly replay selected batches.

    Quarantined rows are durably recorded and the batch is consumed; callers
    must inspect rejected counts before using the result for Gold/publication.
    Unexpected exceptions leave no processing checkpoint and are retryable.
    """
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    if table not in SILVER_SPECS:
        raise ValueError("unknown Silver source table")
    identifier(bronze_namespace)
    identifier(silver_namespace)
    if bronze_namespace == silver_namespace:
        raise ValueError("Bronze and Silver must use different namespaces")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    bronze_table = f"lakehouse.{bronze_namespace}.{table}"
    manifests_table = f"lakehouse.{bronze_namespace}._ingestion_batches"
    base = f"lakehouse.{silver_namespace}"
    spec = SILVER_SPECS[table]
    target = f"{base}.{spec.target_name}"
    checkpoint_table, quarantine_table = f"{base}._processed_batches", f"{base}._quarantine"
    if not spark.catalog.tableExists(manifests_table):
        raise ValueError("no accepted Bronze manifest table; run Bronze ingestion first")
    # Pin the manifest snapshot too, so arrivals during a run wait until next run.
    manifests = read_snapshot(spark, manifests_table).where(F.col("target_table") == bronze_table)
    if batch_id is not None:
        manifests = manifests.where(F.col("batch_id") == batch_id)
    manifests = manifests.select(*[
        F.col(key).cast("string").alias(key)
        if key in ("observed_at", "completed_at", "watermark") else F.col(key)
        for key in MANIFEST_FIELDS
    ]).orderBy("observed_at", "completed_at", "batch_id")
    # Only the small control ledger is collected, never source/business rows.
    batches = [row.asDict() for row in manifests.collect()]
    if not batches:
        raise ValueError("no accepted batches match this source table/batch ID")
    seen = set()
    dataset = None
    for manifest in batches:
        for key in ("observed_at", "completed_at", "watermark"):
            manifest[key] = parse_utc_timestamp(manifest[key])
        source = validate_manifest(manifest, table, bronze_namespace)
        if manifest["batch_id"] in seen:
            raise ValueError("duplicate batch IDs in Bronze manifest; resolve before processing")
        seen.add(manifest["batch_id"])
        if dataset is not None and dataset != source[:3]:
            raise ValueError("Bronze manifests contain multiple source datasets")
        dataset = source[:3]
    properties = {"silver.source_namespace": bronze_namespace,
                  "silver.source_dataset": json.dumps(dataset, separators=(",", ":")),
                  "silver.pipeline_version": PIPELINE_VERSION}
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {base}")
    ensure_table(spark, checkpoint_table, CHECKPOINT_SCHEMA, properties)
    ensure_table(spark, quarantine_table, QUARANTINE_SCHEMA, properties)
    completed = {}
    for row in spark.table(checkpoint_table).where(F.col("bronze_table") == bronze_table).collect():
        item = row.asDict()
        if item["batch_id"] in completed:
            raise ValueError("duplicate Silver checkpoints; serialize writers")
        completed[item["batch_id"]] = item
    if completed and not spark.catalog.tableExists(target):
        raise ValueError("Silver target missing but checkpoints exist; rebuild in a new Silver namespace")
    target_properties = {**properties, "silver.source_key": batches[0]["source_key"]}
    if spark.catalog.tableExists(target):
        actual = table_properties(spark, target)
        if any(actual.get(k) != v for k, v in target_properties.items()):
            raise ValueError("Silver target binding differs from source; use a new namespace")
    results = []
    for manifest in batches:
        fingerprint = manifest_fingerprint(manifest)
        previous = completed.get(manifest["batch_id"])
        if previous is not None:
            if previous["manifest_fingerprint"] != fingerprint:
                raise ValueError("accepted manifest changed after Silver processing")
            if not replay:
                results.append({**previous, "skipped": True})
                continue
        source = json.loads(manifest["source_key"])
        frame = read_accepted_batch(spark, manifest, source)
        current = read_snapshot(spark, target) if spark.catalog.tableExists(target) else None
        updates = rejected = None
        try:
            updates, rejected = prepare_batch(frame, current, table, manifest)
            updates = updates.persist(StorageLevel.MEMORY_AND_DISK)
            rejected = rejected.persist(StorageLevel.MEMORY_AND_DISK)
            # Materialize both before modifying any destination.
            update_count, reject_count = updates.count(), rejected.count()
            ensure_table(spark, target, updates.schema, target_properties)
            if update_count:
                merge_updates(spark, target, updates, spec.key)
            if reject_count:
                merge_quarantine(spark, quarantine_table, rejected)
            record = dict(bronze_table=bronze_table, batch_id=manifest["batch_id"],
                          source_key=manifest["source_key"], manifest_fingerprint=fingerprint,
                          bronze_snapshot_id=manifest["snapshot_id"], silver_table=target,
                          input_rows=manifest["row_count"], upsert_rows=update_count,
                          quarantined_rows=reject_count, processed_at=datetime.now(timezone.utc),
                          pipeline_version=PIPELINE_VERSION)
            # Preserve the first completed result when explicitly replaying it.
            write_checkpoint(spark, checkpoint_table, record)
            results.append({**record, "skipped": False})
        finally:
            if updates is not None:
                updates.unpersist()
            if rejected is not None:
                rejected.unpersist()
    return results


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", choices=(*SILVER_SPECS, "all"), default="all")
    parser.add_argument("--bronze-namespace", default="bronze")
    parser.add_argument("--silver-namespace", default="silver")
    parser.add_argument("--batch-id", help="Process one accepted batch of the selected table")
    parser.add_argument("--replay", action="store_true", help="Revisit already processed batches")
    add_report_argument(parser)
    args = parser.parse_args(argv)
    try:
        identifier(args.bronze_namespace)
        identifier(args.silver_namespace)
        if args.bronze_namespace == args.silver_namespace:
            raise ValueError("Bronze and Silver namespaces must differ")
        if args.batch_id is not None and args.table == "all":
            raise ValueError("--batch-id requires --table with a single table")
    except ValueError as exc:
        parser.error(str(exc))
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("bank-sim-bronze-to-silver").getOrCreate()
    try:
        tables = list(SILVER_SPECS) if args.table == "all" else [args.table]
        results = []
        for table in tables:
            results.extend(process_table(spark, table, bronze_namespace=args.bronze_namespace,
                                         silver_namespace=args.silver_namespace,
                                         batch_id=args.batch_id, replay=args.replay))
        report = {"batches": results}
        if args.orchestration_report:
            targets = [f"lakehouse.{args.silver_namespace}.{SILVER_SPECS[table].target_name}" for table in tables]
            report["tables"] = {target: {"snapshot_id": current_snapshot_id(spark, target)} for target in targets}
        emit_report(report, orchestration=args.orchestration_report)
        # A skipped historical rejection remains in the audit ledger, but is
        # not a new failure of this run. Publication must review quarantine
        # separately; exit 0 alone is not approval for Gold.
        return 1 if any(row["quarantined_rows"] and not row["skipped"] for row in results) else 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
