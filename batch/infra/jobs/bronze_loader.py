"""Shared PostgreSQL -> Iceberg Bronze ingestion (one writer per namespace).

YAML mappings are design specifications. TABLES implements the Bronze source
inventory; business transformations and snapshot comparison belong to Silver.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import json
import os
import re
from urllib.parse import quote
import uuid

SOURCE_SYSTEM = "banking-simulator"
SOURCE_PROPERTY = "bank_sim.source_key"


@dataclass(frozen=True)
class TableSpec:
    name: str
    columns: tuple[str, ...]
    watermark_column: str | None = None


TABLES = {
    "customers": TableSpec("customers", ("id", "name", "region", "created_at")),
    "merchants": TableSpec("merchants", ("id", "name", "mcc", "region", "channel", "created_at")),
    "accounts": TableSpec("accounts", (
        "id", "customer_id", "merchant_id", "currency", "opening_balance", "balance",
        "created_at", "updated_at",
    ), "updated_at"),
    "transactions": TableSpec("transactions", (
        "id", "request_id", "kind", "source_account_id", "destination_account_id",
        "amount", "currency", "channel", "status", "error_code", "original_transaction_id",
        "requested_decline", "occurred_at", "created_at", "updated_at",
    ), "updated_at"),
}


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", value):
        raise ValueError("schema/namespace must be a simple SQL identifier (at most 63 characters)")
    return value


def required_env(name: str) -> str:
    value = os.environ.get(name, "")
    if not value.strip() or value == "replace_me":
        raise ValueError(f"Set {name} in batch/infra/.env before running the loader")
    return value


@dataclass(frozen=True)
class SourceSettings:
    host: str
    port: str
    database: str
    schema: str
    user: str = field(repr=False)
    password: str = field(repr=False)

    def __post_init__(self):
        identifier(self.schema)
        if not re.fullmatch(r"[A-Za-z0-9_.-]+|\[[A-Fa-f0-9:]+\]", self.host):
            raise ValueError("SOURCE_PG_HOST must be a hostname, IPv4 address or bracketed IPv6 address")
        if not str(self.port).isdigit() or not 1 <= int(self.port) <= 65535:
            raise ValueError("SOURCE_PG_PORT must be between 1 and 65535")
        if not self.database:
            raise ValueError("SOURCE_PG_DATABASE must not be empty")

    @classmethod
    def from_env(cls):
        return cls(**{key: required_env(f"SOURCE_PG_{key.upper()}")
                      for key in ("host", "port", "database", "schema", "user", "password")})

    @property
    def jdbc_url(self):
        return f"jdbc:postgresql://{self.host}:{self.port}/{quote(self.database, safe='')}"

    def source_key(self, table: str) -> str:
        # Host/port identify transport (including changing SSH tunnels), not source identity.
        return json.dumps([SOURCE_SYSTEM, self.database, self.schema, table], separators=(",", ":"))


def parse_args(table: str, argv=None):
    spec = TABLES[table]
    parser = argparse.ArgumentParser(description=f"Append PostgreSQL {table} observations to Iceberg Bronze")
    parser.add_argument("--mode", choices=("full", "incremental"),
                        default="incremental" if spec.watermark_column else "full")
    parser.add_argument("--overlap-minutes", type=int, default=5)
    parser.add_argument("--namespace", default="bronze", help="Iceberg namespace in lakehouse (default: bronze)")
    args = parser.parse_args(argv)
    if args.overlap_minutes < 0 or args.overlap_minutes > 525600:
        parser.error("--overlap-minutes must be between 0 and 525600")
    if args.mode == "incremental" and spec.watermark_column is None:
        parser.error(f"{table} has no updated_at; use --mode full on every run")
    try:
        identifier(args.namespace)
    except ValueError as exc:
        parser.error(str(exc))
    return args


def parse_utc_timestamp(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value)
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def build_dbtable(settings: SourceSettings, spec: TableSpec,
                  previous_watermark: datetime | None, overlap_minutes: int) -> str:
    table = f'"{settings.schema}"."{spec.name}"'
    if previous_watermark is None:
        return table
    if spec.watermark_column is None:
        raise ValueError("snapshot-only sources cannot use a watermark")
    watermark = previous_watermark
    if watermark.tzinfo is None:
        watermark = watermark.replace(tzinfo=timezone.utc)
    watermark = watermark.astimezone(timezone.utc)
    overlap = min(timedelta(minutes=overlap_minutes), watermark - datetime.min.replace(tzinfo=timezone.utc))
    lower = (watermark - overlap).isoformat(sep=" ", timespec="microseconds")
    return (f'(SELECT * FROM {table} WHERE "{spec.watermark_column}" >= '
            f"TIMESTAMPTZ '{lower}' ORDER BY \"{spec.watermark_column}\", \"id\") AS changed_source")


def read_watermark(spark, state_table: str, source_key: str) -> datetime | None:
    from pyspark.sql import functions as F

    if not spark.catalog.tableExists(state_table):
        return None
    rows = (spark.table(state_table).where(F.col("source_table") == source_key)
            .select(F.col("watermark").cast("string").alias("watermark")).limit(2).collect())
    if len(rows) > 1:
        raise RuntimeError("duplicate ingestion state; serialize loaders before continuing")
    # Cast in Spark's UTC session; collecting TimestampType directly uses Python host TZ.
    return parse_utc_timestamp(rows[0]["watermark"]) if rows else None


def write_watermark(spark, state_table: str, source_key: str, watermark: datetime) -> None:
    state = spark.createDataFrame([(source_key, watermark, datetime.now(timezone.utc))],
                                  "source_table STRING, watermark TIMESTAMP, updated_at TIMESTAMP")
    state.createOrReplaceTempView("next_ingestion_state")
    spark.sql(f"""MERGE INTO {state_table} target USING next_ingestion_state source
        ON target.source_table = source.source_table
        WHEN MATCHED AND (target.watermark IS NULL OR source.watermark > target.watermark) THEN
            UPDATE SET watermark = source.watermark, updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (source_table, watermark, updated_at)
            VALUES (source.source_table, source.watermark, source.updated_at)""")


def validate_target_source(spark, target: str, settings: SourceSettings, spec: TableSpec) -> None:
    from pyspark.sql import functions as F

    if not spark.catalog.tableExists(target):
        return
    properties = {row["key"]: row["value"] for row in spark.sql(f"SHOW TBLPROPERTIES {target}").collect()}
    expected = settings.source_key(spec.name)
    bound = properties.get(SOURCE_PROPERTY)
    if bound is not None:
        if bound != expected:
            raise ValueError("target belongs to another source; use a separate --namespace")
        return
    # Legacy transactions recorded schema/system/table but no database. Adopting
    # one assumes the same database in .env; unknown database history cannot be inferred.
    existing = spark.table(target)
    required = {"_source_system", "_source_schema", "_source_table"}
    if not required.issubset(existing.columns):
        raise ValueError("existing target has no source provenance; use a separate --namespace")
    same_source = (F.col("_source_system").eqNullSafe(SOURCE_SYSTEM)
                   & F.col("_source_schema").eqNullSafe(settings.schema)
                   & F.col("_source_table").eqNullSafe(spec.name))
    if existing.where(~same_source).limit(1).count():
        raise ValueError("existing target contains another source; use a separate --namespace")
    escaped = expected.replace("\\", "\\\\").replace("'", "\\'")
    spark.sql(f"ALTER TABLE {target} SET TBLPROPERTIES ('{SOURCE_PROPERTY}' = '{escaped}')")


def read_source(spark, settings: SourceSettings, dbtable: str):
    return (spark.read.format("jdbc").option("url", settings.jdbc_url).option("dbtable", dbtable)
            .option("driver", "org.postgresql.Driver").option("user", settings.user)
            .option("password", settings.password).option("fetchsize", "10000")
            .option("numPartitions", "1").load())


def add_ingestion_metadata(source, settings, spec, batch_id, observed_at):
    from pyspark.sql import functions as F

    return (source.withColumn("_source_system", F.lit(SOURCE_SYSTEM))
            .withColumn("_source_schema", F.lit(settings.schema))
            .withColumn("_source_table", F.lit(spec.name))
            .withColumn("_ingested_at", F.lit(observed_at))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_ingestion_date", F.to_date(F.col("_ingested_at"))))


def append_bronze(bronze, target: str, source_key: str) -> None:
    if bronze.sparkSession.catalog.tableExists(target):
        bronze.writeTo(target).append()
    else:
        (bronze.writeTo(target).using("iceberg").partitionedBy("_ingestion_date")
         .tableProperty(SOURCE_PROPERTY, source_key).create())


def current_snapshot_id(spark, target: str) -> int | None:
    from pyspark.sql import functions as F

    row = spark.table(f"{target}.refs").where(F.col("name") == "main").select("snapshot_id").first()
    return row["snapshot_id"] if row else None


def summarize(frame, spec: TableSpec) -> dict:
    from pyspark.sql import functions as F

    aggregates = [F.count(F.lit(1)).alias("row_count"), F.count("id").alias("id_count")]
    if spec.watermark_column:
        aggregates.extend([F.max(spec.watermark_column).cast("string").alias("watermark"),
                           F.count(spec.watermark_column).alias("watermark_count")])
    row = frame.agg(*aggregates).first()
    if row["id_count"] != row["row_count"]:
        raise ValueError("source contains null IDs; batch cannot be accepted")
    if spec.watermark_column and row["watermark_count"] != row["row_count"]:
        raise ValueError("source contains null updated_at; batch cannot be checkpointed")
    return {"row_count": row["row_count"],
            "watermark": parse_utc_timestamp(row["watermark"]) if spec.watermark_column else None}


def write_manifest(spark, manifest_table: str, record: dict) -> None:
    schema = ("batch_id STRING, source_key STRING, target_table STRING, mode STRING, row_count LONG, "
              "observed_at TIMESTAMP, completed_at TIMESTAMP, snapshot_id LONG, watermark TIMESTAMP")
    fields = ("batch_id", "source_key", "target_table", "mode", "row_count", "observed_at",
              "completed_at", "snapshot_id", "watermark")
    spark.createDataFrame([tuple(record[key] for key in fields)], schema).writeTo(manifest_table).append()


def load_table(spark, settings: SourceSettings, spec: TableSpec, *, mode: str,
               overlap_minutes: int = 5, namespace: str = "bronze") -> dict:
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    identifier(namespace)
    if mode not in ("full", "incremental") or (mode == "incremental" and not spec.watermark_column):
        raise ValueError("this table requires full snapshots")
    if not 0 <= overlap_minutes <= 525600:
        raise ValueError("overlap_minutes must be between 0 and 525600")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    base = f"lakehouse.{namespace}"
    target, state_table, manifest_table = (f"{base}.{name}" for name in
                                         (spec.name, "_ingestion_state", "_ingestion_batches"))
    source_key = settings.source_key(spec.name)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {base}")
    validate_target_source(spark, target, settings, spec)
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {state_table}
        (source_table STRING, watermark TIMESTAMP, updated_at TIMESTAMP) USING iceberg""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {manifest_table}
        (batch_id STRING, source_key STRING, target_table STRING, mode STRING, row_count BIGINT,
         observed_at TIMESTAMP, completed_at TIMESTAMP, snapshot_id BIGINT, watermark TIMESTAMP)
        USING iceberg""")
    previous = read_watermark(spark, state_table, source_key) if mode == "incremental" else None
    if previous is not None and not spark.catalog.tableExists(target):
        raise ValueError("target is missing but its watermark exists; reload with --mode full or use a new --namespace")
    effective_mode = "incremental" if previous is not None else "full"
    dbtable = build_dbtable(settings, spec, previous, overlap_minutes)
    batch_id = str(uuid.uuid4())
    observed_at = datetime.now(timezone.utc)
    source = None
    try:
        source = read_source(spark, settings, dbtable)
        if not set(spec.columns).issubset(source.columns) or any(c.startswith("_") for c in source.columns):
            raise ValueError("source schema is missing required columns or has reserved metadata names")
        source = source.persist(StorageLevel.MEMORY_AND_DISK)
        extracted = summarize(source, spec)
        bronze = add_ingestion_metadata(source, settings, spec, batch_id, observed_at)
        append_bronze(bronze, target, source_key)
        snapshot_id = current_snapshot_id(spark, target)
        # Verify durable rows, not a second live JDBC query. If lost cache blocks
        # re-read the source, the watermark must still describe the written batch.
        written = extracted
        if snapshot_id is not None:
            accepted = (spark.read.option("snapshot-id", str(snapshot_id)).table(target)
                        .where(F.col("_batch_id") == batch_id))
            written = summarize(accepted, spec)
            if written["row_count"] != extracted["row_count"]:
                raise RuntimeError("written batch count differs from extraction; watermark was not advanced")
        elif extracted["row_count"]:
            raise RuntimeError("nonempty write has no Iceberg snapshot; watermark was not advanced")
        record = dict(batch_id=batch_id, source_key=source_key, target_table=target,
                      mode=effective_mode, row_count=written["row_count"], observed_at=observed_at,
                      completed_at=datetime.now(timezone.utc), snapshot_id=snapshot_id,
                      watermark=written["watermark"])
        write_manifest(spark, manifest_table, record)
        # Separate Iceberg commits: failure may replay observations. Consumers
        # must select only batch_ids recorded in the accepted manifest table.
        if written["watermark"] is not None:
            write_watermark(spark, state_table, source_key, written["watermark"])
        return record
    finally:
        if source is not None:
            source.unpersist()


def main(table: str, argv=None) -> None:
    args = parse_args(table, argv)
    settings = SourceSettings.from_env()
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(f"bank-sim-{table}-to-bronze").getOrCreate()
    try:
        result = load_table(spark, settings, TABLES[table], mode=args.mode,
                            overlap_minutes=args.overlap_minutes, namespace=args.namespace)
        print(json.dumps(result, default=str))
    finally:
        spark.stop()
