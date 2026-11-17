"""Current-state transformations for accepted Bronze observations.

This module does not read JDBC, commit Iceberg tables, or advance checkpoints.
It returns deterministic updates and quarantine rows for the loader to commit.
Customer/merchant history and cross-table financial checks are separate stages.
"""

from __future__ import annotations

from dataclasses import dataclass
import json

from bronze_loader import TABLES


@dataclass(frozen=True)
class SilverSpec:
    target_name: str
    key: str
    renames: tuple[tuple[str, str], ...]


SILVER_SPECS = {
    "customers": SilverSpec("dim_customers", "customer_id", (
        ("id", "customer_id"), ("name", "customer_name"),
    )),
    "merchants": SilverSpec("dim_merchants", "merchant_id", (
        ("id", "merchant_id"), ("name", "merchant_name"),
    )),
    "accounts": SilverSpec("dim_accounts", "account_id", (
        ("id", "account_id"), ("opening_balance", "opening_balance_vnd"),
        ("balance", "balance_vnd"),
    )),
    "transactions": SilverSpec("fact_transactions", "transaction_id", (
        ("id", "transaction_id"), ("kind", "transaction_type"),
        ("amount", "amount_vnd"),
    )),
}

BRONZE_METADATA = (
    "_source_system", "_source_schema", "_source_table", "_ingested_at",
    "_batch_id", "_ingestion_date",
)
SILVER_METADATA = BRONZE_METADATA + (
    "_source_key", "_bronze_snapshot_id", "_version_at", "_record_hash",
)
QUARANTINE_COLUMNS = (
    "rejection_id", "source_table", "source_key", "batch_id",
    "bronze_snapshot_id", "record_id", "reason_codes", "raw_record", "observed_at",
)
UUID_PATTERN = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
MONEY_COLUMNS = {"opening_balance", "balance", "amount"}
INTEGRAL_TYPES = {"tinyint", "smallint", "int", "bigint"}


def business_columns(table: str) -> tuple[str, ...]:
    """Ordered destination columns, also used for deterministic payload hashing."""
    renames = dict(SILVER_SPECS[table].renames)
    return tuple(renames.get(name, name) for name in TABLES[table].columns)


def validate_source_schema(schema, table: str) -> None:
    """Fail the batch on schema drift instead of silently truncating money/time.

    Additional business columns may remain in Bronze, but the explicitly mapped
    columns must retain their types. Null *values* are checked per row later.
    """
    names = [field.name for field in schema.fields]
    if len(names) != len(set(names)):
        raise ValueError("Bronze schema has duplicate column names")
    types = {field.name: field.dataType.simpleString() for field in schema.fields}
    for name in TABLES[table].columns + BRONZE_METADATA:
        if name not in types:
            raise ValueError(f"Bronze schema is missing required column: {name}")
        expected = {"string"}
        if name in MONEY_COLUMNS:
            expected = INTEGRAL_TYPES
        elif name.endswith("_at"):
            expected = {"timestamp"}
        elif name == "_ingestion_date":
            expected = {"date"}
        elif name == "requested_decline":
            expected = {"boolean"}
        if types[name] not in expected:
            raise ValueError(f"Unsupported Bronze type for {name}: {types[name]}")


def _local_rules(table: str, manifest: dict):
    """Return named predicates. A null predicate is a violation, never a pass."""
    from pyspark.sql import functions as F

    spec = SILVER_SPECS[table]
    source_identity = json.loads(manifest["source_key"])
    if not isinstance(source_identity, list) or len(source_identity) != 4 or source_identity[3] != table:
        raise ValueError("manifest source identity does not match the requested table")
    rules = [("invalid_id", F.col(spec.key).rlike(UUID_PATTERN))]
    for name in business_columns(table):
        if name.endswith("_at"):
            rules.append((f"missing_{name}", F.col(name).isNotNull()))

    provenance = (
        (F.col("_source_system") == F.lit(source_identity[0]))
        & (F.col("_source_schema") == F.lit(source_identity[2]))
        & (F.col("_source_table") == F.lit(table))
        & (F.col("_batch_id") == F.lit(manifest["batch_id"]))
        & (F.col("_ingested_at") == F.lit(manifest["observed_at"]))
        & (F.col("_ingestion_date") == F.to_date(F.col("_ingested_at")))
    )
    rules.append(("invalid_batch_metadata", provenance))
    if table in ("customers", "merchants"):
        rules.extend([
            ("blank_name", F.length(F.col("customer_name" if table == "customers" else "merchant_name")) > 0),
            ("blank_region", F.length(F.col("region")) > 0),
        ])
    if table == "merchants":
        rules.extend([
            ("invalid_mcc", F.col("mcc").rlike(r"^[0-9]{4}$")),
            ("invalid_channel", F.col("channel").isin("QR", "POS")),
        ])
    if table == "accounts":
        for name in ("customer_id", "merchant_id"):
            rules.append((f"invalid_{name}", F.col(name).isNull() | F.col(name).rlike(UUID_PATTERN)))
        rules.extend([
            ("invalid_owner", F.col("customer_id").isNotNull() != F.col("merchant_id").isNotNull()),
            ("invalid_currency", F.col("currency") == "VND"),
            ("negative_opening_balance", F.col("opening_balance_vnd") >= 0),
            ("negative_balance", F.col("balance_vnd") >= 0),
        ])
    if table == "transactions":
        for name in ("request_id", "source_account_id", "destination_account_id"):
            rules.append((f"invalid_{name}", F.col(name).rlike(UUID_PATTERN)))
        rules.extend([
            ("invalid_original_transaction_id", F.col("original_transaction_id").isNull()
             | F.col("original_transaction_id").rlike(UUID_PATTERN)),
            ("invalid_transaction_type", F.col("transaction_type").isin("PAYMENT", "REFUND")),
            ("invalid_status", F.col("status").isin("PENDING", "SUCCEEDED", "FAILED")),
            ("invalid_currency", F.col("currency") == "VND"),
            ("invalid_channel", F.col("channel").isin("QR", "POS")),
            ("nonpositive_amount", F.col("amount_vnd") > 0),
            ("same_accounts", F.col("source_account_id") != F.col("destination_account_id")),
            ("missing_decline_flag", F.col("requested_decline").isNotNull()),
            ("invalid_status_error", ((F.col("status") == "FAILED") & (F.length(F.trim(F.col("error_code"))) > 0))
             | (F.col("status").isin("PENDING", "SUCCEEDED") & F.col("error_code").isNull())),
            ("invalid_refund_link", ((F.col("transaction_type") == "PAYMENT") & F.col("original_transaction_id").isNull())
             | ((F.col("transaction_type") == "REFUND") & F.col("original_transaction_id").isNotNull()
                & (F.col("original_transaction_id") != F.col("transaction_id")))),
        ])
    return rules


def _append_reason(frame, condition, reason: str):
    from pyspark.sql import functions as F

    addition = F.when(F.coalesce(condition, F.lit(False)), F.array(F.lit(reason))).otherwise(
        F.array().cast("array<string>"))
    return frame.withColumn("__reasons", F.concat(F.col("__reasons"), addition))


def _quarantine(frame, table: str, manifest: dict):
    from pyspark.sql import functions as F

    identity = F.to_json(F.struct(
        F.lit(manifest["source_key"]).alias("source_key"),
        F.lit(manifest["batch_id"]).alias("batch_id"), F.col("__raw_hash").alias("raw_hash"),
    ))
    return frame.select(
        F.sha2(identity, 256).alias("rejection_id"), F.lit(table).alias("source_table"),
        F.lit(manifest["source_key"]).alias("source_key"),
        F.lit(manifest["batch_id"]).alias("batch_id"),
        F.lit(manifest["snapshot_id"]).cast("long").alias("bronze_snapshot_id"),
        F.col(SILVER_SPECS[table].key).cast("string").alias("record_id"),
        F.sort_array(F.array_distinct(F.col("__reasons"))).alias("reason_codes"),
        F.col("__raw_record").alias("raw_record"),
        F.lit(manifest["observed_at"]).cast("timestamp").alias("observed_at"),
    )


def _validate_current(current, table: str) -> None:
    from pyspark.sql import functions as F

    spec = SILVER_SPECS[table]
    missing = set(business_columns(table) + SILVER_METADATA) - set(current.columns)
    if missing:
        raise ValueError(f"Silver target is missing columns: {sorted(missing)}")
    bad_metadata = (F.col(spec.key).isNull() | F.col("_version_at").isNull()
                    | F.col("_record_hash").isNull())
    if current.where(bad_metadata).limit(1).count():
        raise ValueError("Silver target has null key/version/hash; repair it before merging")
    if current.groupBy(spec.key).count().where(F.col("count") > 1).limit(1).count():
        raise ValueError("Silver target has duplicate business keys; repair it before merging")
    if table == "transactions" and current.groupBy("request_id").count().where(
        F.col("count") > 1
    ).limit(1).count():
        raise ValueError("Silver target has duplicate request IDs; repair it before merging")


def prepare_batch(source_df, current_df_or_None, table: str, manifest: dict):
    """Return only newer valid rows and deterministic quarantine entries.

    A timestamp tie with different normalized payload is ambiguous and is
    quarantined. No UUID ordering is used to pretend that a source version is
    known. Missing keys in full snapshots are retained (no delete inference).
    """
    from pyspark.sql import Window, functions as F

    spec = SILVER_SPECS[table]
    validate_source_schema(source_df.schema, table)
    source_df.sparkSession.conf.set("spark.sql.session.timeZone", "UTC")
    renames = dict(spec.renames)
    mapped = []
    for name in TABLES[table].columns:
        value = F.col(name)
        if name == "id" or name.endswith("_id"):
            value = F.lower(value)
        elif name == "name":
            value = F.trim(value)
        elif name == "region":
            value = F.upper(F.trim(value))
        elif name in MONEY_COLUMNS:
            value = value.cast("long")
        mapped.append(value.alias(renames.get(name, name)))
    raw = F.to_json(F.struct(*[F.col(name) for name in sorted(source_df.columns)]),
                    options={"ignoreNullFields": "false"})
    frame = source_df.select(*mapped, *[F.col(name) for name in BRONZE_METADATA],
                             raw.alias("__raw_record"))
    version = F.col("updated_at") if TABLES[table].watermark_column else F.lit(manifest["observed_at"])
    frame = (frame.withColumn("__raw_hash", F.sha2(F.col("__raw_record"), 256))
             .withColumn("_source_key", F.lit(manifest["source_key"]))
             .withColumn("_bronze_snapshot_id", F.lit(manifest["snapshot_id"]).cast("long"))
             .withColumn("_version_at", version.cast("timestamp"))
             .withColumn("_record_hash", F.sha2(F.to_json(
                 F.struct(*[F.col(name) for name in business_columns(table)]),
                 options={"ignoreNullFields": "false"}), 256)))
    rules = _local_rules(table, manifest)
    frame = frame.withColumn("__reasons", F.filter(F.array(*[
        F.when(~F.coalesce(valid, F.lit(False)), F.lit(reason)) for reason, valid in rules
    ]), lambda reason: reason.isNotNull()))

    # Check ambiguity before filtering invalid rows: an invalid competing
    # payload must not make a same-version conflict disappear.
    ambiguous = (frame.where(F.col(spec.key).isNotNull() & F.col("_version_at").isNotNull())
                 .groupBy(spec.key, "_version_at").agg(F.countDistinct("_record_hash").alias("__variants"))
                 .where(F.col("__variants") > 1)
                 .select(spec.key, "_version_at", F.lit(True).alias("__ambiguous")))
    frame = frame.join(ambiguous, [spec.key, "_version_at"], "left")
    frame = _append_reason(frame, F.col("__ambiguous"), "conflicting_version").drop("__ambiguous")
    rejected = _quarantine(frame.where(F.size("__reasons") > 0), table, manifest)
    # Retain the latest eligible version after quarantining invalid/conflicting
    # observations. Target comparison below prevents regressions on replay.
    order = Window.partitionBy(spec.key).orderBy(F.col("_version_at").desc_nulls_last())
    candidates = (frame.where(F.size("__reasons") == 0)
                  .withColumn("__rank", F.dense_rank().over(order))
                  .where(F.col("__rank") == 1)
                  .drop("__rank").dropDuplicates([spec.key]))

    current = current_df_or_None
    if current is not None:
        _validate_current(current, table)
        selected = [F.col(spec.key), F.col("_version_at").alias("__current_version"),
                    F.col("_record_hash").alias("__current_hash")]
        if table == "transactions":
            selected.append(F.col("request_id").alias("__current_request"))
        candidates = candidates.join(current.select(*selected), spec.key, "left")
        # Ignore valid older observations before checking incoming request IDs.
        candidates = candidates.where(F.col("__current_version").isNull()
                                      | (F.col("_version_at") >= F.col("__current_version")))
        conflict = ((F.col("_version_at") == F.col("__current_version"))
                    & (F.col("_record_hash") != F.col("__current_hash")))
        candidates = _append_reason(candidates, conflict, "conflicting_version")
        if table == "transactions":
            changed_request = F.col("__current_request").isNotNull() & (
                F.col("request_id") != F.col("__current_request"))
            candidates = _append_reason(candidates, changed_request, "request_id_changed")
        rejected = rejected.unionByName(_quarantine(candidates.where(F.size("__reasons") > 0), table, manifest))
        candidates = candidates.where((F.size("__reasons") == 0) & (
            F.col("__current_version").isNull() | (F.col("_version_at") > F.col("__current_version"))))

    if table == "transactions":
        repeated = (candidates.groupBy("request_id").count().where(F.col("count") > 1)
                    .select("request_id", F.lit(True).alias("__request_duplicate")))
        candidates = candidates.join(repeated, "request_id", "left")
        candidates = _append_reason(candidates, F.col("__request_duplicate"), "duplicate_request_id")
        if current is not None:
            owners = current.select(F.col("request_id").alias("__owned_request"),
                                    F.col(spec.key).alias("__request_owner"))
            candidates = candidates.join(owners, F.col("request_id") == F.col("__owned_request"), "left")
            candidates = _append_reason(candidates, F.col("__request_owner").isNotNull()
                                        & (F.col(spec.key) != F.col("__request_owner")), "duplicate_request_id")
        rejected = rejected.unionByName(_quarantine(candidates.where(F.size("__reasons") > 0), table, manifest))
        candidates = candidates.where(F.size("__reasons") == 0)

    # A malformed source row can appear repeatedly in Bronze. Retain its stable
    # identity once per batch and combine every discovered reason deterministically.
    fixed = [name for name in QUARANTINE_COLUMNS if name != "reason_codes"]
    rejected = (rejected.groupBy(*fixed).agg(F.sort_array(F.array_distinct(
        F.flatten(F.collect_list("reason_codes")))).alias("reason_codes"))
        .select(*QUARANTINE_COLUMNS))
    updates = candidates.select(*business_columns(table), *SILVER_METADATA)
    return updates, rejected
