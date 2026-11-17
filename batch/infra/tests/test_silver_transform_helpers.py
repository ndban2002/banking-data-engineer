"""Schema contract checks that do not start Spark or require Java."""

from types import SimpleNamespace

import pytest

from silver_transform import validate_source_schema


ACCOUNT_TYPES = {
    "id": "string", "customer_id": "string", "merchant_id": "string",
    "currency": "string", "opening_balance": "bigint", "balance": "bigint",
    "created_at": "timestamp", "updated_at": "timestamp",
    "_source_system": "string", "_source_schema": "string",
    "_source_table": "string", "_ingested_at": "timestamp",
    "_batch_id": "string", "_ingestion_date": "date",
}


def schema(types):
    return SimpleNamespace(fields=[
        SimpleNamespace(name=name, dataType=SimpleNamespace(simpleString=lambda kind=kind: kind))
        for name, kind in types.items()
    ])


@pytest.mark.parametrize("kind", ["tinyint", "smallint", "int", "bigint"])
def test_integer_money_can_be_widened_without_losing_value(kind):
    validate_source_schema(schema({**ACCOUNT_TYPES, "balance": kind}), "accounts")


@pytest.mark.parametrize("column,kind", [
    ("balance", "double"), ("opening_balance", "float"),
    ("balance", "decimal(20,2)"), ("balance", "string"),
    ("id", "bigint"), ("created_at", "string"),
    ("updated_at", "timestamp_ntz"), ("_ingestion_date", "timestamp"),
])
def test_incompatible_schema_fails_before_row_casting(column, kind):
    with pytest.raises(ValueError, match=f"Unsupported Bronze type for {column}"):
        validate_source_schema(schema({**ACCOUNT_TYPES, column: kind}), "accounts")


def test_missing_required_column_is_batch_failure():
    types = dict(ACCOUNT_TYPES)
    del types["balance"]
    with pytest.raises(ValueError, match="missing required column: balance"):
        validate_source_schema(schema(types), "accounts")


def test_duplicate_column_name_is_batch_failure():
    source_schema = schema(ACCOUNT_TYPES)
    source_schema.fields.append(source_schema.fields[0])
    with pytest.raises(ValueError, match="duplicate column names"):
        validate_source_schema(source_schema, "accounts")


def test_extra_source_columns_do_not_force_unreviewed_silver_schema_changes():
    validate_source_schema(schema({**ACCOUNT_TYPES, "future_field": "array<string>"}), "accounts")
