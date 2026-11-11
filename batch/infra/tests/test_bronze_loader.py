from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
import time

import pytest

from bronze_loader import (
    TABLES,
    SourceSettings,
    build_dbtable,
    parse_args,
    parse_utc_timestamp,
)


UTC = timezone.utc


def settings(**overrides):
    values = dict(host="localhost", port="15432", database="bank", schema="core_sim",
                  user="test_reader", password="test_password")
    values.update(overrides)
    return SourceSettings(**values)


@pytest.mark.parametrize("table", ["customers", "merchants"])
def test_dimensions_require_complete_snapshots(table):
    assert TABLES[table].watermark_column is None
    assert parse_args(table, []).mode == "full"
    assert parse_args(table, ["--mode", "full"]).mode == "full"
    with pytest.raises(SystemExit):
        parse_args(table, ["--mode", "incremental"])


@pytest.mark.parametrize("table", ["accounts", "transactions"])
def test_mutable_tables_support_initial_and_incremental_load(table):
    assert TABLES[table].watermark_column == "updated_at"
    assert parse_args(table, []).mode == "incremental"
    assert parse_args(table, ["--mode", "full"]).mode == "full"
    options = parse_args(table, ["--mode", "incremental", "--overlap-minutes", "12",
                                "--namespace", "bronze_test"])
    assert options.overlap_minutes == 12
    assert options.namespace == "bronze_test"


@pytest.mark.parametrize("arguments", [
    ["--mode", "unknown"], ["--overlap-minutes", "-1"],
    ["--namespace", "bronze;DROP TABLE accounts"], ["--namespace", "bronze.other"],
])
def test_invalid_options_fail_before_spark_starts(arguments):
    with pytest.raises(SystemExit):
        parse_args("accounts", arguments)


def test_checkpoint_key_is_scoped_to_system_database_schema_and_table():
    source = settings()
    assert json.loads(source.source_key("accounts")) == [
        "banking-simulator", "bank", "core_sim", "accounts",
    ]
    keys = {
        source.source_key("accounts"), source.source_key("transactions"),
        settings(database="other_bank").source_key("accounts"),
        settings(schema="core_sim_new").source_key("accounts"),
    }
    assert len(keys) == 4
    assert "test_reader" not in source.source_key("accounts")
    assert "test_password" not in source.source_key("accounts")


def test_jdbc_url_excludes_connection_credentials():
    source = settings()
    assert source.jdbc_url.startswith("jdbc:postgresql://localhost:15432/")
    assert "test_reader" not in source.jdbc_url
    assert "test_password" not in source.jdbc_url


@pytest.mark.parametrize("table", list(TABLES))
def test_first_load_has_no_time_filter_and_uses_quoted_identifiers(table):
    query = build_dbtable(settings(schema="BankLab"), TABLES[table], None, 5)
    assert f'"BankLab"."{table}"' in query
    assert "WHERE" not in query.upper()


@pytest.mark.parametrize("table", ["accounts", "transactions"])
def test_incremental_overlap_is_inclusive_and_normalized_to_utc(table):
    watermark = datetime(2026, 11, 1, 19, 0, 0, 123456, tzinfo=timezone(timedelta(hours=7)))
    query = build_dbtable(settings(schema="BankLab"), TABLES[table], watermark, 5)
    assert f'"BankLab"."{table}"' in query
    assert '"updated_at" >=' in query
    assert "2026-11-01 11:55:00.123456+00" in query
    assert 'ORDER BY "updated_at", "id"' in query


@pytest.mark.parametrize("table", ["customers", "merchants"])
def test_created_at_is_never_used_as_dimension_watermark(table):
    with pytest.raises((ValueError, RuntimeError)):
        build_dbtable(settings(), TABLES[table], datetime(2026, 1, 1, tzinfo=UTC), 5)


def test_timestamp_parsing_keeps_microseconds_and_is_independent_of_driver_timezone(monkeypatch):
    try:
        with monkeypatch.context() as context:
            context.setenv("TZ", "Asia/Ho_Chi_Minh")
            if hasattr(time, "tzset"):
                time.tzset()
            actual = parse_utc_timestamp("2026-11-01T19:30:00.123456+07:00")
            assert actual == datetime(2026, 11, 1, 12, 30, 0, 123456, tzinfo=UTC)
            assert actual.utcoffset() == timedelta(0)
            assert parse_utc_timestamp(None) is None
    finally:
        if hasattr(time, "tzset"):
            time.tzset()


def test_source_settings_read_only_expected_environment_fields(monkeypatch):
    values = dict(host="localhost", port="15432", database="bank", schema="core_sim",
                  user="test_reader", password="test_password")
    for field, value in values.items():
        monkeypatch.setenv(f"SOURCE_PG_{field.upper()}", value)
    source = SourceSettings.from_env()
    assert source.schema == "core_sim"
    assert source.database == "bank"
    monkeypatch.setenv("SOURCE_PG_USER", "replace_me")
    with pytest.raises((ValueError, RuntimeError)):
        SourceSettings.from_env()


@pytest.mark.parametrize("table", list(TABLES))
def test_table_column_inventory_matches_source_to_bronze_mapping(table):
    mapping = Path(__file__).resolve().parents[3] / "contracts" / "mappings" / "postgresql" / f"{table}.yaml"
    # Inspect the explicit source-column list, without requiring a YAML runtime dependency.
    columns = re.findall(r"^\s+- source: (\w+)\s*$", mapping.read_text(), flags=re.MULTILINE)
    assert tuple(columns) == TABLES[table].columns
