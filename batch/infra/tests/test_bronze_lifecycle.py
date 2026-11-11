"""Exercise acceptance/checkpoint boundaries without requiring a JVM.

These fakes deliberately model only orchestration: PostgreSQL/JDBC and Iceberg
semantics still require the separately documented runtime acceptance workflow.
"""

from datetime import datetime, timezone
import sys
from types import ModuleType, SimpleNamespace

import pytest

import bronze_loader as loader


validate_target_source = loader.validate_target_source


class Frame:
    def __init__(self, events):
        self.events = events
        self.columns = sorted({column for spec in loader.TABLES.values() for column in spec.columns})

    def persist(self, *_):
        self.events.append("persist")
        return self

    def unpersist(self, *_):
        self.events.append("unpersist")

    def where(self, *_):
        return self


class Reader:
    def __init__(self, frame):
        self.frame = frame

    def option(self, *_):
        return self

    def table(self, *_):
        return self.frame


@pytest.fixture
def scenario(monkeypatch):
    events = []
    source = Frame(events)
    written = Frame(events)
    spark = SimpleNamespace(read=Reader(written), sql=lambda *_: None,
                            conf=SimpleNamespace(set=lambda *_: None),
                            catalog=SimpleNamespace(tableExists=lambda *_: True))
    # No PySpark installation or active SparkContext is needed by these tests.
    pyspark = ModuleType("pyspark")
    pyspark.StorageLevel = SimpleNamespace(MEMORY_AND_DISK=object())
    sql = ModuleType("pyspark.sql")
    sql.functions = SimpleNamespace(col=lambda name: name)
    pyspark.sql = sql
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    config = loader.SourceSettings(host="localhost", port="15432", database="bank",
                                   schema="core_sim", user="reader", password="secret")
    watermark = datetime(2026, 11, 1, 12, tzinfo=timezone.utc)
    state = SimpleNamespace(events=events, source=source, written=written, spark=spark,
                            settings=config, watermark=watermark, row_count=2,
                            previous_watermark=watermark,
                            written_count=2, snapshot_id=123, manifests=[])

    def summarize(frame, spec):
        events.append("summarize_written" if frame is written else "summarize_source")
        return {"row_count": state.written_count if frame is written else state.row_count,
                "watermark": state.watermark if spec.watermark_column else None}

    def manifest(spark, table, record):
        events.append("manifest")
        state.manifests.append(record)

    monkeypatch.setattr(loader, "validate_target_source", lambda *_: events.append("validate_source"))
    monkeypatch.setattr(loader, "read_watermark", lambda *_: state.previous_watermark)
    monkeypatch.setattr(loader, "read_source", lambda *_: source)
    monkeypatch.setattr(loader, "add_ingestion_metadata", lambda *_: source)
    monkeypatch.setattr(loader, "summarize", summarize)
    monkeypatch.setattr(loader, "append_bronze", lambda *_: events.append("append"))
    monkeypatch.setattr(loader, "current_snapshot_id", lambda *_: state.snapshot_id)
    monkeypatch.setattr(loader, "write_manifest", manifest)
    monkeypatch.setattr(loader, "write_watermark", lambda *_: events.append("checkpoint"))
    return state


def test_success_accepts_manifest_before_advancing_checkpoint(scenario):
    loader.load_table(scenario.spark, scenario.settings, loader.TABLES["accounts"], mode="incremental")
    events = scenario.events
    assert events.index("append") < events.index("summarize_written")
    assert events.index("summarize_written") < events.index("manifest") < events.index("checkpoint")
    assert events[-1] == "unpersist"
    assert len(scenario.manifests) == 1
    record = scenario.manifests[0]
    assert record["source_key"] == scenario.settings.source_key("accounts")
    assert record["target_table"] == "lakehouse.bronze.accounts"
    assert record["snapshot_id"] == 123
    assert record["row_count"] == 2
    assert record["mode"] == "incremental"


@pytest.mark.parametrize("failure", ["append_bronze", "write_manifest"])
def test_write_failure_never_advances_checkpoint_and_releases_cache(monkeypatch, scenario, failure):
    def fail(*_):
        scenario.events.append(failure)
        raise RuntimeError("injected storage failure")

    monkeypatch.setattr(loader, failure, fail)
    with pytest.raises(RuntimeError, match="injected storage failure"):
        loader.load_table(scenario.spark, scenario.settings, loader.TABLES["accounts"], mode="full")
    assert "checkpoint" not in scenario.events
    if failure == "append_bronze":
        assert "manifest" not in scenario.events
    assert scenario.events[-1] == "unpersist"


def test_incomplete_bronze_write_is_not_accepted(monkeypatch, scenario):
    scenario.written_count = 1
    with pytest.raises((ValueError, RuntimeError)):
        loader.load_table(scenario.spark, scenario.settings, loader.TABLES["customers"], mode="full")
    assert "manifest" not in scenario.events
    assert "checkpoint" not in scenario.events
    assert scenario.events[-1] == "unpersist"


def test_empty_dimension_snapshot_still_has_manifest(scenario):
    scenario.row_count = scenario.written_count = 0
    scenario.snapshot_id = None
    scenario.watermark = None
    loader.load_table(scenario.spark, scenario.settings, loader.TABLES["customers"], mode="full")
    assert "append" in scenario.events
    assert len(scenario.manifests) == 1
    assert scenario.manifests[0]["row_count"] == 0
    assert scenario.manifests[0]["mode"] == "full"
    assert scenario.manifests[0]["snapshot_id"] is None
    assert "checkpoint" not in scenario.events
    assert scenario.events[-1] == "unpersist"


def test_empty_increment_has_no_watermark_to_advance(scenario):
    scenario.row_count = scenario.written_count = 0
    scenario.watermark = None
    loader.load_table(scenario.spark, scenario.settings, loader.TABLES["accounts"], mode="incremental")
    assert len(scenario.manifests) == 1
    assert scenario.manifests[0]["mode"] == "incremental"
    assert "checkpoint" not in scenario.events


def test_missing_checkpoint_bootstraps_full_snapshot(scenario):
    scenario.previous_watermark = None
    loader.load_table(scenario.spark, scenario.settings, loader.TABLES["accounts"],
                      mode="incremental", namespace="bronze_test")
    record = scenario.manifests[0]
    assert record["mode"] == "full"
    assert record["target_table"] == "lakehouse.bronze_test.accounts"


def test_nonempty_write_without_snapshot_cannot_be_accepted(scenario):
    scenario.snapshot_id = None
    with pytest.raises(RuntimeError, match="no Iceberg snapshot"):
        loader.load_table(scenario.spark, scenario.settings, loader.TABLES["accounts"], mode="full")
    assert "manifest" not in scenario.events
    assert "checkpoint" not in scenario.events
    assert scenario.events[-1] == "unpersist"


def test_checkpoint_failure_leaves_accepted_manifest_and_cleans_up(monkeypatch, scenario):
    def fail(*_):
        raise RuntimeError("injected checkpoint failure")

    monkeypatch.setattr(loader, "write_watermark", fail)
    with pytest.raises(RuntimeError, match="injected checkpoint failure"):
        loader.load_table(scenario.spark, scenario.settings, loader.TABLES["accounts"], mode="full")
    assert len(scenario.manifests) == 1
    assert scenario.events[-1] == "unpersist"


def test_extraction_failure_releases_cached_input(monkeypatch, scenario):
    def fail(*_):
        raise RuntimeError("injected source read failure")

    monkeypatch.setattr(loader, "summarize", fail)
    with pytest.raises(RuntimeError, match="injected source read failure"):
        loader.load_table(scenario.spark, scenario.settings, loader.TABLES["merchants"], mode="full")
    assert "append" not in scenario.events
    assert "manifest" not in scenario.events
    assert "checkpoint" not in scenario.events
    assert scenario.events[-1] == "unpersist"


def test_source_mismatch_stops_before_reading_or_appending(monkeypatch, scenario):
    def fail(*_):
        raise RuntimeError("source identity mismatch")

    monkeypatch.setattr(loader, "validate_target_source", fail)
    with pytest.raises(RuntimeError, match="source identity mismatch"):
        loader.load_table(scenario.spark, scenario.settings, loader.TABLES["accounts"], mode="full")
    assert "persist" not in scenario.events
    assert "append" not in scenario.events
    assert "manifest" not in scenario.events
    assert "checkpoint" not in scenario.events


def test_bound_target_rejects_another_database(scenario):
    other = loader.SourceSettings(host="localhost", port="15432", database="other_bank",
                                  schema="core_sim", user="reader", password="secret")
    scenario.spark.catalog = SimpleNamespace(tableExists=lambda *_: True)
    scenario.spark.sql = lambda *_: SimpleNamespace(collect=lambda: [
        {"key": loader.SOURCE_PROPERTY, "value": other.source_key("accounts")},
    ])
    with pytest.raises(ValueError, match="another source"):
        validate_target_source(scenario.spark, "lakehouse.bronze.accounts", scenario.settings,
                               loader.TABLES["accounts"])


def test_bound_target_accepts_matching_source_without_scanning_data(scenario):
    scenario.spark.sql = lambda *_: SimpleNamespace(collect=lambda: [
        {"key": loader.SOURCE_PROPERTY, "value": scenario.settings.source_key("accounts")},
    ])
    validate_target_source(scenario.spark, "lakehouse.bronze.accounts", scenario.settings,
                           loader.TABLES["accounts"])


def test_stale_checkpoint_cannot_skip_missing_target_data(scenario):
    scenario.spark.catalog.tableExists = lambda table: table != "lakehouse.bronze.accounts"
    with pytest.raises(ValueError, match="target is missing"):
        loader.load_table(scenario.spark, scenario.settings, loader.TABLES["accounts"], mode="incremental")
    assert "persist" not in scenario.events
    assert "append" not in scenario.events
    assert "manifest" not in scenario.events


class Predicate:
    def __init__(self, evaluate):
        self.evaluate = evaluate

    def __and__(self, other):
        return Predicate(lambda row: self.evaluate(row) and other.evaluate(row))

    def __invert__(self):
        return Predicate(lambda row: not self.evaluate(row))


class Column:
    def __init__(self, name):
        self.name = name

    def eqNullSafe(self, value):
        return Predicate(lambda row: row.get(self.name) == value)


class LegacyFrame:
    columns = ["_source_system", "_source_schema", "_source_table"]

    def __init__(self, rows):
        self.rows = rows

    def where(self, predicate):
        return LegacyFrame([row for row in self.rows if predicate.evaluate(row)])

    def limit(self, count):
        return LegacyFrame(self.rows[:count])

    def count(self):
        return len(self.rows)


@pytest.mark.parametrize("changed_field,bad_value", [
    (None, None), ("_source_schema", "other_schema"),
    ("_source_table", "accounts"), ("_source_system", None),
])
def test_legacy_table_binding_requires_matching_row_provenance(scenario, changed_field, bad_value):
    sys.modules["pyspark.sql"].functions.col = Column
    row = dict(_source_system="banking-simulator", _source_schema="core_sim", _source_table="transactions")
    if changed_field:
        row[changed_field] = bad_value
    scenario.spark.table = lambda *_: LegacyFrame([row])
    statements = []

    def sql(statement):
        statements.append(statement)
        return SimpleNamespace(collect=lambda: [])

    scenario.spark.sql = sql
    if changed_field:
        with pytest.raises(ValueError, match="another source"):
            validate_target_source(scenario.spark, "lakehouse.bronze.transactions", scenario.settings,
                                   loader.TABLES["transactions"])
        assert not any(statement.startswith("ALTER TABLE") for statement in statements)
    else:
        validate_target_source(scenario.spark, "lakehouse.bronze.transactions", scenario.settings,
                               loader.TABLES["transactions"])
        assert any(statement.startswith("ALTER TABLE") for statement in statements)


def test_main_stops_spark_when_load_fails(monkeypatch, scenario):
    builder = SimpleNamespace()
    builder.appName = lambda *_: builder
    builder.getOrCreate = lambda: scenario.spark
    sys.modules["pyspark.sql"].SparkSession = SimpleNamespace(builder=builder)
    scenario.spark.stop = lambda: scenario.events.append("stop")
    monkeypatch.setattr(loader.SourceSettings, "from_env", classmethod(lambda cls: scenario.settings))

    def fail(*_, **__):
        raise RuntimeError("injected load failure")

    monkeypatch.setattr(loader, "load_table", fail)
    with pytest.raises(RuntimeError, match="injected load failure"):
        loader.main("accounts", ["--mode", "full"])
    assert scenario.events[-1] == "stop"
