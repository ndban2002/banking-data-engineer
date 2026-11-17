"""Fast checks for accepted input identity; data semantics use the runtime suite."""

from datetime import datetime, timedelta, timezone
import json

import pytest

from silver_loader import manifest_fingerprint, validate_manifest


def manifest(table="customers", **changes):
    observed = datetime(2026, 11, 16, tzinfo=timezone.utc)
    return {
        "batch_id": "accepted-batch", "source_key": json.dumps(
            ["banking-simulator", "test_bank", "core_sim", table]),
        "target_table": f"lakehouse.bronze_test.{table}", "mode": "full", "row_count": 1,
        "observed_at": observed, "completed_at": observed + timedelta(seconds=1),
        "snapshot_id": 123, "watermark": None,
    } | changes


def test_accepted_manifest_identity_is_scoped_to_source_and_bronze_namespace():
    accepted = manifest()
    assert validate_manifest(accepted, "customers", "bronze_test") == [
        "banking-simulator", "test_bank", "core_sim", "customers",
    ]
    with pytest.raises(ValueError, match="target"):
        validate_manifest(accepted, "customers", "another_bronze")
    with pytest.raises(ValueError, match="source identity"):
        validate_manifest(manifest(source_key=manifest("merchants")["source_key"]),
                          "customers", "bronze_test")


@pytest.mark.parametrize("changes", [
    {"row_count": -1}, {"row_count": "1"}, {"batch_id": ""},
    {"source_key": "not json"}, {"source_key": json.dumps(["missing", "fields"])},
    {"mode": "incremental"}, {"snapshot_id": None}, {"observed_at": None},
    {"completed_at": datetime(2026, 11, 15, tzinfo=timezone.utc)},
])
def test_invalid_acceptance_metadata_is_rejected(changes):
    with pytest.raises(ValueError):
        validate_manifest(manifest(**changes), "customers", "bronze_test")


def test_empty_initial_batch_can_lack_a_snapshot_without_becoming_a_delete():
    accepted = manifest(row_count=0, snapshot_id=None)
    assert validate_manifest(accepted, "customers", "bronze_test")[-1] == "customers"


def test_mutable_source_accepts_incremental_batch():
    accepted = manifest("transactions", mode="incremental")
    assert validate_manifest(accepted, "transactions", "bronze_test")[-1] == "transactions"


@pytest.mark.parametrize("changes", [
    {"snapshot_id": 124}, {"row_count": 2}, {"mode": "incremental"},
    {"observed_at": datetime(2026, 11, 16, 0, 0, 1, tzinfo=timezone.utc)},
    {"completed_at": datetime(2026, 11, 16, 0, 0, 2, tzinfo=timezone.utc)},
    {"watermark": datetime(2026, 11, 15, tzinfo=timezone.utc)},
    {"source_key": json.dumps(["banking-simulator", "another_bank", "core_sim", "customers"])},
    {"target_table": "lakehouse.other_namespace.customers"},
])
def test_reusing_a_batch_id_with_changed_input_changes_its_fingerprint(changes):
    before = manifest()
    assert before["batch_id"] == manifest(**changes)["batch_id"]
    assert manifest_fingerprint(before) != manifest_fingerprint(manifest(**changes))


def test_fingerprint_is_independent_of_dictionary_order_and_non_manifest_annotations():
    accepted = manifest()
    reordered = dict(reversed(list(accepted.items())))
    reordered["operator_note"] = "unrelated annotation"
    assert manifest_fingerprint(accepted) == manifest_fingerprint(reordered)
