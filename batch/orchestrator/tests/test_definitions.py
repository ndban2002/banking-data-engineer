import json

import dagster as dg
import pytest

from banking_orchestrator.definitions import defs
from banking_orchestrator.spark import SparkJobs


TABLES = {
    "customers": "dim_customers",
    "merchants": "dim_merchants",
    "accounts": "dim_accounts",
    "transactions": "fact_transactions",
}
BRONZE_SCRIPTS = [f"load_{table}_bronze.py" for table in TABLES]
SILVER_LOADER = "silver_loader.py"
SILVER_GATE = "silver_quality.py"
DAILY_GOLD = "gold_daily_merchant.py"
ROLLUPS = "gold_rollups.py"
TOP_TRANSACTIONS = "gold_monthly_top_transactions.py"
DAILY_KEY = ("gold", "daily_merchant_performance")
ROLLUP_TABLES = ("daily_platform_performance", "monthly_merchant_performance")
RUN_CONFIG = {"resources": {"spark": {"config": {"namespace_suffix": "test"}}}}
QUALITY_CHECKS = (
    "customers_key_unique", "merchants_key_unique", "accounts_key_unique",
    "transactions_key_unique", "transaction_request_unique", "account_owner_xor",
    "account_customer_fk", "account_merchant_fk", "account_customer_unique",
    "account_merchant_unique", "transaction_account_fk", "refund_reference",
    "successful_payment_direction", "successful_refund_consistency",
    "successful_refund_limit", "account_balance_reconciliation",
)


class FakeSpark:
    def __init__(self):
        self.calls = []
        self.quality_failed = False
        self.snapshots = {
            f"lakehouse.silver_test.{table}": 9_007_199_254_740_993 + index
            for index, table in enumerate(TABLES.values())
        }
        self.totals = {
            "payment_amount_vnd": "12345678901234567890",
            "refund_amount_vnd": "4000",
            "net_amount_vnd": "12345678901234563890",
        }

    def submit(self, context, script, *args, allow_quality_failure=False):
        self.calls.append((script, args))
        if script in BRONZE_SCRIPTS:
            assert args[args.index("--namespace") + 1] == "bronze_test"
            table = script.removeprefix("load_").removesuffix("_bronze.py")
            return {
                "target_table": f"lakehouse.bronze_test.{table}",
                "batch_id": f"batch-{table}",
                "snapshot_id": BRONZE_SCRIPTS.index(script) + 1,
                "row_count": 1,
                "mode": args[args.index("--mode") + 1],
                "watermark": None,
            }
        if script == SILVER_LOADER:
            assert args[args.index("--bronze-namespace") + 1] == "bronze_test"
            assert args[args.index("--silver-namespace") + 1] == "silver_test"
            return {
                "batches": [
                    {
                        "bronze_table": f"lakehouse.bronze_test.{table}",
                        "batch_id": f"batch-{table}",
                        "bronze_snapshot_id": index + 1,
                        "silver_table": f"lakehouse.silver_test.{silver}",
                        "input_rows": 1,
                        "upsert_rows": 1,
                        "quarantined_rows": 0,
                        "skipped": False,
                    }
                    for index, (table, silver) in enumerate(TABLES.items())
                ],
                "tables": {
                    table: {"snapshot_id": snapshot, "row_count": 1}
                    for table, snapshot in self.snapshots.items()
                },
            }
        if script == SILVER_GATE:
            assert allow_quality_failure
            assert "--require-processed" in args
            assert args[args.index("--namespace") + 1] == "silver_test"
            return {
                "ok": not self.quality_failed,
                "namespace": "silver_test",
                "snapshots": self.snapshots,
                "violations": {
                    check: int(self.quality_failed and check == "account_balance_reconciliation")
                    for check in QUALITY_CHECKS
                },
                "pending_transactions": 0,
                "processing_violations": {"quarantined_rows": 0, "uncheckpointed_batches": 0},
            }
        if script == DAILY_GOLD:
            assert args[args.index("--silver-namespace") + 1] == "silver_test"
            assert args[args.index("--gold-namespace") + 1] == "gold_test"
            assert json.loads(args[args.index("--source-snapshots") + 1]) == self.snapshots
            return self.gold_record(DAILY_KEY[-1], self.snapshots)
        if script == ROLLUPS:
            sources = {"lakehouse.gold_test.daily_merchant_performance": 9_007_199_254_740_999}
            return {"tables": {table: self.gold_record(table, sources) for table in ROLLUP_TABLES}}
        if script == TOP_TRANSACTIONS:
            return self.gold_record("monthly_top_transactions", self.snapshots)
        pytest.fail(f"Unrecognized Spark job {script}")

    def gold_record(self, table, sources):
        return {
            "target_table": f"lakehouse.gold_test.{table}",
            "run_id": "completed-gold-run",
            "input_id": "frozen-input",
            "source_snapshots": json.dumps(sources),
            "output_snapshot_id": 9_007_199_254_740_999,
            "row_count": 1,
            "reconciliation": {"ok": True, "totals": self.totals},
        }


@pytest.fixture
def fake_spark(monkeypatch):
    runner = FakeSpark()

    def submit(resource, *args, **kwargs):
        return runner.submit(*args, **kwargs)

    monkeypatch.setattr(SparkJobs, "submit", submit)
    return runner


def materializations(result):
    return {
        tuple(event.asset_key.path): event.step_materialization_data.materialization.metadata
        for event in result.get_asset_materialization_events()
    }


def test_daily_materializes_all_assets_after_the_silver_gate(fake_spark):
    dg.Definitions.validate_loadable(defs)
    job = defs.resolve_job_def("banking_daily")
    result = job.execute_in_process(run_config=RUN_CONFIG)

    assert result.success
    assert [script for script, _ in fake_spark.calls] == [
        *BRONZE_SCRIPTS, SILVER_LOADER, SILVER_GATE, DAILY_GOLD,
    ]
    assets = materializations(result)
    assert set(assets) == {
        *(("bronze", table) for table in TABLES),
        *(("silver", table) for table in TABLES.values()),
        DAILY_KEY,
    }
    assert assets[("bronze", "customers")]["batch_id"].value == "batch-customers"
    assert assets[DAILY_KEY]["row_count"].value == 1
    assert assets[DAILY_KEY]["snapshot_id"].value == "9007199254740999"
    [check] = result.get_asset_check_evaluations()
    assert check.check_name == "silver_quality"
    assert check.passed


def test_failed_quality_blocks_gold_and_rebuild_reuses_bronze(fake_spark):
    daily = defs.resolve_job_def("banking_daily")
    original = daily.execute_in_process(run_config=RUN_CONFIG)
    gold = materializations(original)[DAILY_KEY]

    fake_spark.quality_failed = True
    fake_spark.calls.clear()
    rebuild = defs.resolve_job_def("banking_rebuild")
    failed = rebuild.execute_in_process(run_config=RUN_CONFIG, raise_on_error=False)

    assert not failed.success
    assert [script for script, _ in fake_spark.calls] == [SILVER_LOADER, SILVER_GATE]
    assert DAILY_KEY not in materializations(failed)
    [check] = failed.get_asset_check_evaluations()
    assert not check.passed

    fake_spark.quality_failed = False
    fake_spark.calls.clear()
    retried = rebuild.execute_in_process(run_config=RUN_CONFIG)

    assert retried.success
    assert [script for script, _ in fake_spark.calls] == [SILVER_LOADER, SILVER_GATE, DAILY_GOLD]
    retry_gold = materializations(retried)[DAILY_KEY]
    for key in ("input_id", "row_count", "source_snapshots", "reconciliation"):
        assert retry_gold[key].value == gold[key].value


def test_gold_refresh_still_checks_the_existing_silver_assets(fake_spark):
    fake_spark.quality_failed = True
    refresh = defs.resolve_job_def("banking_refresh_gold")
    result = refresh.execute_in_process(run_config=RUN_CONFIG, raise_on_error=False)

    assert not result.success
    assert [script for script, _ in fake_spark.calls] == [SILVER_GATE]
    assert not materializations(result)
    [check] = result.get_asset_check_evaluations()
    assert not check.passed


def test_complete_batch_only_builds_gold_marts_after_daily_gold(fake_spark):
    job = defs.resolve_job_def("banking_batch")
    result = job.execute_in_process(run_config=RUN_CONFIG)

    assert result.success
    scripts = [script for script, _ in fake_spark.calls]
    assert scripts[:7] == [*BRONZE_SCRIPTS, SILVER_LOADER, SILVER_GATE, DAILY_GOLD]
    assert set(scripts[7:]) == {ROLLUPS, TOP_TRANSACTIONS}
    assert {key[-1] for key in materializations(result) if key[0] == "gold"} == {
        DAILY_KEY[-1], *ROLLUP_TABLES, "monthly_top_transactions",
    }

    fake_spark.quality_failed = True
    fake_spark.calls.clear()
    failed = job.execute_in_process(run_config=RUN_CONFIG, raise_on_error=False)

    assert not failed.success
    assert [script for script, _ in fake_spark.calls] == [*BRONZE_SCRIPTS, SILVER_LOADER, SILVER_GATE]
    assert all(key[0] != "gold" for key in materializations(failed))
