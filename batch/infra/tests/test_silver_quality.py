"""Exercise relational check predicates; actual Spark/Iceberg tests run separately."""

import json
import sqlite3
import sys
from types import SimpleNamespace

import pytest

from job_reporting import REPORT_PREFIX
from silver_quality import main, quality_queries


@pytest.fixture
def quality_database():
    connection = sqlite3.connect(":memory:")
    connection.executescript("""
        CREATE TABLE customers (customer_id TEXT);
        CREATE TABLE merchants (merchant_id TEXT);
        CREATE TABLE accounts (
            account_id TEXT, customer_id TEXT, merchant_id TEXT,
            opening_balance_vnd BIGINT, balance_vnd BIGINT
        );
        CREATE TABLE transactions (
            transaction_id TEXT, request_id TEXT, transaction_type TEXT,
            source_account_id TEXT, destination_account_id TEXT, amount_vnd BIGINT,
            channel TEXT, status TEXT, original_transaction_id TEXT, occurred_at TEXT
        );
        INSERT INTO customers VALUES ('customer');
        INSERT INTO merchants VALUES ('merchant');
        INSERT INTO accounts VALUES ('payer', 'customer', NULL, 1000, 900);
        INSERT INTO accounts VALUES ('payee', NULL, 'merchant', 0, 100);
        INSERT INTO transactions VALUES
            ('payment', 'request-1', 'PAYMENT', 'payer', 'payee', 100,
             'QR', 'SUCCEEDED', NULL, '2026-11-01T00:00:00');
    """)
    yield connection
    connection.close()


def counts(connection):
    views = {name: name for name in ("customers", "merchants", "accounts", "transactions")}
    return {rule: len(connection.execute(sql).fetchall())
            for rule, sql in quality_queries(views).items()}


def test_complete_balanced_fixture_passes(quality_database):
    assert not any(counts(quality_database).values())


def test_failed_invalid_refund_is_not_a_successful_movement(quality_database):
    # Invalid direction, amount, channel and time are legitimate reasons to reject
    # a refund request. They do not make the stored FAILED request corrupt.
    quality_database.execute("""
        INSERT INTO transactions VALUES
            ('failed-refund', 'request-2', 'REFUND', 'payer', 'payee', 30,
             'POS', 'FAILED', 'payment', '2026-10-01T00:00:00')
    """)
    assert not any(counts(quality_database).values())


def test_pending_does_not_change_reconciled_balances(quality_database):
    quality_database.execute("""
        INSERT INTO transactions VALUES
            ('pending', 'request-2', 'PAYMENT', 'payer', 'payee', 50,
             'QR', 'PENDING', NULL, '2026-11-01T00:01:00')
    """)
    assert not any(counts(quality_database).values())


def test_invalid_successful_refund_and_balance_drift_are_visible(quality_database):
    quality_database.execute("""
        INSERT INTO transactions VALUES
            ('bad-refund', 'request-2', 'REFUND', 'payer', 'payee', 30,
             'POS', 'SUCCEEDED', 'payment', '2026-10-01T00:00:00')
    """)
    result = counts(quality_database)
    assert result["successful_refund_consistency"] == 1
    assert result["account_balance_reconciliation"] == 2


def test_balanced_successful_refund_passes(quality_database):
    quality_database.execute("""
        INSERT INTO transactions VALUES
            ('refund', 'request-2', 'REFUND', 'payee', 'payer', 100,
             'QR', 'SUCCEEDED', 'payment', '2026-11-01T00:01:00')
    """)
    quality_database.execute("UPDATE accounts SET balance_vnd = opening_balance_vnd")
    assert not any(counts(quality_database).values())


def test_account_mismatch_is_found_even_when_total_balance_matches(quality_database):
    quality_database.execute("UPDATE accounts SET balance_vnd = 850 WHERE account_id = 'payer'")
    quality_database.execute("UPDATE accounts SET balance_vnd = 150 WHERE account_id = 'payee'")
    assert counts(quality_database)["account_balance_reconciliation"] == 2


def test_missing_reference_is_checked_for_failed_refund_too(quality_database):
    quality_database.execute("""
        INSERT INTO transactions VALUES
            ('failed-refund', 'request-2', 'REFUND', 'payee', 'payer', 100,
             'QR', 'FAILED', 'missing-payment', '2026-11-01T00:01:00')
    """)
    result = counts(quality_database)
    assert result["refund_reference"] == 1
    assert result["successful_refund_consistency"] == 0


def test_duplicate_business_keys_are_reported(quality_database):
    quality_database.execute("INSERT INTO customers VALUES ('customer')")
    quality_database.execute("INSERT INTO transactions SELECT * FROM transactions")
    result = counts(quality_database)
    assert result["customers_key_unique"] == 1
    assert result["transactions_key_unique"] == 1
    assert result["transaction_request_unique"] == 1


@pytest.mark.parametrize("orchestration", [False, True])
def test_quality_cli_emits_report_before_exiting_on_violation(monkeypatch, capsys, orchestration):
    stopped = []
    spark = SimpleNamespace(stop=lambda: stopped.append(True))
    builder = SimpleNamespace()
    builder.appName = lambda *_: builder
    builder.getOrCreate = lambda: spark
    monkeypatch.setitem(sys.modules, "pyspark.sql", SimpleNamespace(SparkSession=SimpleNamespace(builder=builder)))
    report = {"ok": False, "violations": {"account_balance_reconciliation": 1}}
    if orchestration:
        report["processing_violations"] = {"quarantined_rows": 0, "uncheckpointed_batches": 0}

    def check(spark_session, namespace, *, require_processed):
        assert spark_session is spark
        assert namespace == "silver_fixture"
        assert require_processed is orchestration
        return report

    monkeypatch.setattr("silver_quality.check_silver", check)
    args = ["--namespace", "silver_fixture"]
    if orchestration:
        args.extend(["--orchestration-report", "--require-processed"])
    with pytest.raises(SystemExit) as failure:
        main(args)

    assert failure.value.code == 1
    output = capsys.readouterr().out
    if orchestration:
        assert output.startswith(REPORT_PREFIX)
        output = output[len(REPORT_PREFIX):]
    assert json.loads(output) == report
    assert stopped == [True]
