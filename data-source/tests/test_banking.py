from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import threading
from unittest.mock import patch
from uuid import uuid4

import pytest
from psycopg.pq import TransactionStatus

from bank_sim.service import BankingService, IdempotencyConflict, TransactionRequest
from bank_sim.verify import verify

pytestmark = pytest.mark.integration


def payment(actors, amount=100, **kwargs):
    customer, merchant, channel = actors
    return TransactionRequest(uuid4(), customer, merchant, amount, channel, datetime.now(UTC), **kwargs)


def refund(original, **changes):
    request = TransactionRequest(
        uuid4(), original["destination_account_id"], original["source_account_id"],
        original["amount"], original["channel"], datetime.now(UTC),
        kind="REFUND", original_transaction_id=original["id"],
    )
    return replace(request, **changes)


def balances(db):
    with db.connect() as conn:
        return {row["id"]: row["balance"] for row in conn.execute(db.query(
            "SELECT id, balance FROM {schema}.accounts ORDER BY id"
        ))}


def concurrently(service, requests):
    barrier = threading.Barrier(len(requests))

    def submit(request):
        barrier.wait(timeout=10)
        return service.submit(request)

    with ThreadPoolExecutor(max_workers=len(requests)) as pool:
        return list(pool.map(submit, requests))


def test_seed_and_init_are_idempotent(db, actors, test_config):
    BankingService(db).submit(payment(actors))
    before = balances(db)
    db.init_db()
    assert db.seed(test_config) == {"customers": 0, "merchants": 0, "accounts": 0}
    assert balances(db) == before
    assert verify(db)["ok"]


def test_payment_and_full_refund(db, actors):
    service = BankingService(db)
    before = balances(db)
    paid = service.submit(payment(actors, 400))
    assert paid["status"] == "SUCCEEDED"
    assert balances(db)[actors[0]] == before[actors[0]] - 400
    assert balances(db)[actors[1]] == before[actors[1]] + 400
    returned = service.submit(refund(paid))
    assert returned["status"] == "SUCCEEDED"
    assert balances(db) == before
    again = service.submit(refund(paid))
    assert again["error_code"] == "ALREADY_REFUNDED"
    assert balances(db) == before
    assert verify(db)["ok"]


@pytest.mark.parametrize("amount,decline,reason", [
    (1_001, False, "INSUFFICIENT_FUNDS"), (100, True, "SIMULATED_DECLINE"),
])
def test_failed_payment_and_its_retry_never_move_funds(db, actors, amount, decline, reason):
    service = BankingService(db)
    before = balances(db)
    request = payment(actors, amount, requested_decline=decline)
    failed = service.submit(request)
    assert failed["status"] == "FAILED"
    assert failed["error_code"] == reason
    assert service.submit(request) == failed
    assert balances(db) == before
    assert verify(db)["ok"]


def test_idempotency_payload_mismatch_is_rejected(db, actors):
    service = BankingService(db)
    request = payment(actors)
    original = service.submit(request)
    assert service.submit(request) == original
    with pytest.raises(IdempotencyConflict):
        service.submit(replace(request, amount=101))
    assert verify(db)["ok"]


def test_duplicate_requests_concurrently_move_money_once(db, actors):
    service = BankingService(db)
    request = payment(actors, 700)
    results = concurrently(service, [request, request])
    assert results[0]["id"] == results[1]["id"]
    assert all(result["status"] == "SUCCEEDED" for result in results)
    assert balances(db)[actors[0]] == 300
    assert sum(row["count"] for row in verify(db)["transactions"]) == 1
    assert verify(db)["ok"]


def test_many_concurrent_retries_handle_both_unique_indexes(db, actors):
    service = BankingService(db)
    request = payment(actors, 700)
    results = concurrently(service, [request] * 12)
    assert len({row["id"] for row in results}) == 1
    assert all(row["status"] == "SUCCEEDED" for row in results)
    assert balances(db)[actors[0]] == 300
    assert sum(row["count"] for row in verify(db)["transactions"]) == 1
    assert verify(db)["ok"]


def test_competing_payments_cannot_overdraw(db, actors):
    service = BankingService(db)
    results = concurrently(service, [payment(actors, 700), payment(actors, 700)])
    assert sorted(row["status"] for row in results) == ["FAILED", "SUCCEEDED"]
    assert next(row for row in results if row["status"] == "FAILED")["error_code"] == "INSUFFICIENT_FUNDS"
    assert balances(db)[actors[0]] == 300
    assert verify(db)["ok"]


def test_competing_refunds_cannot_return_money_twice(db, actors):
    service = BankingService(db)
    before = balances(db)
    original = service.submit(payment(actors, 400))
    results = concurrently(service, [refund(original), refund(original)])
    assert sorted(row["status"] for row in results) == ["FAILED", "SUCCEEDED"]
    assert next(row for row in results if row["status"] == "FAILED")["error_code"] == "ALREADY_REFUNDED"
    assert balances(db) == before
    assert verify(db)["ok"]


def test_new_refund_intent_can_commit_while_another_is_settling(db, actors):
    """Regression: FK KEY SHARE locks must not deadlock with settlement locks."""
    first, second = BankingService(db), BankingService(db)
    original = first.submit(payment(actors, 400))
    first_request, second_request = refund(original), refund(original)
    first_is_settling, second_intent_saved = threading.Event(), threading.Event()
    finish, settle = first._finish, second._settle

    def hold_first_settlement(conn, transaction_id, error=None):
        first_is_settling.set()
        assert second_intent_saved.wait(5), "foreign-key validation blocked on settlement locks"
        return finish(conn, transaction_id, error)

    def mark_second_intent_saved(conn, transaction_id):
        second_intent_saved.set()
        return settle(conn, transaction_id)

    with patch.object(first, "_finish", side_effect=hold_first_settlement), patch.object(
        second, "_settle", side_effect=mark_second_intent_saved
    ), ThreadPoolExecutor(max_workers=2) as pool:
        first_result = pool.submit(first.submit, first_request)
        assert first_is_settling.wait(5)
        second_result = pool.submit(second.submit, second_request)
        assert first_result.result(timeout=10)["status"] == "SUCCEEDED"
        assert second_result.result(timeout=10)["error_code"] == "ALREADY_REFUNDED"
    assert verify(db)["ok"]


@pytest.mark.parametrize("change", ["amount", "time", "failed_original", "accounts", "channel"])
def test_invalid_refund_is_recorded_as_failure(db, actors, change):
    service = BankingService(db)
    original = service.submit(payment(actors, requested_decline=change == "failed_original"))
    changes = {
        "amount": {"amount": 50},
        "time": {"occurred_at": original["occurred_at"] - timedelta(seconds=1)},
        "accounts": {"source_account_id": original["source_account_id"],
                     "destination_account_id": original["destination_account_id"]},
        "channel": {"channel": "QR" if original["channel"] == "POS" else "POS"},
        "failed_original": {},
    }[change]
    before = balances(db)
    result = service.submit(refund(original, **changes))
    assert result["error_code"] == "INVALID_REFUND"
    assert balances(db) == before
    assert verify(db)["ok"]


@pytest.mark.parametrize("failure_point", ["after_debit", "before_final_status"])
def test_database_transaction_rolls_back_and_pending_is_recoverable(db, actors, failure_point):
    service = BankingService(db)
    before = balances(db)
    request = payment(actors, 400)
    query = db.query

    def fail_credit(statement):
        if "balance = balance +" in statement:
            raise RuntimeError("injected failure after debit")
        return query(statement)

    target = patch.object(db, "query", side_effect=fail_credit) if failure_point == "after_debit" else patch.object(
        service, "_finish", side_effect=RuntimeError("injected failure before final status")
    )
    with target, pytest.raises(RuntimeError):
        service.submit(request)
    assert balances(db) == before
    report = verify(db)
    assert report["pending"] == 1
    assert not report["ok"]
    assert report["account_mismatch_count"] == 0
    assert service.resume_pending() == 1
    assert service.submit(request)["status"] == "SUCCEEDED"
    assert service.resume_pending() == 0
    assert verify(db)["ok"]


def test_invalid_payment_direction_does_not_change_balances(db, actors):
    request = payment(actors)
    before = balances(db)
    result = BankingService(db).submit(replace(
        request, source_account_id=request.destination_account_id,
        destination_account_id=request.source_account_id,
    ))
    assert result["error_code"] == "INVALID_ACCOUNTS"
    assert balances(db) == before


def test_recovery_honors_stop_between_transactions(db, actors):
    service = BankingService(db)
    for _ in range(2):
        with patch.object(service, "_finish", side_effect=RuntimeError("simulate process failure")):
            with pytest.raises(RuntimeError):
                service.submit(payment(actors, 100))
    completed = []
    finish = service._finish

    def record_completion(*args, **kwargs):
        result = finish(*args, **kwargs)
        completed.append(result)
        return result

    with patch.object(service, "_finish", side_effect=record_completion):
        assert service.resume_pending(should_stop=lambda: bool(completed)) == 1
    assert verify(db)["pending"] == 1
    assert service.resume_pending() == 1
    assert verify(db)["ok"]


def test_verify_detects_per_account_corruption_even_if_total_is_preserved(db, actors):
    with db.connect() as conn, conn.transaction():
        conn.execute(db.query("UPDATE {schema}.accounts SET balance = balance - 1 WHERE id = %s"), (actors[0],))
        conn.execute(db.query("UPDATE {schema}.accounts SET balance = balance + 1 WHERE id = %s"), (actors[1],))
    report = verify(db)
    assert report["opening_total"] == report["current_total"]
    assert report["account_mismatch_count"] == 2
    assert not report["ok"]


def test_reused_connection_preserves_idempotency_and_remains_usable(db, actors):
    service = BankingService(db)
    request = payment(actors, 300)
    with db.connect() as conn:
        # A supplied connection must avoid creating additional connections.
        with patch.object(db, "connect", side_effect=AssertionError("unexpected connection")):
            first = service.submit(request, conn=conn)
            assert service.submit(request, conn=conn) == first
            with pytest.raises(IdempotencyConflict):
                service.submit(replace(request, amount=301), conn=conn)
            assert conn.info.transaction_status == TransactionStatus.IDLE
            assert service.submit(payment(actors, 200), conn=conn)["status"] == "SUCCEEDED"
        assert not conn.closed
    assert balances(db)[actors[0]] == 500
    assert verify(db)["ok"]


@pytest.mark.parametrize("enclosing_transaction", [True, False])
def test_submit_rejects_connection_that_cannot_commit_intent(db, actors, enclosing_transaction):
    service = BankingService(db)
    request = payment(actors)
    with db.connect() as conn:
        if enclosing_transaction:
            with conn.transaction(), pytest.raises(ValueError, match="idle connection"):
                service.submit(request, conn=conn)
        else:
            conn.autocommit = False
            with pytest.raises(ValueError, match="autocommit"):
                service.submit(request, conn=conn)
            conn.autocommit = True
        assert conn.execute(db.query(
            "SELECT id FROM {schema}.transactions WHERE id = %s"
        ), (request.id,)).fetchone() is None
    assert verify(db)["ok"]


def test_reused_connection_rolls_back_money_and_retains_committed_pending(db, actors):
    service = BankingService(db)
    request = payment(actors, 400)
    before = balances(db)
    with db.connect() as conn:
        with patch.object(service, "_finish", side_effect=RuntimeError("injected settlement failure")):
            with pytest.raises(RuntimeError):
                service.submit(request, conn=conn)
        assert conn.info.transaction_status == TransactionStatus.IDLE
        # Separate connections see committed PENDING, but no movement of money.
        assert balances(db) == before
        report = verify(db)
        assert report["pending"] == 1
        assert report["account_mismatch_count"] == 0
        assert service.submit(payment(actors, 100), conn=conn)["status"] == "SUCCEEDED"
        assert service.submit(request, conn=conn)["status"] == "SUCCEEDED"
        assert service.submit(request, conn=conn)["status"] == "SUCCEEDED"
    assert balances(db)[actors[0]] == before[actors[0]] - 500
    assert service.resume_pending() == 0
    assert verify(db)["ok"]


def test_verify_limits_mismatch_details_without_losing_total(db, test_config):
    db.seed(replace(test_config, seed=replace(test_config.seed, customers=105)))
    with db.connect() as conn:
        conn.execute(db.query("UPDATE {schema}.accounts SET balance = balance + 1 WHERE customer_id IS NOT NULL"))
    report = verify(db)
    assert report["account_mismatch_count"] == 105
    assert len(report["account_mismatches"]) == 100
    assert not report["ok"]
