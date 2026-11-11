from dataclasses import dataclass
from datetime import datetime
from collections.abc import Callable
from uuid import UUID, uuid5

from psycopg import Connection
from psycopg.pq import TransactionStatus

from .config import positive_int
from .db import Database

TRANSACTION_NAMESPACE = UUID("83c3e1eb-90e8-46b2-81f0-643913ccdf1b")


class IdempotencyConflict(ValueError):
    """The same request key was used for a different payload."""


@dataclass(frozen=True)
class TransactionRequest:
    request_id: UUID
    source_account_id: UUID
    destination_account_id: UUID
    amount: int
    channel: str
    occurred_at: datetime
    kind: str = "PAYMENT"
    original_transaction_id: UUID | None = None
    requested_decline: bool = False

    def __post_init__(self) -> None:
        positive_int("amount", self.amount)
        for value in (self.request_id, self.source_account_id, self.destination_account_id):
            if not isinstance(value, UUID):
                raise ValueError("request and account IDs must be UUIDs")
        if self.original_transaction_id is not None and not isinstance(self.original_transaction_id, UUID):
            raise ValueError("original_transaction_id must be a UUID")
        if self.source_account_id == self.destination_account_id:
            raise ValueError("source and destination accounts must differ")
        if self.kind not in ("PAYMENT", "REFUND") or self.channel not in ("QR", "POS"):
            raise ValueError("invalid transaction kind or channel")
        if (self.kind == "REFUND") != (self.original_transaction_id is not None):
            raise ValueError("only refunds require an original transaction")
        if self.original_transaction_id == self.id:
            raise ValueError("a refund cannot reference itself")
        if not isinstance(self.occurred_at, datetime) or self.occurred_at.utcoffset() is None:
            raise ValueError("occurred_at must include a timezone")
        if type(self.requested_decline) is not bool:
            raise ValueError("requested_decline must be boolean")

    @property
    def id(self) -> UUID:
        return uuid5(TRANSACTION_NAMESPACE, str(self.request_id))

    def payload(self) -> dict:
        return dict(self.__dict__)


class BankingService:
    def __init__(self, db: Database) -> None:
        self.db = db

    def submit(self, request: TransactionRequest, *, conn: Connection | None = None) -> dict:
        """Submit with an owned connection or an idle connection held by one worker."""
        if conn is None:
            with self.db.connect() as owned:
                return self.submit(request, conn=owned)
        # An enclosing transaction would turn these blocks into savepoints,
        # preventing the PENDING intent from being committed independently.
        if not conn.autocommit or conn.info.transaction_status != TransactionStatus.IDLE:
            raise ValueError("submit requires an idle connection with autocommit enabled")

        # Persist the intent separately. A process failure in settlement leaves a
        # recoverable PENDING row, never a partially applied movement of money.
        with conn.transaction():
            payload = request.payload()
            conn.execute(self.db.query("""
                INSERT INTO {schema}.transactions (
                    id, request_id, kind, source_account_id, destination_account_id,
                    amount, channel, occurred_at, original_transaction_id, requested_decline
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """), (request.id, request.request_id, request.kind, request.source_account_id,
                    request.destination_account_id, request.amount, request.channel,
                    request.occurred_at, request.original_transaction_id, request.requested_decline))
            existing = conn.execute(self.db.query(
                "SELECT * FROM {schema}.transactions WHERE request_id = %s"
            ), (request.request_id,)).fetchone()
            # Both the primary ID (derived from request_id) and request_id
            # are unique. Concurrent identical inserts can conflict on
            # either index, so the INSERT handles all unique conflicts.
            if existing is None:
                raise ValueError("request identifier collides with an existing transaction")
            if any(existing[key] != value for key, value in payload.items()):
                raise IdempotencyConflict("request_id already exists with a different payload")
        return self._settle(conn, existing["id"])

    def resume_pending(self, *, should_stop: Callable[[], bool] | None = None) -> int:
        with self.db.connect() as conn:
            # Fetch bounded batches; do not hold a transaction across the run.
            recovered = 0
            while True:
                if should_stop and should_stop():
                    return recovered
                pending = conn.execute(self.db.query("""
                    SELECT id FROM {schema}.transactions WHERE status = 'PENDING'
                    ORDER BY created_at, id LIMIT 500
                """)).fetchall()
                if not pending:
                    return recovered
                for row in pending:
                    if should_stop and should_stop():
                        return recovered
                    self._settle(conn, row["id"])
                    recovered += 1

    def _settle(self, conn, transaction_id: UUID) -> dict:
        with conn.transaction():
            tx = conn.execute(self.db.query(
                "SELECT * FROM {schema}.transactions WHERE id = %s FOR NO KEY UPDATE"
            ), (transaction_id,)).fetchone()
            if tx["status"] != "PENDING":
                return tx

            if tx["kind"] == "REFUND":
                # All refunds for a payment serialize on its original row.
                original = conn.execute(self.db.query(
                    "SELECT * FROM {schema}.transactions WHERE id = %s FOR NO KEY UPDATE"
                ), (tx["original_transaction_id"],)).fetchone()
                if not self._valid_refund(tx, original):
                    return self._finish(conn, tx["id"], "INVALID_REFUND")
                refunded = conn.execute(self.db.query("""
                    SELECT id FROM {schema}.transactions
                    WHERE original_transaction_id = %s AND status = 'SUCCEEDED'
                """), (original["id"],)).fetchone()
                if refunded:
                    return self._finish(conn, tx["id"], "ALREADY_REFUNDED")

            # No primary/foreign key values change during settlement. NO KEY
            # UPDATE serializes money movements while allowing FK KEY SHARE
            # checks from other intents, avoiding an intent/settlement deadlock.
            accounts = conn.execute(self.db.query("""
                SELECT * FROM {schema}.accounts WHERE id = ANY(%s) ORDER BY id FOR NO KEY UPDATE
            """), ([tx["source_account_id"], tx["destination_account_id"]],)).fetchall()
            by_id = {account["id"]: account for account in accounts}
            source, destination = by_id[tx["source_account_id"]], by_id[tx["destination_account_id"]]
            if tx["kind"] == "PAYMENT":
                if source["customer_id"] is None or destination["merchant_id"] is None:
                    return self._finish(conn, tx["id"], "INVALID_ACCOUNTS")
                merchant = conn.execute(self.db.query(
                    "SELECT channel FROM {schema}.merchants WHERE id = %s"
                ), (destination["merchant_id"],)).fetchone()
                if merchant["channel"] != tx["channel"]:
                    return self._finish(conn, tx["id"], "INVALID_CHANNEL")
            if tx["requested_decline"]:
                return self._finish(conn, tx["id"], "SIMULATED_DECLINE")
            if source["balance"] < tx["amount"]:
                return self._finish(conn, tx["id"], "INSUFFICIENT_FUNDS")

            conn.execute(self.db.query("""
                UPDATE {schema}.accounts SET balance = balance - %s, updated_at = clock_timestamp()
                WHERE id = %s
            """), (tx["amount"], source["id"]))
            conn.execute(self.db.query("""
                UPDATE {schema}.accounts SET balance = balance + %s, updated_at = clock_timestamp()
                WHERE id = %s
            """), (tx["amount"], destination["id"]))
            return self._finish(conn, tx["id"])

    @staticmethod
    def _valid_refund(tx: dict, original: dict | None) -> bool:
        return bool(original and original["kind"] == "PAYMENT" and original["status"] == "SUCCEEDED"
                    and tx["source_account_id"] == original["destination_account_id"]
                    and tx["destination_account_id"] == original["source_account_id"]
                    and tx["amount"] == original["amount"]
                    and tx["channel"] == original["channel"]
                    and tx["occurred_at"] >= original["occurred_at"])

    def _finish(self, conn, transaction_id: UUID, error: str | None = None) -> dict:
        return conn.execute(self.db.query("""
            UPDATE {schema}.transactions SET status = %s, error_code = %s, updated_at = clock_timestamp()
            WHERE id = %s RETURNING *
        """), ("FAILED" if error else "SUCCEEDED", error, transaction_id)).fetchone()
