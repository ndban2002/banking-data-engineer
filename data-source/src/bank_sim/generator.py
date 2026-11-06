from collections import deque
from datetime import UTC, datetime
import random
import signal
import threading
import time
from uuid import UUID, uuid4, uuid5

from .config import Config, SCENARIOS, positive_int, positive_number
from .db import Database
from .service import BankingService, TransactionRequest


class WorkloadGenerator:
    def __init__(self, db: Database, config: Config) -> None:
        self.db = db
        self.config = config
        self.service = BankingService(db)
        self.rng = random.Random(config.seed.random_seed)
        self.recent: deque[TransactionRequest] = deque(maxlen=1000)
        self.stop = threading.Event()
        self.stats = {"run_id": str(uuid4()), "attempts": 0, "succeeded": 0,
                      "failed": 0, "retries": 0, "recovered": 0}

    def _inventory(self, conn) -> None:
        self.customers = conn.execute(self.db.query(
            "SELECT id FROM {schema}.accounts WHERE customer_id IS NOT NULL ORDER BY id"
        )).fetchall()
        self.merchants = conn.execute(self.db.query("""
            SELECT a.id, m.channel FROM {schema}.accounts a
            JOIN {schema}.merchants m ON m.id = a.merchant_id ORDER BY a.id
        """)).fetchall()
        if not self.customers or not self.merchants:
            raise ValueError("no customer/merchant accounts; run init-db and seed first")

    def _request(self, conn, occurred_at: datetime) -> tuple[TransactionRequest, bool]:
        scenario = self.rng.choices(SCENARIOS, [self.config.generator.scenarios[s] for s in SCENARIOS])[0]
        if scenario == "retry" and self.recent:
            return self.rng.choice(self.recent), True
        request_id = uuid5(UUID(self.stats["run_id"]), str(self.stats["attempts"]))
        if scenario == "refund":
            candidates = conn.execute(self.db.query("""
                SELECT p.* FROM {schema}.transactions p
                WHERE p.kind = 'PAYMENT' AND p.status = 'SUCCEEDED' AND p.occurred_at <= %s
                    AND NOT EXISTS (
                        SELECT 1 FROM {schema}.transactions r
                        WHERE r.original_transaction_id = p.id AND r.status = 'SUCCEEDED'
                    )
                ORDER BY p.occurred_at, p.id LIMIT 1000
            """), (occurred_at,)).fetchall()
            if candidates:
                original = self.rng.choice(candidates)
                return TransactionRequest(
                    request_id, original["destination_account_id"], original["source_account_id"],
                    original["amount"], original["channel"], occurred_at,
                    kind="REFUND", original_transaction_id=original["id"],
                ), False

        source, destination = self.rng.choice(self.customers), self.rng.choice(self.merchants)
        amount = self.rng.randint(self.config.generator.amount_min, self.config.generator.amount_max)
        if scenario == "insufficient_funds":
            balance = conn.execute(self.db.query(
                "SELECT balance FROM {schema}.accounts WHERE id = %s"
            ), (source["id"],)).fetchone()["balance"]
            amount = balance + 1
        return TransactionRequest(
            request_id, source["id"], destination["id"], amount,
            destination["channel"], occurred_at, requested_decline=scenario == "decline",
        ), False

    def _attempt(self, conn, occurred_at: datetime) -> None:
        request, retry = self._request(conn, occurred_at)
        result = self.service.submit(request)
        if not retry:
            self.recent.append(request)
        self.stats["attempts"] += 1
        self.stats["retries"] += int(retry)
        self.stats["succeeded" if result["status"] == "SUCCEEDED" else "failed"] += 1

    def run(self, *, mode: str, start: datetime | None = None, end: datetime | None = None,
            count: int | None = None, rate: float | None = None, duration: float | None = None) -> dict:
        if mode not in ("history", "realtime"):
            raise ValueError("mode must be history or realtime")
        if mode == "history":
            if start is None or end is None or start.utcoffset() is None or end.utcoffset() is None:
                raise ValueError("history requires timezone-aware start and end")
            start, end = start.astimezone(UTC), end.astimezone(UTC)
            if start >= end:
                raise ValueError("start must be before end")
            positive_int("count", count)
            if rate is not None or duration is not None:
                raise ValueError("rate and duration are only supported in realtime mode")
        elif start is not None or end is not None or count is not None:
            raise ValueError("start, end and count are only supported in history mode")
        rate = self.config.generator.rate if rate is None else rate
        positive_number("rate", rate)
        if duration is not None:
            positive_number("duration", duration)
        if mode == "history" and (end - start).total_seconds() * 1_000_000 < count:
            raise ValueError("history range must allow at least one microsecond per request")

        # Handlers request a graceful stop; they never interrupt the two SQL updates.
        old_handlers = {}
        if threading.current_thread() is threading.main_thread():
            for signum in (signal.SIGINT, signal.SIGTERM):
                old_handlers[signum] = signal.signal(signum, lambda *_: self.stop.set())
        started = time.monotonic()
        try:
            self.stats["recovered"] = self.service.resume_pending(should_stop=self.stop.is_set)
            with self.db.connect() as conn:
                self._inventory(conn)
                if mode == "history":
                    interval = end - start
                    for index in range(count):
                        if self.stop.is_set():
                            break
                        self._attempt(conn, start + interval * index / count)
                else:
                    # Use monotonic time for pacing, UTC wall clock for business time.
                    epoch = time.monotonic()
                    deadline = None if duration is None else epoch + duration
                    while not self.stop.is_set():
                        attempt_started = time.monotonic()
                        if deadline is not None and attempt_started >= deadline:
                            break
                        self._attempt(conn, datetime.now(UTC))
                        # Never catch up in a burst when the DB is slower than the target rate.
                        next_due = attempt_started + 1 / rate
                        if deadline is not None:
                            next_due = min(next_due, deadline)
                        self.stop.wait(max(0.0, next_due - time.monotonic()))
        finally:
            for signum, handler in old_handlers.items():
                signal.signal(signum, handler)
        self.stats["stopped"] = self.stop.is_set()
        self.stats["elapsed_seconds"] = round(time.monotonic() - started, 3)
        return self.stats.copy()
