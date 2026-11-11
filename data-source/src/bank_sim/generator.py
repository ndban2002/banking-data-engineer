from collections import deque
from datetime import UTC, datetime, timedelta
import random
import signal
import threading
import time
from uuid import UUID, uuid4, uuid5

from .config import Config, MAX_BIGINT, SCENARIOS, positive_int, positive_number
from .db import Database
from .profiles import payment_amount
from .service import BankingService, TransactionRequest
from .workload import RetailSampler, history_times


class WorkloadGenerator:
    def __init__(self, db: Database, config: Config) -> None:
        self.db = db
        self.config = config
        self.service = BankingService(db)
        self.rng = random.Random(config.seed.random_seed)
        self.clock_rng = random.Random(f'{config.seed.random_seed}:clock')
        self.recent: deque[TransactionRequest] = deque(maxlen=1000)
        self.stop = threading.Event()
        self.stats = {"run_id": str(uuid4()), "attempts": 0, "succeeded": 0,
                      "failed": 0, "retries": 0, "recovered": 0,
                      "scenario_attempts": {name: 0 for name in SCENARIOS},
                      "fallback_payments": 0}

    def _inventory(self, conn) -> None:
        self.customers = conn.execute(self.db.query(
            "SELECT a.id, c.region FROM {schema}.accounts a "
            "JOIN {schema}.customers c ON c.id = a.customer_id ORDER BY a.id"
        )).fetchall()
        self.merchants = conn.execute(self.db.query("""
            SELECT a.id, m.channel, m.mcc, m.region FROM {schema}.accounts a
            JOIN {schema}.merchants m ON m.id = a.merchant_id ORDER BY a.id
        """)).fetchall()
        if not self.customers or not self.merchants:
            raise ValueError("no customer/merchant accounts; run init-db and seed first")
        self.retail = RetailSampler(self.customers, self.merchants,
                                    self.config.seed.random_seed, self.config.generator)

    def _request(self, conn, occurred_at: datetime) -> tuple[TransactionRequest, bool]:
        scenario = self.rng.choices(SCENARIOS, [self.config.generator.scenarios[s] for s in SCENARIOS])[0]
        self.stats['scenario_attempts'][scenario] += 1
        if scenario == "retry" and self.recent:
            # Most retries concern a recent request; retain its exact id and payload.
            offset = min(len(self.recent) - 1, int(self.rng.expovariate(0.2)))
            return self.recent[-1 - offset], True
        request_id = uuid5(UUID(self.stats["run_id"]), str(self.stats["attempts"]))
        if scenario == "refund":
            available_age = occurred_at - datetime.min.replace(tzinfo=UTC)
            minimum_age = timedelta(seconds=self.config.generator.refund_min_age_seconds)
            maximum_age = min(available_age, timedelta(days=self.config.generator.refund_max_age_days))
            candidates = []
            if available_age >= minimum_age:
                candidates = conn.execute(self.db.query("""
                SELECT p.* FROM {schema}.transactions p
                WHERE p.kind = 'PAYMENT' AND p.status = 'SUCCEEDED'
                    AND p.occurred_at >= %s AND p.occurred_at <= %s
                    AND NOT EXISTS (
                        SELECT 1 FROM {schema}.transactions r
                        WHERE r.original_transaction_id = p.id AND r.status = 'SUCCEEDED'
                    )
                ORDER BY p.occurred_at DESC, p.id DESC LIMIT 1000
                """), (occurred_at - maximum_age, occurred_at - minimum_age)).fetchall()
            if candidates:
                original = self.rng.choice(candidates)
                return TransactionRequest(
                    request_id, original["destination_account_id"], original["source_account_id"],
                    original["amount"], original["channel"], occurred_at,
                    kind="REFUND", original_transaction_id=original["id"],
                ), False

        if scenario in ('refund', 'retry'):
            self.stats['fallback_payments'] += 1
        config = self.config.generator
        if config.profile == 'retail':
            source, destination = self.retail.choose(self.rng, occurred_at)
            amount = payment_amount(self.rng, destination['mcc'], config.amount_min,
                                    config.amount_max, config.amount_rounding)
        else:
            source, destination = self.rng.choice(self.customers), self.rng.choice(self.merchants)
            amount = self.rng.randint(config.amount_min, config.amount_max)
        if scenario == "insufficient_funds":
            balance = conn.execute(self.db.query(
                "SELECT balance FROM {schema}.accounts WHERE id = %s"
            ), (source["id"],)).fetchone()["balance"]
            if balance == MAX_BIGINT:
                raise ValueError('cannot generate an insufficient-funds amount above BIGINT maximum')
            # Overshoot by a shopping-sized amount rather than an artificial one VND.
            quantum = config.amount_rounding
            amount = min(MAX_BIGINT, ((balance + amount + quantum - 1) // quantum) * quantum)
        return TransactionRequest(
            request_id, source["id"], destination["id"], amount,
            destination["channel"], occurred_at, requested_decline=scenario == "decline",
        ), False

    def _attempt(self, conn, occurred_at: datetime) -> None:
        request, retry = self._request(conn, occurred_at)
        result = self.service.submit(request, conn=conn)
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
        if mode == "history" and (end - start) // timedelta(microseconds=1) < count:
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
                    for occurred_at in history_times(
                        start, end, count, profile=self.config.generator.profile,
                        timezone=self.config.generator.timezone, rng=self.clock_rng,
                    ):
                        if self.stop.is_set():
                            break
                        self._attempt(conn, occurred_at)
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
                        jitter = self.clock_rng.uniform(0.5, 1.5) if self.config.generator.realtime_jitter else 1
                        next_due = attempt_started + jitter / rate
                        if deadline is not None:
                            next_due = min(next_due, deadline)
                        self.stop.wait(max(0.0, next_due - time.monotonic()))
        finally:
            for signum, handler in old_handlers.items():
                signal.signal(signum, handler)
        self.stats["stopped"] = self.stop.is_set()
        self.stats["elapsed_seconds"] = round(time.monotonic() - started, 3)
        return self.stats.copy()
