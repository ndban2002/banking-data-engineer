from dataclasses import dataclass, field
from pathlib import Path
import math
import re
import tomllib
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .profiles import opening_balance_radius

MAX_BIGINT = 2**63 - 1
SCENARIOS = ("normal", "insufficient_funds", "decline", "refund", "retry")


def positive_int(name: str, value: object, *, allow_zero: bool = False) -> None:
    if type(value) is not int or not (0 if allow_zero else 1) <= value <= MAX_BIGINT:
        raise ValueError(f"{name} must be an integer between {0 if allow_zero else 1} and {MAX_BIGINT}")


def positive_number(name: str, value: object) -> None:
    try:
        valid = type(value) in (int, float) and math.isfinite(value) and value > 0
    except OverflowError:
        valid = False
    if not valid:
        raise ValueError(f"{name} must be a finite positive number")


def probability(name: str, value: object) -> None:
    if type(value) not in (int, float) or not 0 <= value <= 1:
        raise ValueError(f"{name} must be a finite number between 0 and 1")


@dataclass(frozen=True)
class SeedConfig:
    random_seed: int = 42
    customers: int = 100
    merchants: int = 20
    opening_balance: int = 100_000_000
    opening_balance_spread: float = 0.0

    def __post_init__(self) -> None:
        positive_int("random_seed", self.random_seed, allow_zero=True)
        positive_int("customers", self.customers)
        positive_int("merchants", self.merchants)
        positive_int("opening_balance", self.opening_balance, allow_zero=True)
        probability("opening_balance_spread", self.opening_balance_spread)
        maximum_balance = self.opening_balance + opening_balance_radius(
            self.opening_balance, self.opening_balance_spread
        )
        if self.customers * maximum_balance >= MAX_BIGINT:
            raise ValueError("total opening funds must be below PostgreSQL BIGINT maximum")


@dataclass(frozen=True)
class GeneratorConfig:
    rate: float = 1.0
    amount_min: int = 10_000
    amount_max: int = 500_000
    profile: str = "retail"
    timezone: str = "Asia/Ho_Chi_Minh"
    amount_rounding: int = 1000
    local_merchant_probability: float = 0.85
    repeat_merchant_probability: float = 0.6
    refund_min_age_seconds: int = 300
    refund_max_age_days: int = 30
    realtime_jitter: bool = True
    scenarios: dict[str, int] = field(default_factory=lambda: dict(zip(SCENARIOS, (90, 3, 3, 2, 2))))

    def __post_init__(self) -> None:
        positive_number("rate", self.rate)
        positive_int("amount_min", self.amount_min)
        positive_int("amount_max", self.amount_max)
        if self.amount_min > self.amount_max:
            raise ValueError("amount_min must not exceed amount_max")
        if self.profile not in ("retail", "uniform"):
            raise ValueError("profile must be retail or uniform")
        if not isinstance(self.timezone, str):
            raise ValueError("timezone must be an IANA timezone name")
        try:
            ZoneInfo(self.timezone)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError("timezone must be an available IANA timezone name") from exc
        positive_int("amount_rounding", self.amount_rounding)
        probability("local_merchant_probability", self.local_merchant_probability)
        probability("repeat_merchant_probability", self.repeat_merchant_probability)
        positive_int("refund_min_age_seconds", self.refund_min_age_seconds, allow_zero=True)
        positive_int("refund_max_age_days", self.refund_max_age_days)
        if self.refund_max_age_days > 36500:
            raise ValueError("refund_max_age_days must not exceed 36500")
        if self.refund_min_age_seconds >= self.refund_max_age_days * 86400:
            raise ValueError("refund minimum age must be smaller than maximum age")
        if type(self.realtime_jitter) is not bool:
            raise ValueError("realtime_jitter must be boolean")
        if not isinstance(self.scenarios, dict):
            raise ValueError("scenarios must be a table of scenario weights")
        if set(self.scenarios) != set(SCENARIOS):
            raise ValueError(f"scenarios must contain exactly: {', '.join(SCENARIOS)}")
        for name, weight in self.scenarios.items():
            positive_int(f"scenarios.{name}", weight, allow_zero=True)
        if not sum(self.scenarios.values()):
            raise ValueError("at least one scenario weight must be positive")


@dataclass(frozen=True)
class Config:
    schema: str = "core_sim"
    seed: SeedConfig = field(default_factory=SeedConfig)
    generator: GeneratorConfig = field(default_factory=GeneratorConfig)

    def __post_init__(self) -> None:
        if not isinstance(self.schema, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", self.schema):
            raise ValueError("schema must be a lowercase SQL identifier of at most 63 characters")
        if self.schema in ("public", "information_schema") or self.schema.startswith("pg_"):
            raise ValueError("use a dedicated simulator schema")


def load_config(path: Path | None = None) -> Config:
    if path is None:
        path = Path("config.toml")
        if not path.exists():
            return Config()
    with path.open("rb") as stream:
        data = tomllib.load(stream)
    if set(data) - {"database", "seed", "generator"}:
        raise ValueError("unknown configuration section")
    if any(not isinstance(data.get(section, {}), dict) for section in ("database", "seed", "generator")):
        raise ValueError("database, seed and generator must be TOML tables")
    database = data.get("database", {})
    if set(database) - {"schema"}:
        raise ValueError("unknown database configuration field")
    try:
        return Config(
            schema=database.get("schema", "core_sim"),
            seed=SeedConfig(**data.get("seed", {})),
            generator=GeneratorConfig(**data.get("generator", {})),
        )
    except TypeError as exc:
        raise ValueError("invalid configuration fields or types") from exc
