from dataclasses import dataclass, field
from pathlib import Path
import math
import re
import tomllib

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


@dataclass(frozen=True)
class SeedConfig:
    random_seed: int = 42
    customers: int = 100
    merchants: int = 20
    opening_balance: int = 100_000_000

    def __post_init__(self) -> None:
        positive_int("random_seed", self.random_seed, allow_zero=True)
        positive_int("customers", self.customers)
        positive_int("merchants", self.merchants)
        positive_int("opening_balance", self.opening_balance, allow_zero=True)
        if self.customers * self.opening_balance >= MAX_BIGINT:
            raise ValueError("total opening funds must be below PostgreSQL BIGINT maximum")


@dataclass(frozen=True)
class GeneratorConfig:
    rate: float = 1.0
    amount_min: int = 10_000
    amount_max: int = 500_000
    scenarios: dict[str, int] = field(default_factory=lambda: dict(zip(SCENARIOS, (90, 3, 3, 2, 2))))

    def __post_init__(self) -> None:
        positive_number("rate", self.rate)
        positive_int("amount_min", self.amount_min)
        positive_int("amount_max", self.amount_max)
        if self.amount_min > self.amount_max:
            raise ValueError("amount_min must not exceed amount_max")
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
