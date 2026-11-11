"""Synthetic retail assumptions for a learning workload, not market statistics.

Names are assembled from fictional combinations, with no external personal data.
Category weights, ticket sizes and hourly curves are illustrative tuning defaults;
they must not be interpreted as measured Vietnamese consumer behavior.
"""

from dataclasses import dataclass
import math
import random
from statistics import NormalDist


REGIONS = ("HA_NOI", "DA_NANG", "HO_CHI_MINH", "CAN_THO")
REGION_WEIGHTS = (36, 14, 42, 8)
REGION_LABELS = ("Hà Nội", "Đà Nẵng", "Hồ Chí Minh", "Cần Thơ")


@dataclass(frozen=True)
class RetailCategory:
    label: str
    weight: int
    median_amount: int
    log_sigma: float
    qr_probability: float


CATEGORIES = {
    "5411": RetailCategory("Tạp hóa", 34, 180_000, 0.70, 0.75),
    "5812": RetailCategory("Quán ăn", 38, 75_000, 0.65, 0.90),
    "5311": RetailCategory("Bách hóa", 14, 350_000, 0.75, 0.40),
    "5912": RetailCategory("Nhà thuốc", 10, 130_000, 0.70, 0.65),
    "5732": RetailCategory("Điện tử", 4, 1_200_000, 0.90, 0.25),
}

# Midnight through 23:00 local time. Deliberately nonzero overnight so short
# overnight simulations still have observations.
HOURLY_ACTIVITY = (
    0.12, 0.07, 0.04, 0.03, 0.04, 0.15, 0.55, 1.30,
    1.55, 1.20, 1.25, 1.85, 2.10, 1.50, 1.15, 1.20,
    1.50, 2.00, 2.40, 2.30, 1.75, 1.10, 0.55, 0.25,
)


def entity_rng(seed: int, kind: str, index: int) -> random.Random:
    # Separate streams keep an entity stable when another population grows.
    return random.Random(f"{seed}:{kind}:{index}")


def customer_profile(seed: int, index: int) -> dict[str, str]:
    rng = entity_rng(seed, "customer", index)
    surnames = ("Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh", "Phan",
                "Vũ", "Võ", "Đặng", "Bùi", "Đỗ", "Hồ", "Ngô", "Dương")
    names = (
        ("Minh", "Anh"), ("Quốc", "Bảo"), ("Thanh", "Bình"),
        ("Gia", "Huy"), ("Đức", "Khang"), ("Hoàng", "Nam"),
        ("Trung", "Kiên"), ("Tuấn", "Phong"), ("Hữu", "Phúc"),
        ("Nhật", "Quang"), ("Ngọc", "Anh"), ("Thảo", "Chi"),
        ("Khánh", "Linh"), ("Thu", "Hà"), ("Bảo", "Ngọc"),
        ("Phương", "Nhi"), ("Hải", "Yến"), ("Mai", "Hương"),
        ("Quỳnh", "Trang"), ("Tú", "Uyên"),
    )
    surname = rng.choice(surnames)
    middle, given = rng.choice(names)
    return {
        "name": f"{surname} {middle} {given}",
        "region": rng.choices(REGIONS, weights=REGION_WEIGHTS)[0],
    }


def merchant_profile(seed: int, index: int) -> dict[str, str]:
    rng = entity_rng(seed, "merchant", index)
    mcc = rng.choices(tuple(CATEGORIES), weights=[c.weight for c in CATEGORIES.values()])[0]
    category = CATEGORIES[mcc]
    region = rng.choices(REGIONS, weights=REGION_WEIGHTS)[0]
    name = rng.choice(("Mây Xanh", "Nắng Mai", "Bình An", "Hoa Sen", "Sông Nhỏ",
                       "Vườn Xanh", "Góc Phố", "Ánh Dương"))
    return {
        "name": f"SIM {category.label} {name} {index + 1:04}",
        "mcc": mcc,
        "region": region,
        "channel": "QR" if rng.random() < category.qr_probability else "POS",
    }


def opening_balance_radius(base: int, spread: float) -> int:
    # Integer arithmetic preserves BIGINT bounds even for very large balances.
    numerator, denominator = float(spread).as_integer_ratio()
    return base * numerator // denominator


def opening_balance(seed: int, index: int, base: int, spread: float) -> int:
    radius = opening_balance_radius(base, spread)
    return entity_rng(seed, "opening-balance", index).randint(base - radius, base + radius)


def retail_hour_weight(hour: int) -> float:
    return HOURLY_ACTIVITY[hour]


def merchant_hour_weight(mcc: str, hour: int) -> float:
    """Relative category affinity at a local hour, used in merchant selection."""
    if mcc == "5812":
        if hour in (7, 8, 11, 12, 18, 19):
            return 2.4
        return 0.45 if hour < 6 or hour >= 22 else 0.8
    if mcc == "5411":
        return 1.6 if hour in (7, 8, 16, 17, 18) else 0.9
    if mcc in ("5311", "5732"):
        return 1.4 if 10 <= hour <= 21 else 0.15
    return 1.0 if 7 <= hour <= 21 else 0.25


def payment_amount(rng: random.Random, mcc: str, amount_min: int,
                   amount_max: int, rounding: int) -> int:
    """Sample a bounded lognormal ticket, rounded to an available VND increment.

    Truncation before sampling avoids a pile of clipped values at amount_max.
    Tiny/custom ranges outside the model's numerical support use uniform draws.
    """
    if amount_min == amount_max:
        return amount_min
    category = CATEGORIES.get(mcc, CATEGORIES["5411"])
    distribution = NormalDist(math.log(category.median_amount), category.log_sigma)
    lower = distribution.cdf(math.log(amount_min))
    upper = distribution.cdf(math.log(amount_max))
    if upper - lower < 1e-12:
        amount = rng.randint(amount_min, amount_max)
    else:
        quantile = min(math.nextafter(1.0, 0.0), max(math.nextafter(0.0, 1.0),
                                                   rng.uniform(lower, upper)))
        amount = round(math.exp(distribution.inv_cdf(quantile)))
    first_tick = (amount_min + rounding - 1) // rounding
    last_tick = amount_max // rounding
    if first_tick <= last_tick:
        tick = (amount + rounding // 2) // rounding
        return min(last_tick, max(first_tick, tick)) * rounding
    return min(amount_max, max(amount_min, amount))
