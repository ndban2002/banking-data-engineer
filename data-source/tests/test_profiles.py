from dataclasses import replace
import random
from statistics import median

import pytest

from bank_sim.config import GeneratorConfig, MAX_BIGINT, SeedConfig
from bank_sim.profiles import (
    CATEGORIES,
    REGIONS,
    customer_profile,
    merchant_hour_weight,
    merchant_profile,
    opening_balance,
    payment_amount,
    retail_hour_weight,
)


def test_entity_profiles_are_deterministic_and_independent_of_population_growth():
    customers = [customer_profile(42, i) for i in range(100)]
    merchants = [merchant_profile(42, i) for i in range(20)]
    extended_customers = [customer_profile(42, i) for i in range(1000)]
    extended_merchants = [merchant_profile(42, i) for i in range(100)]
    assert customers == extended_customers[:100]
    assert merchants == extended_merchants[:20]
    assert customers != [customer_profile(43, i) for i in range(100)]
    assert len({row["name"] for row in customers}) > 60
    assert {row["region"] for row in customers} == set(REGIONS)
    assert all(row["mcc"] in CATEGORIES and row["name"].startswith("SIM ") for row in merchants)


def test_channel_mix_is_related_to_merchant_category():
    merchants = [merchant_profile(42, i) for i in range(5000)]
    restaurant = [row for row in merchants if row["mcc"] == "5812"]
    electronics = [row for row in merchants if row["mcc"] == "5732"]
    qr_rate = lambda rows: sum(row["channel"] == "QR" for row in rows) / len(rows)
    assert qr_rate(restaurant) > 0.8
    assert qr_rate(electronics) < 0.4


@pytest.mark.parametrize("amount_min,amount_max,rounding", [
    (10_000, 500_000, 1000), (1, 10, 1000), (1, 1, 1000),
    (1500, 2500, 1000), (1501, 1999, 1000),
    (MAX_BIGINT - 100, MAX_BIGINT, 1000),
])
def test_ticket_amounts_are_bounded_whole_vnd(amount_min, amount_max, rounding):
    rng = random.Random(42)
    for mcc in (*CATEGORIES, "9999"):
        values = [payment_amount(rng, mcc, amount_min, amount_max, rounding) for _ in range(100)]
        assert all(type(value) is int and amount_min <= value <= amount_max for value in values)
        if (amount_min + rounding - 1) // rounding <= amount_max // rounding:
            assert all(value % rounding == 0 for value in values)


def test_ticket_distributions_differ_by_category_without_clipping_pileups():
    def tickets(mcc):
        rng = random.Random(42)
        return [payment_amount(rng, mcc, 10_000, 500_000, 1000) for _ in range(3000)]

    food, electronics = tickets("5812"), tickets("5732")
    assert median(food) < median(electronics) / 2
    assert food.count(500_000) < len(food) * 0.02
    assert electronics.count(500_000) < len(electronics) * 0.02
    assert max(food) > median(food) * 3
    assert tickets("5812") == food


def test_hour_profiles_have_meal_peaks_and_keep_overnight_support():
    assert all(retail_hour_weight(hour) > 0 for hour in range(24))
    assert retail_hour_weight(18) > retail_hour_weight(3) * 10
    assert merchant_hour_weight("5812", 12) > merchant_hour_weight("5812", 15)
    assert merchant_hour_weight("5732", 14) > merchant_hour_weight("5732", 3)


def test_opening_funds_vary_only_when_enabled():
    assert {opening_balance(42, i, 100_000_000, 0) for i in range(100)} == {100_000_000}
    balances = [opening_balance(42, i, 100_000_000, 0.5) for i in range(100)]
    assert all(50_000_000 <= value <= 150_000_000 for value in balances)
    assert len(set(balances)) > 90
    assert balances == [opening_balance(42, i, 100_000_000, 0.5) for i in range(100)]
    assert opening_balance(42, 0, 0, 1) == 0


@pytest.mark.parametrize("kwargs", [
    {"opening_balance_spread": -0.1}, {"opening_balance_spread": 1.1},
    {"opening_balance_spread": True}, {"opening_balance_spread": float("nan")},
    {"customers": 1, "opening_balance": MAX_BIGINT // 2 + 1, "opening_balance_spread": 1},
])
def test_invalid_seed_spread_is_rejected(kwargs):
    with pytest.raises(ValueError):
        SeedConfig(**kwargs)


@pytest.mark.parametrize("kwargs", [
    {"profile": "unknown"}, {"timezone": "invalid/timezone"}, {"timezone": None},
    {"amount_rounding": 0}, {"amount_rounding": True},
    {"local_merchant_probability": float("nan")}, {"local_merchant_probability": 1.1},
    {"repeat_merchant_probability": -0.1}, {"repeat_merchant_probability": True},
    {"refund_min_age_seconds": -1}, {"refund_max_age_days": 0},
    {"refund_max_age_days": 36501},
    {"refund_min_age_seconds": 86400, "refund_max_age_days": 1},
    {"realtime_jitter": 1}, {"scenarios": []},
])
def test_invalid_retail_configuration_is_rejected(kwargs):
    with pytest.raises(ValueError):
        GeneratorConfig(**kwargs)


@pytest.mark.integration
def test_reseed_with_spread_preserves_existing_accounts_and_profiles(db, test_config):
    with db.connect() as conn:
        before_accounts = conn.execute(db.query("SELECT * FROM {schema}.accounts ORDER BY id")).fetchall()
        before_customers = conn.execute(db.query("SELECT * FROM {schema}.customers ORDER BY id")).fetchall()
    config = replace(test_config, seed=replace(test_config.seed, opening_balance_spread=0.5))
    assert db.seed(config) == {"customers": 0, "merchants": 0, "accounts": 0}
    with db.connect() as conn:
        assert conn.execute(db.query("SELECT * FROM {schema}.accounts ORDER BY id")).fetchall() == before_accounts
        assert conn.execute(db.query("SELECT * FROM {schema}.customers ORDER BY id")).fetchall() == before_customers
