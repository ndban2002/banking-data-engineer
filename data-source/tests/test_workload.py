from collections import Counter
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import random
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest

from bank_sim.config import GeneratorConfig
from bank_sim.workload import RetailSampler, history_times


def schedule(start, end, count, profile='retail', timezone='Asia/Ho_Chi_Minh'):
    return list(history_times(start, end, count, profile=profile,
                              timezone=timezone, rng=random.Random(42)))


def test_history_has_local_evening_peaks_and_reproducible_nonuniform_gaps():
    start = datetime.fromisoformat('2026-11-01T00:00:00+07:00')
    end = start + timedelta(days=7)
    times = schedule(start, end, 10000)
    assert times == schedule(start, end, 10000)
    assert len(times) == len(set(times)) == 10000
    assert times == sorted(times)
    assert all(start <= instant < end for instant in times)
    hours = Counter(t.astimezone(ZoneInfo('Asia/Ho_Chi_Minh')).hour for t in times)
    assert sum(hours[h] for h in range(17, 21)) > sum(hours[h] for h in range(1, 5)) * 10
    assert len({b - a for a, b in zip(times, times[1:])}) > 100


@pytest.mark.parametrize('timezone,start', [
    ('Asia/Ho_Chi_Minh', '2026-11-01T00:59:59+07:00'),
    ('America/New_York', '2026-11-01T00:00:00-04:00'),
    ('America/New_York', '2026-03-08T00:00:00-05:00'),
    ('Asia/Kolkata', '2026-11-01T00:00:00+05:30'),
])
def test_schedules_across_partial_hours_and_timezone_transitions(timezone, start):
    start = datetime.fromisoformat(start).astimezone(UTC)
    end = start + timedelta(hours=30)
    times = schedule(start, end, 1000, timezone=timezone)
    assert len(times) == len(set(times)) == 1000
    assert times == sorted(times)
    assert all(start <= t < end for t in times)


def test_dense_microsecond_window_and_uniform_compatibility():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    for profile in ('retail', 'uniform'):
        assert schedule(start, start + timedelta(microseconds=10), 10, profile) == [
            start + timedelta(microseconds=i) for i in range(10)
        ]
    assert schedule(start, start + timedelta(hours=1), 4, 'uniform') == [
        start + timedelta(minutes=15 * i) for i in range(4)
    ]


def test_uniform_long_interval_avoids_intermediate_timedelta_overflow():
    start = datetime(1, 1, 1, tzinfo=UTC)
    end = datetime(9999, 1, 1, tzinfo=UTC)
    times = schedule(start, end, 1024, 'uniform')
    assert len(times) == len(set(times)) == 1024
    assert times == sorted(times)
    assert all(start <= t < end for t in times)


def population():
    customers = [{'id': UUID(int=i + 1), 'region': 'HA_NOI'} for i in range(30)]
    merchants = [{'id': UUID(int=100 + i), 'region': 'HA_NOI' if i < 20 else 'DA_NANG',
                  'mcc': '5411', 'channel': 'QR'} for i in range(40)]
    return customers, merchants


def test_local_affinity_activity_skew_and_popularity():
    customers, merchants = population()
    sampler = RetailSampler(customers, merchants, 42, GeneratorConfig())
    rng = random.Random(42)
    instant = datetime(2026, 1, 1, 5, tzinfo=UTC)
    pairs = [sampler.choose(rng, instant) for _ in range(5000)]
    assert sum(c['region'] == m['region'] for c, m in pairs) / len(pairs) > .85
    active = Counter(c['id'] for c, _ in pairs)
    popular = Counter(m['id'] for _, m in pairs if m['region'] == 'HA_NOI')
    assert max(active.values()) > min(active.values()) * 3
    assert max(popular.values()) > min(popular.values()) * 3


def test_customer_has_stable_favorite_merchants_and_missing_region_fallback():
    customers, merchants = population()
    config = replace(GeneratorConfig(), local_merchant_probability=1, repeat_merchant_probability=1)
    for region in ('HA_NOI', 'NO_STORES_HERE'):
        customer = dict(customers[0], region=region)
        sampler = RetailSampler([customer], merchants, 42, config)
        again = RetailSampler([customer], merchants, 42, config)
        rng, other_rng = random.Random(42), random.Random(42)
        instant = datetime(2026, 1, 1, tzinfo=UTC)
        draws = [sampler.choose(rng, instant)[1] for _ in range(100)]
        assert len({m['id'] for m in draws}) <= 3
        assert draws == [again.choose(other_rng, instant)[1] for _ in range(100)]
        if region == 'HA_NOI':
            assert all(m['region'] == region for m in draws)
