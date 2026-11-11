"""Synthetic retail sampling; weights are learning assumptions, not market data."""

from bisect import bisect_right
from datetime import UTC, datetime, timedelta
from itertools import accumulate
import random
from zoneinfo import ZoneInfo

from .profiles import merchant_hour_weight, retail_hour_weight


class WeightedSampler:
    def __init__(self, rows, weights):
        self.rows = list(rows)
        self.cumulative = list(accumulate(weights))

    def choose(self, rng):
        index = bisect_right(self.cumulative, rng.random() * self.cumulative[-1])
        return self.rows[min(index, len(self.rows) - 1)]


class RetailSampler:
    """Cache selection weights so each request avoids scanning every account."""

    def __init__(self, customers, merchants, seed, config):
        self.config = config
        self.seed = seed
        self.timezone = ZoneInfo(config.timezone)
        self.customers = WeightedSampler(customers, [self._weight(c['id']) for c in customers])
        self.merchants = merchants
        self.local = {}
        for merchant in merchants:
            self.local.setdefault(merchant['region'], []).append(merchant)
        self.popularity = {m['id']: self._weight(m['id']) for m in merchants}
        self.samplers = {}
        self.favorite_samplers = {}
        self.favorites = {}

    def _weight(self, identity):
        rng = random.Random(f'{self.seed}:activity:{identity}')
        return min(10.0, max(0.1, rng.lognormvariate(0, 0.9)))

    def choose(self, rng, occurred_at):
        customer = self.customers.choose(rng)
        region = customer['region'] if rng.random() < self.config.local_merchant_probability else None
        pool = self.local.get(region, self.merchants)
        hour = occurred_at.astimezone(self.timezone).hour
        key = (region, hour)
        if key not in self.samplers:
            self.samplers[key] = WeightedSampler(pool, [
                self.popularity[m['id']] * merchant_hour_weight(m['mcc'], hour) for m in pool
            ])
        if rng.random() < self.config.repeat_merchant_probability:
            favorite_key = (customer['id'], region)
            if favorite_key not in self.favorites:
                favorite_rng = random.Random(f'{self.seed}:favorites:{customer["id"]}:{region}')
                if region not in self.favorite_samplers:
                    self.favorite_samplers[region] = WeightedSampler(
                        pool, [self.popularity[m['id']] for m in pool])
                population = self.favorite_samplers[region]
                # Sampling with replacement deliberately lets a regular favor one shop strongly.
                self.favorites[favorite_key] = [population.choose(favorite_rng) for _ in range(3)]
            favorites = self.favorites[favorite_key]
            merchant = WeightedSampler(favorites, [merchant_hour_weight(m['mcc'], hour)
                                                   for m in favorites]).choose(rng)
        else:
            merchant = self.samplers[key].choose(rng)
        return customer, merchant


def _segments(start, end, timezone):
    cursor = start
    while cursor < end:
        local = cursor.astimezone(timezone)
        # Advance on the UTC timeline; local arithmetic across DST can skip/repeat instants.
        step = timedelta(hours=1, minutes=-local.minute, seconds=-local.second,
                         microseconds=-local.microsecond)
        boundary = cursor + min(step, end - cursor)
        weight = retail_hour_weight(local.hour) * (1.15 if local.weekday() >= 5 else 1.0)
        yield cursor, boundary, weight
        cursor = boundary


def history_times(start: datetime, end: datetime, count: int, *, profile: str,
                  timezone: str, rng: random.Random):
    """Ordered stratified sampling, bounded memory even for large histories.

    Each request gets a jittered quantile of the hourly activity distribution.
    Enforce distinct microseconds in [start, end), including very short ranges.
    Callers validate count and the available microsecond range first.
    """
    start, end = start.astimezone(UTC), end.astimezone(UTC)
    if profile == 'uniform':
        duration_us = (end - start) // timedelta(microseconds=1)
        for index in range(count):
            yield start + timedelta(microseconds=duration_us * index // count)
        return
    zone = ZoneInfo(timezone)
    total_mass = sum((right - left).total_seconds() * weight
                     for left, right, weight in _segments(start, end, zone))
    segments = iter(_segments(start, end, zone))
    left, right, weight = next(segments)
    mass_before = 0.0
    mass_end = (right - left).total_seconds() * weight
    previous = start
    for index in range(count):
        target = (index + rng.random()) * total_mass / count
        while target >= mass_end and right < end:
            mass_before = mass_end
            left, right, weight = next(segments)
            mass_end += (right - left).total_seconds() * weight
        instant = left + timedelta(seconds=(target - mass_before) / weight)
        earliest = start if index == 0 else previous + timedelta(microseconds=1)
        latest = end - timedelta(microseconds=count - index)
        previous = min(latest, max(earliest, instant))
        yield previous
