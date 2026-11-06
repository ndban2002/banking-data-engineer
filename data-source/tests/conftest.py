import os
from uuid import uuid4

import pytest
from psycopg import sql

from bank_sim.config import Config, SeedConfig
from bank_sim.db import Database


@pytest.fixture
def test_config():
    return Config(seed=SeedConfig(customers=3, merchants=2, opening_balance=1_000))


@pytest.fixture
def db(test_config):
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("set TEST_DATABASE_URL to run PostgreSQL integration tests")
    schema = f"bank_sim_test_{uuid4().hex}"
    database = Database(url, schema)
    try:
        database.init_db()
        database.seed(test_config)
        yield database
    finally:
        # Only this fixture's newly generated schema can be removed.
        assert schema.startswith("bank_sim_test_")
        with database.connect() as conn:
            conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))


@pytest.fixture
def actors(db):
    with db.connect() as conn:
        customer = conn.execute(db.query(
            "SELECT id FROM {schema}.accounts WHERE customer_id IS NOT NULL ORDER BY id LIMIT 1"
        )).fetchone()
        merchant = conn.execute(db.query("""
            SELECT a.id, m.channel FROM {schema}.accounts a
            JOIN {schema}.merchants m ON m.id = a.merchant_id ORDER BY a.id LIMIT 1
        """)).fetchone()
    return customer["id"], merchant["id"], merchant["channel"]
