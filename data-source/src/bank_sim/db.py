from contextlib import contextmanager
from importlib.resources import files
import random
from uuid import UUID, uuid5

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from .config import Config

IDENTITY_NAMESPACE = UUID("51870a77-d115-4494-b230-78790e979e83")


class Database:
    def __init__(self, url: str, schema: str = "core_sim") -> None:
        Config(schema=schema)  # also validate callers outside the CLI
        self.url = url
        self.schema = schema

    def query(self, statement: str) -> sql.Composed:
        return sql.SQL(statement).format(schema=sql.Identifier(self.schema))

    @contextmanager
    def connect(self):
        with psycopg.connect(self.url, autocommit=True, row_factory=dict_row, connect_timeout=5) as conn:
            conn.execute("SET TIME ZONE 'UTC'")
            yield conn

    def init_db(self) -> None:
        ddl = files("bank_sim").joinpath("schema.sql").read_text()
        with self.connect() as conn, conn.transaction():
            conn.execute(self.query(ddl))

    def seed(self, config: Config) -> dict[str, int]:
        rng = random.Random(config.seed.random_seed)
        regions = ("HA_NOI", "DA_NANG", "HO_CHI_MINH", "CAN_THO")
        mccs = ("5411", "5812", "5311", "5912", "5732")
        inserted = {"customers": 0, "merchants": 0, "accounts": 0}

        def identity(name: str) -> UUID:
            return uuid5(IDENTITY_NAMESPACE, f"{config.seed.random_seed}:{name}")

        with self.connect() as conn, conn.transaction():
            for index in range(config.seed.customers):
                owner_id = identity(f"customer:{index}")
                inserted["customers"] += conn.execute(self.query("""
                    INSERT INTO {schema}.customers (id, name, region) VALUES (%s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """), (owner_id, f"Khach hang gia lap {index + 1:04}", rng.choice(regions))).rowcount
                inserted["accounts"] += conn.execute(self.query("""
                    INSERT INTO {schema}.accounts (id, customer_id, opening_balance, balance)
                    VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING
                """), (identity(f"customer-account:{index}"), owner_id,
                        config.seed.opening_balance, config.seed.opening_balance)).rowcount
            for index in range(config.seed.merchants):
                owner_id = identity(f"merchant:{index}")
                inserted["merchants"] += conn.execute(self.query("""
                    INSERT INTO {schema}.merchants (id, name, mcc, region, channel)
                    VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING
                """), (owner_id, f"Merchant gia lap {index + 1:04}", rng.choice(mccs),
                        rng.choice(regions), ("QR", "POS")[index % 2])).rowcount
                inserted["accounts"] += conn.execute(self.query("""
                    INSERT INTO {schema}.accounts (id, merchant_id, opening_balance, balance)
                    VALUES (%s, %s, 0, 0) ON CONFLICT DO NOTHING
                """), (identity(f"merchant-account:{index}"), owner_id)).rowcount
        return inserted
