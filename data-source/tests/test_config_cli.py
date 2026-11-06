from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

from bank_sim.cli import main, parser
from bank_sim.config import Config, GeneratorConfig, SeedConfig, load_config
from bank_sim.service import TransactionRequest


@pytest.mark.parametrize("schema", ["public", "pg_catalog", "a.b", "bad;drop", "A", "a" * 64])
def test_schema_must_be_dedicated_identifier(schema):
    with pytest.raises(ValueError):
        Config(schema=schema)


@pytest.mark.parametrize("kwargs", [
    {"rate": 0}, {"rate": float("nan")}, {"rate": float("inf")}, {"rate": True},
    {"amount_min": 500, "amount_max": 100}, {"amount_min": 0},
    {"scenarios": {"normal": 100}},
    {"scenarios": {name: 0 for name in GeneratorConfig().scenarios}},
])
def test_invalid_generator_configuration(kwargs):
    with pytest.raises(ValueError):
        GeneratorConfig(**kwargs)


def test_total_seed_funds_cannot_overflow():
    with pytest.raises(ValueError):
        SeedConfig(customers=2, opening_balance=2**62)


def test_load_example_and_reject_unknown_field(tmp_path):
    example = Path(__file__).resolve().parents[1] / "config.example.toml"
    assert load_config(example) == Config()
    path = tmp_path / "config.toml"
    path.write_text('[generator]\nraet = 2\n')
    with pytest.raises(ValueError):
        load_config(path)


def test_invalid_toml_section_is_rejected_without_traceback(tmp_path, capsys):
    path = tmp_path / "config.toml"
    path.write_text('database = ["schema"]\n')
    assert main(["--config", str(path), "verify"]) == 2
    assert "must be TOML tables" in capsys.readouterr().err


def test_cli_does_not_leak_database_credentials(monkeypatch, capsys):
    secret = "postgresql://secret_user:SECRET_PASSWORD@127.0.0.1/db"
    monkeypatch.setenv("DATABASE_URL", secret)

    def fail(*_):
        raise psycopg.OperationalError(f"connection failed: {secret}")

    monkeypatch.setattr("bank_sim.cli.Database.init_db", fail)
    assert main(["init-db"]) == 2
    output = capsys.readouterr()
    assert "CONNECTION_ERROR" in output.err
    assert "SECRET_PASSWORD" not in output.err + output.out


def test_cli_requires_database_url(monkeypatch, capsys):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    assert main(["verify"]) == 2
    assert "DATABASE_URL" in capsys.readouterr().err


def test_cli_rejects_timestamp_without_timezone():
    with pytest.raises(SystemExit):
        parser().parse_args(["generate", "--mode", "history", "--start", "2026-01-01"])


def test_request_validation():
    request = TransactionRequest(uuid4(), uuid4(), uuid4(), 100, "QR", datetime.now(UTC))
    for changes in ({"amount": 0}, {"amount": True}, {"occurred_at": datetime(2026, 1, 1)},
                    {"source_account_id": request.destination_account_id}, {"kind": "REFUND"}):
        with pytest.raises(ValueError):
            replace(request, **changes)
