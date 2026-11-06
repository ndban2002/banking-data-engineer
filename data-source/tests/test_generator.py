from dataclasses import replace
from datetime import UTC, datetime, timedelta
import json
import os
import signal
import subprocess
import sys
import time

import pytest

from bank_sim.cli import main
from bank_sim.config import GeneratorConfig
from bank_sim.generator import WorkloadGenerator
from bank_sim.verify import verify

pytestmark = pytest.mark.integration


def test_history_then_realtime_and_cli_verify(db, test_config, tmp_path, monkeypatch, capsys):
    config = replace(test_config, generator=GeneratorConfig(amount_min=1, amount_max=10))
    start = datetime(2026, 1, 1, tzinfo=UTC)
    history = WorkloadGenerator(db, config).run(mode="history", start=start, end=start + timedelta(days=1), count=100)
    assert history["attempts"] == 100
    with db.connect() as conn:
        times = [row["occurred_at"] for row in conn.execute(db.query(
            "SELECT occurred_at FROM {schema}.transactions ORDER BY created_at, id"
        ))]
    assert times == sorted(times)
    assert all(start <= instant < start + timedelta(days=1) for instant in times)
    realtime = WorkloadGenerator(db, config).run(mode="realtime", rate=20, duration=0.16)
    assert realtime["attempts"] > 0
    assert verify(db)["ok"]
    path = tmp_path / "config.toml"
    path.write_text(f'[database]\nschema = "{db.schema}"\n')
    monkeypatch.setenv("DATABASE_URL", db.url)
    assert main(["--config", str(path), "verify"]) == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True


@pytest.mark.parametrize("scenario", ["normal", "insufficient_funds", "decline", "refund", "retry"])
def test_each_scenario_is_executable_and_consistent(db, test_config, scenario):
    weights = {key: int(key == scenario) for key in GeneratorConfig().scenarios}
    config = replace(test_config, generator=GeneratorConfig(amount_min=1, amount_max=10, scenarios=weights))
    start = datetime(2026, 1, 1, tzinfo=UTC)
    stats = WorkloadGenerator(db, config).run(mode="history", start=start, end=start + timedelta(days=1), count=5)
    assert stats["attempts"] == 5
    if scenario in ("decline", "insufficient_funds"):
        assert stats["failed"] == 5
    elif scenario == "retry":
        assert stats["retries"] == 4
        assert sum(row["count"] for row in verify(db)["transactions"]) == 1
    elif scenario == "refund":
        report = verify(db)
        assert any(row["kind"] == "REFUND" and row["status"] == "SUCCEEDED" for row in report["transactions"])
    assert verify(db)["ok"]


@pytest.mark.parametrize("args", [
    {"mode": "history"},
    {"mode": "realtime", "rate": 0},
    {"mode": "realtime", "duration": float("nan")},
    {"mode": "realtime", "count": 1},
    {"mode": "history", "start": datetime(2026, 1, 2, tzinfo=UTC),
     "end": datetime(2026, 1, 1, tzinfo=UTC), "count": 1},
])
def test_invalid_run_parameters_do_not_touch_database(test_config, args):
    # DB placeholder proves validation happens before opening a connection.
    with pytest.raises(ValueError):
        WorkloadGenerator(None, test_config).run(**args)


def test_graceful_stop_finishes_current_transaction(db, test_config):
    generator = WorkloadGenerator(db, test_config)
    attempt = generator._attempt

    def stop_after_one(conn, occurred_at):
        attempt(conn, occurred_at)
        generator.stop.set()

    generator._attempt = stop_after_one
    stats = generator.run(mode="realtime", rate=1)
    assert stats["attempts"] == 1
    assert stats["stopped"] is True
    assert verify(db)["ok"]


def test_cli_workflow_and_sigterm_with_real_processes(db, tmp_path):
    path = tmp_path / "workflow.toml"
    path.write_text(f'''[database]
schema = "{db.schema}"
[seed]
customers = 3
merchants = 2
opening_balance = 1000
[generator]
amount_min = 1
amount_max = 10
''')
    base = [sys.executable, "-m", "bank_sim", "--config", str(path)]
    env = dict(os.environ, DATABASE_URL=db.url)

    def run(*args):
        result = subprocess.run(base + list(args), env=env, capture_output=True, text=True, timeout=10)
        assert result.returncode == 0, result.stderr
        return json.loads(result.stdout)

    assert run("init-db")["ok"]
    assert run("seed")["accounts"] == 0
    result = run("generate", "--mode", "history", "--start", "2026-01-01T00:00:00Z",
                 "--end", "2026-01-02T00:00:00Z", "--count", "20")
    assert result["attempts"] == 20
    assert run("generate", "--mode", "realtime", "--rate", "20", "--duration", "0.08")["attempts"] > 0
    assert run("verify")["ok"]

    with db.connect() as conn:
        before = conn.execute(db.query("SELECT count(*) AS n FROM {schema}.transactions")).fetchone()["n"]
    process = subprocess.Popen(base + ["generate", "--mode", "realtime", "--rate", "20"],
                               env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        deadline = time.monotonic() + 5
        with db.connect() as conn:
            while time.monotonic() < deadline:
                count = conn.execute(db.query("SELECT count(*) AS n FROM {schema}.transactions")).fetchone()["n"]
                if count > before:
                    break
                time.sleep(0.01)
            else:
                pytest.fail("realtime process did not produce a transaction")
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=5)
        assert process.returncode == 0, stderr
        assert json.loads(stdout)["stopped"] is True
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()
    assert run("verify")["ok"]
