import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import sys

import psycopg

from .config import load_config
from .db import Database
from .generator import WorkloadGenerator
from .verify import verify


def timestamp(value: str) -> datetime:
    try:
        result = datetime.fromisoformat(value)
        if result.utcoffset() is None:
            raise ValueError
        return result
    except ValueError as exc:
        raise argparse.ArgumentTypeError("use an ISO-8601 timestamp with timezone, e.g. 2026-11-01T00:00:00+07:00") from exc


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="bank-sim", description="Synthetic PostgreSQL banking data source")
    root.add_argument("--config", type=Path, help="TOML file (default: ./config.toml if present)")
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("init-db", help="Create dedicated schema and tables without deleting data")
    commands.add_parser("seed", help="Create synthetic owners and opening balances idempotently")
    commands.add_parser("verify", help="Reconcile all balances and report transaction statistics")
    generate = commands.add_parser("generate", help="Recover pending transactions and generate workload")
    generate.add_argument("--mode", choices=("history", "realtime"), required=True)
    generate.add_argument("--start", type=timestamp)
    generate.add_argument("--end", type=timestamp)
    generate.add_argument("--count", type=int, help="Number of attempts (including retries), history only")
    generate.add_argument("--rate", type=float, help="Target requests/second, realtime only")
    generate.add_argument("--duration", type=float, help="Realtime duration in seconds; omit to run until stopped")
    return root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        config = load_config(args.config)
        url = os.environ.get("DATABASE_URL")
        if not url:
            raise ValueError("set DATABASE_URL to your existing PostgreSQL database")
        db = Database(url, config.schema)
        if args.command == "init-db":
            db.init_db()
            result = {"ok": True, "schema": config.schema}
        elif args.command == "seed":
            result = db.seed(config)
        elif args.command == "verify":
            result = verify(db)
        else:
            result = WorkloadGenerator(db, config).run(
                mode=args.mode, start=args.start, end=args.end, count=args.count,
                rate=args.rate, duration=args.duration,
            )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 1 if result.get("ok") is False else 0
    except psycopg.Error as exc:
        # libpq errors and server DETAIL can contain DSNs, credentials or row data.
        # Never print the exception, diagnostic message, or connection string.
        code = exc.sqlstate or "CONNECTION_ERROR"
        print(f"Database operation failed ({code}). Check connectivity, permissions and init-db; pending work can be resumed.", file=sys.stderr)
        return 2
    except (ValueError, OSError, TypeError) as exc:
        # Do not echo arbitrary file contents or operating-system messages.
        message = str(exc) if isinstance(exc, ValueError) else "could not read configuration or invalid configuration structure"
        print(f"Error: {message}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("Stopped; any unfinished PENDING transaction will be recovered by generate.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
