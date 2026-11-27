"""Emit Spark CLI result objects with an optional machine-readable marker."""

import json


REPORT_PREFIX = "BANKING_SPARK_REPORT="


def add_report_argument(parser):
    parser.add_argument("--orchestration-report", action="store_true",
                        help="Prefix the JSON result so an orchestrator can isolate it from Spark logs")


def emit_report(report, *, orchestration=False):
    payload = json.dumps(report, default=str)
    print(REPORT_PREFIX + payload if orchestration else payload, flush=True)
