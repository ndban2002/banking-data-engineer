import json
import re
import subprocess
from pathlib import Path
from typing import Literal

import dagster as dg
from pydantic import Field, field_validator

INFRA_DIRECTORY = Path(__file__).resolve().parents[2] / "infra"
REPORT_PREFIX = "BANKING_SPARK_REPORT="


def snapshot_id(value):
    # Dagster UI uses JavaScript numbers, which cannot represent all Iceberg IDs.
    return None if value is None else str(value)


def snapshot_map(snapshots: dict) -> dict:
    return {table: snapshot_id(value) for table, value in snapshots.items()}


def silver_batches(batches: list[dict]) -> list[dict]:
    return [
        {
            "bronze_table": row["bronze_table"],
            "batch_id": row["batch_id"],
            "bronze_snapshot_id": snapshot_id(row["bronze_snapshot_id"]),
            "silver_table": row["silver_table"],
            "input_rows": row["input_rows"],
            "upsert_rows": row["upsert_rows"],
            "quarantined_rows": row["quarantined_rows"],
            "skipped": row["skipped"],
        }
        for row in batches
    ]


class SparkJobs(dg.ConfigurableResource):
    namespace_suffix: str = "m4"
    load_mode: Literal["full", "incremental"] = "full"
    overlap_minutes: int = Field(default=5, ge=0, le=525600)
    compose_directory: str = str(INFRA_DIRECTORY)

    @field_validator("namespace_suffix")
    @classmethod
    def validate_suffix(cls, suffix: str) -> str:
        if not re.fullmatch(r"[a-z0-9_]{1,56}", suffix):
            raise ValueError("Namespace suffix must be 1–56 lowercase letters, digits, or underscores.")
        return suffix

    def namespace(self, layer: str) -> str:
        return f"{layer}_{self.namespace_suffix}"

    def table(self, layer: str, table: str) -> str:
        return f"lakehouse.{self.namespace(layer)}.{table}"

    def submit(self, context, script: str, *args: str, allow_quality_failure: bool = False) -> dict:
        command = [
            "docker", "compose", "exec", "-T", "spark-master",
            "/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077",
            f"/opt/spark/work-dir/jobs/{script}", *args, "--orchestration-report",
        ]
        context.log.info(f"Running {script} in dataset {self.namespace_suffix}.")
        try:
            completed = subprocess.run(
                command, cwd=self.compose_directory, stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL, encoding="utf-8", errors="replace", check=False,
            )
        except OSError:
            raise dg.Failure(
                f"Could not start Docker Compose for {script}. Check Docker and the Compose directory."
            ) from None

        lines = [line.removeprefix(REPORT_PREFIX) for line in completed.stdout.splitlines()
                 if line.startswith(REPORT_PREFIX)]
        try:
            report = json.loads(lines[0]) if len(lines) == 1 else None
        except ValueError:
            report = None

        status = completed.returncode
        if isinstance(report, dict):
            if status == 0 or (allow_quality_failure and status == 1 and report.get("ok") is False):
                return report
            if script == "silver_loader.py" and status == 1:
                rejected = [batch for batch in silver_batches(report["batches"])
                            if batch["quarantined_rows"] and not batch["skipped"]]
                if rejected:
                    raise dg.Failure("Silver recorded rejected rows in quarantine.", metadata={
                        "quarantined_rows": sum(batch["quarantined_rows"] for batch in rejected),
                        "rejected_batches": dg.MetadataValue.json(rejected),
                    })

        cause = "did not return a valid report" if status == 0 else f"exited with status {status}"
        raise dg.Failure(
            f"{script} {cause}. Check the Spark application and Docker Compose service status.",
            metadata={"exit_code": status, "namespace_suffix": self.namespace_suffix},
        )
