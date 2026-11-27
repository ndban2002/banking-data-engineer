from subprocess import CompletedProcess, DEVNULL, PIPE

import dagster as dg
import pytest

from banking_orchestrator import spark


def stub_process(monkeypatch, stdout, status):
    def run(command, **kwargs):
        assert command[:6] == ["docker", "compose", "exec", "-T", "spark-master",
                               "/opt/spark/bin/spark-submit"]
        assert command[-1] == "--orchestration-report"
        assert kwargs["stdout"] == PIPE
        assert kwargs["stderr"] == DEVNULL
        return CompletedProcess(command, status, stdout)

    monkeypatch.setattr(spark.subprocess, "run", run)


def test_report_parser_ignores_unmarked_output(monkeypatch, caplog):
    stub_process(monkeypatch, 'fixture-secret\n{"ok": false}\nBANKING_SPARK_REPORT={"ok": true}\n', 0)

    with dg.build_op_context() as context:
        result = spark.SparkJobs().submit(context, "silver_quality.py")

    assert result == {"ok": True}
    assert "fixture-secret" not in caplog.text


@pytest.mark.parametrize(("stdout", "status"), [
    ('unmarked {"ok": true}\n', 0),
    ('BANKING_SPARK_REPORT={"ok": true}\nBANKING_SPARK_REPORT={"ok": true}\n', 0),
    ('BANKING_SPARK_REPORT=fixture-secret\n', 0),
    ('BANKING_SPARK_REPORT={"ok": true}\n', 1),
    ('BANKING_SPARK_REPORT={"ok": false}\n', 2),
])
def test_report_parser_rejects_invalid_output_and_unexpected_status(monkeypatch, stdout, status):
    stub_process(monkeypatch, stdout, status)

    with dg.build_op_context() as context, pytest.raises(dg.Failure) as failure:
        spark.SparkJobs().submit(context, "silver_quality.py", allow_quality_failure=True)

    assert failure.value.metadata["exit_code"].value == status
    assert "fixture-secret" not in str(failure.value)


def test_spark_quality_failure_requires_opt_in(monkeypatch):
    stub_process(monkeypatch, 'BANKING_SPARK_REPORT={"ok": false}\n', 1)

    with dg.build_op_context() as context:
        resource = spark.SparkJobs()
        assert resource.submit(context, "silver_quality.py", allow_quality_failure=True) == {"ok": False}
        with pytest.raises(dg.Failure):
            resource.submit(context, "silver_quality.py")
