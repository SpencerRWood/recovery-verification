"""Manual operational interface reports execution separately from assurance."""

import json
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
from dagster import DagsterInstance

from recovery_verification import manual
from recovery_verification.orchestration import OrchestrationConfig


@pytest.mark.parametrize("successful", [True, False])
def test_manual_operational_requests(
    document: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    successful: bool,
) -> None:
    config = OrchestrationConfig.model_validate_json(
        json.dumps(
            {
                "manifest_json": json.dumps(document),
                "checkouts": {"example-linux": str(tmp_path)},
                "state_directory": str(tmp_path / "state"),
                "environment": "test",
            }
        )
    )
    path = tmp_path / "orchestration.json"
    path.write_text(config.model_dump_json())
    monkeypatch.setenv("RECOVERY_ORCHESTRATION_CONFIG", str(path))
    monkeypatch.setattr(DagsterInstance, "get", DagsterInstance.ephemeral)
    job = Mock()
    job.execute_in_process.return_value = Mock(
        success=successful, run_id="simulated-run"
    )
    definition = Mock()
    definition.resolve_job_def.return_value = job
    monkeypatch.setattr(manual, "defs", definition)
    assert manual.main(
        ["readiness", "--target", "example-linux", "--execution-key", "test"]
    ) == (0 if successful else 1)
    response = json.loads(capsys.readouterr().out)
    assert response["assurance"] == "inspect_retained_evidence"
    assert response["run_id"] == "simulated-run"
    assert isinstance(
        job.execute_in_process.call_args.kwargs["instance"], DagsterInstance
    )
    assert (
        job.execute_in_process.call_args.kwargs["tags"]["recovery/execution_key"]
        == "test"
    )
    assert manual.main(["readiness"]) == (0 if successful else 1)


def test_manual_missing_runtime_configuration(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("RECOVERY_ORCHESTRATION_CONFIG", raising=False)
    assert manual.main(["verification"]) == 2
    assert json.loads(capsys.readouterr().out)["state"] == "unavailable"
