"""Real Dagster resources with simulated checks, separate from activation."""

import json
from pathlib import Path
from typing import Any

import pytest
from tests.unit.test_scorecard import local  # noqa: F401 -- Synthetic boundary fixture.

from recovery_verification.contract import Level, parse_manifest
from recovery_verification.dagster.definitions import defs
from recovery_verification.execution import RetryingProbe, read_state
from recovery_verification.history import read_history
from recovery_verification.orchestration import (
    JOBS,
    OrchestrationConfig,
    execution_request,
)
from recovery_verification.probes import result


@pytest.mark.usefixtures("local")
@pytest.mark.parametrize("tier", ["readiness", "verification", "drill"])
def test_operational_jobs_archive_and_resume(
    document: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    tier: Level,
) -> None:
    config = OrchestrationConfig.model_validate_json(
        json.dumps(
            {
                "manifest_json": json.dumps(document),
                "checkouts": {"example-linux": str(tmp_path)},
                "state_directory": str(tmp_path / "state"),
                "environment": "test",
                "isolated_test_mode": True,
            }
        )
    )
    path = tmp_path / "orchestration.json"
    path.write_text(config.model_dump_json())
    monkeypatch.setenv("RECOVERY_ORCHESTRATION_CONFIG", str(path))
    monkeypatch.setattr(
        RetryingProbe, "__call__", lambda *_: result("passed", "simulated_prerequisite")
    )
    request = execution_request(
        config, parse_manifest(config.manifest_json), tier, "restart-test"
    )
    job = defs.resolve_job_def(JOBS[tier])
    first = job.execute_in_process(run_config=request.run_config, tags=request.tags)
    assert first.success
    state = read_state(tmp_path / "state/execution.json")
    assert len(state.attempts) == 1
    assert next(iter(state.attempts.values())).state == "complete"
    history = read_history(tmp_path / "state/history.json")
    monkeypatch.setattr(
        RetryingProbe,
        "__call__",
        lambda *_: pytest.fail("cached execution must not repeat"),
    )
    repeated = job.execute_in_process(run_config=request.run_config, tags=request.tags)
    assert repeated.success
    assert read_history(tmp_path / "state/history.json") == history


@pytest.mark.usefixtures("local")
def test_operational_monthly_requires_explicit_isolation(
    document: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
    monkeypatch.setattr(
        RetryingProbe, "__call__", lambda *_: result("passed", "simulated_prerequisite")
    )
    request = execution_request(
        config, parse_manifest(config.manifest_json), "drill", "disallowed"
    )
    run = defs.resolve_job_def(JOBS["drill"]).execute_in_process(
        run_config=request.run_config, tags=request.tags, raise_on_error=False
    )
    assert not run.success
    state = read_state(tmp_path / "state/execution.json")
    assert next(iter(state.attempts.values())).state == "interrupted"
    history = read_history(tmp_path / "state/history.json")
    assert len(history.entries) == 2
    interrupted = next(e for e in history.entries if e.tier == "drill")
    assert all(
        c.state == "unavailable"
        for c in interrupted.report.checks
        if c.check_id.startswith("monthly_")
    )
