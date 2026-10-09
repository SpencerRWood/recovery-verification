"""Real Dagster Definitions load and local execution through the generic boundary."""

import json
from pathlib import Path
from typing import Any

import pytest
from dagster import Definitions, RunRequest, SkipReason
from dagster._core.workspace.autodiscovery import loadable_targets_from_python_module
from tests.unit.test_monthly import Provider, unsupported
from tests.unit.test_weekly import consumer

from recovery_verification import monthly, readiness, weekly
from recovery_verification.contract import parse_manifest
from recovery_verification.dagster.definitions import defs
from recovery_verification.dagster.jobs import (
    PreflightConfig,
    contract_preflight,
    recovery_readiness_daily_schedule,
)
from recovery_verification.drill import execute_drill
from recovery_verification.probes import BoundedProbe, result


def test_grpc_module_discovery_exposes_one_code_location() -> None:
    targets = loadable_targets_from_python_module(
        "recovery_verification.dagster.definitions", working_directory=None
    )
    assert len(targets) == 1
    assert targets[0].attribute == "defs"
    assert targets[0].target_definition is defs


def test_definitions_and_smoke() -> None:
    Definitions.validate_loadable(defs)
    assert {job.name for job in defs.resolve_all_job_defs()} == {
        "runtime_smoke_job",
        "contract_preflight_job",
        "recovery_readiness_daily",
        "recovery_verification_weekly",
        "recovery_drill_monthly",
        "__ASSET_JOB",
    }
    assert defs.schedules
    assert not defs.sensors
    result = defs.get_job_def("runtime_smoke_job").execute_in_process()
    assert result.success
    assert result.output_for_node("runtime_smoke") == "ok"


def test_preflight_job(document: dict[str, Any]) -> None:
    result = defs.get_job_def("contract_preflight_job").execute_in_process(
        run_config={
            "ops": {
                "contract_preflight": {
                    "config": {
                        "manifest_json": json.dumps(document),
                        "target_ids": ["example-linux"],
                    }
                }
            }
        }
    )
    assert result.success
    output = json.loads(result.output_for_node("contract_preflight"))
    assert output["contract_state"] == "passed"
    assert output["readiness_state"] == "unavailable"


def test_invalid_manifest_job_fails_closed() -> None:
    result = defs.get_job_def("contract_preflight_job").execute_in_process(
        run_config={
            "ops": {
                "contract_preflight": {
                    "config": {
                        "manifest_json": '{"schema_version":"2"}',
                    }
                }
            }
        },
        raise_on_error=False,
    )
    assert not result.success


def test_job_selection_fails_closed(document: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="invalid target selection"):
        contract_preflight(
            PreflightConfig(manifest_json=json.dumps(document), target_ids=["absent"])
        )


@pytest.mark.parametrize(
    ("state", "expected"),
    [("passed", "READY"), ("failed", "NOT_READY"), ("unavailable", "UNKNOWN")],
)
def test_daily_job_uses_shared_checks(
    document: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    state: str,
    expected: str,
) -> None:
    for name in ("_checkout", "_interfaces", "_consumer"):
        monkeypatch.setattr(readiness, name, lambda *_: result("passed", "local_valid"))
    monkeypatch.setattr(
        BoundedProbe, "__call__", lambda *_: result(state, "probe_result")
    )
    run = defs.resolve_job_def("recovery_readiness_daily").execute_in_process(
        run_config={
            "ops": {
                "recovery_readiness": {
                    "config": {
                        "manifest_json": json.dumps(document),
                        "checkouts": {"example-linux": str(tmp_path)},
                    }
                }
            }
        }
    )
    assert run.success
    report = json.loads(run.output_for_node("recovery_readiness"))
    assert report["readiness"] == expected
    evaluations = run.get_asset_check_evaluations()
    assert len(evaluations) == 1
    assert evaluations[0].passed == (expected == "READY")
    assert evaluations[0].metadata["evidence"].value == report


def test_daily_schedule_explicit_configuration(
    document: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("RECOVERY_READINESS_CONFIG", raising=False)
    assert isinstance(recovery_readiness_daily_schedule(), SkipReason)
    source = tmp_path / "daily.json"
    monkeypatch.setenv("RECOVERY_READINESS_CONFIG", str(source))
    assert isinstance(recovery_readiness_daily_schedule(), SkipReason)
    source.write_text(json.dumps({"manifest_json": json.dumps(document)}))
    request = recovery_readiness_daily_schedule()
    assert isinstance(request, RunRequest)
    assert request.run_config["ops"]["recovery_readiness"]["config"]["manifest_json"]
    source.write_text('{"manifest_json":"invalid"}')
    assert isinstance(recovery_readiness_daily_schedule(), SkipReason)
    assert recovery_readiness_daily_schedule.execution_timezone == "UTC"


@pytest.mark.parametrize("state", ["passed", "failed", "unavailable"])
def test_weekly_job_reuses_daily_asset_and_preserves_evidence(
    document: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    state: str,
) -> None:
    for name in ("_checkout", "_interfaces", "_consumer"):
        monkeypatch.setattr(readiness, name, lambda *_: result("passed", "local_valid"))
    monkeypatch.setattr(weekly, "_checkout", lambda *_: result("passed", "local_valid"))
    monkeypatch.setattr(
        BoundedProbe, "__call__", lambda *_: result("passed", "probe_valid")
    )
    observed = consumer(state)
    monkeypatch.setattr(weekly, "invoke_verification", lambda *_a, **_k: observed)
    config = {
        "manifest_json": json.dumps(document),
        "checkouts": {"example-linux": str(tmp_path)},
    }
    run = defs.resolve_job_def("recovery_verification_weekly").execute_in_process(
        run_config={
            "ops": {
                "recovery_readiness": {"config": config},
                "recovery_verification": {"config": config},
            }
        }
    )
    assert run.success
    report = json.loads(run.output_for_node("recovery_verification"))
    daily = json.loads(run.output_for_node("recovery_readiness"))
    assert report["checks"][: len(daily["checks"])] == daily["checks"]
    assert report["consumer_evidence"][
        "example-linux/synthetic"
    ] == observed.model_dump(mode="json")
    evaluations = run.get_asset_check_evaluations()
    assert len(evaluations) == 2
    check = next(
        item for item in evaluations if item.check_name == "recovery_paths_verified"
    )
    assert check.passed == (state == "passed")
    assert check.metadata["evidence"].value == report


@pytest.mark.parametrize("supported", [True, False])
def test_monthly_job_reuses_shared_daily_evidence(
    document: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    supported: bool,
) -> None:
    manifest = (
        parse_manifest(json.dumps(document)) if supported else unsupported(document)
    )
    for module, names in (
        (readiness, ("_checkout", "_interfaces", "_consumer")),
        (weekly, ("_checkout",)),
        (monthly, ("_checkout",)),
    ):
        for name in names:
            monkeypatch.setattr(module, name, lambda *_: result("passed", "fixture_ok"))
    monkeypatch.setattr(
        BoundedProbe, "__call__", lambda *_: result("passed", "fixture_ok")
    )
    monkeypatch.setattr(weekly, "invoke_verification", lambda *_a, **_k: consumer())
    if supported:
        observed = execute_drill(manifest.targets[0], Provider())
        monkeypatch.setattr(monthly, "invoke_drill", lambda *_a, **_k: observed)
    config = {
        "manifest_json": manifest.model_dump_json(),
        "checkouts": {"example-linux": str(tmp_path)},
    }
    run = defs.resolve_job_def("recovery_drill_monthly").execute_in_process(
        run_config={
            "ops": {
                "recovery_readiness": {"config": config},
                "recovery_drill": {"config": config},
            }
        }
    )
    assert run.success
    report = json.loads(run.output_for_node("recovery_drill"))
    daily = json.loads(run.output_for_node("recovery_readiness"))
    assert report["checks"][: len(daily["checks"])] == daily["checks"]
    assert report["readiness"] == ("READY" if supported else "UNKNOWN")
    check = next(
        item
        for item in run.get_asset_check_evaluations()
        if item.check_name == "isolated_recovery_proven"
    )
    assert check.passed == supported
    assert check.metadata["evidence"].value == report
