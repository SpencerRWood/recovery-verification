"""Real Dagster Definitions load and local execution through the generic boundary."""

import json
from typing import Any

import pytest
from dagster import Definitions

from recovery_verification.dagster.definitions import defs
from recovery_verification.dagster.jobs import PreflightConfig, contract_preflight


def test_definitions_and_smoke() -> None:
    Definitions.validate_loadable(defs)
    assert {job.name for job in defs.resolve_all_job_defs()} == {
        "runtime_smoke_job",
        "contract_preflight_job",
    }
    assert not defs.schedules
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
