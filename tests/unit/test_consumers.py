"""Both consumer wire fixtures use the same public provider interfaces."""

from pathlib import Path

import pytest

from recovery_verification.adapters import validation_plan
from recovery_verification.contract import parse_manifest
from recovery_verification.preflight import preflight


@pytest.mark.parametrize("name", ["infrastructure", "homelab"])
def test_consumer_contract_fixture(name: str) -> None:
    fixture = Path(__file__).parents[1] / "fixtures" / f"{name}.v1.json"
    manifest = parse_manifest(fixture.read_text())
    target = manifest.targets[0]
    assert target.allowed_verification_levels == ("readiness",)
    assert target.entrypoints.recovery.entrypoint == "scripts/rollback.py"
    assert "--apply" not in target.entrypoints.recovery.args
    assert target.backup_sources
    kinds = {dependency.kind for dependency in target.required_dependencies}
    assert {"repository", "secret", "artifact", "storage", "runbook"} <= kinds
    plan = validation_plan(target, "readiness")
    assert plan[0].command.entrypoint == "scripts/recovery.py"
    assert plan[0].command.args == ("preflight",)
    assert preflight(manifest).readiness_state == "unavailable"
