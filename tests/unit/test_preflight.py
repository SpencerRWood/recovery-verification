"""No execution, consumer-specific dispatch, or false recovery-readiness claims."""

import json
import subprocess
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from recovery_verification.adapters import validation_plan
from recovery_verification.cli import main
from recovery_verification.contract import parse_manifest
from recovery_verification.models import State, normalize_readiness
from recovery_verification.preflight import preflight


def test_consumer_boundary_and_no_execution(
    document: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail(f"unexpected execution: {len(args)} {len(kwargs)}")

    monkeypatch.setattr(subprocess, "run", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    for name in ("infrastructure", "homelab", "future-linux"):
        target = deepcopy(document["targets"][0])
        target["id"] = name
        target["owning_repository"] = f"example/{name}"
        document["targets"].append(target)
    manifest = parse_manifest(json.dumps(document))
    result = preflight(manifest)
    assert result == preflight(manifest)
    assert result.contract_state == "passed"
    assert result.readiness_state == "unavailable"
    assert len(result.checks) == 4
    assert all(check.evidence.tested_revision == "1" * 40 for check in result.checks)
    assert len(result.checks[0].evidence.manifest_sha256) == 64
    selected = preflight(manifest, ("homelab",))
    assert [check.target_id for check in selected.checks] == ["homelab"]
    assert (
        selected.checks[0].evidence.manifest_sha256
        == result.checks[0].evidence.manifest_sha256
    )
    for target in manifest.targets:
        plan = validation_plan(target, "verification")
        assert plan[0].owning_repository == target.owning_repository
        assert plan[0].revision == target.revision
        assert plan[0].command.entrypoint == "scripts/validate"


@pytest.mark.parametrize("selection", [("absent",), ("example-linux", "example-linux")])
def test_invalid_selection(
    document: dict[str, Any], selection: tuple[str, ...]
) -> None:
    with pytest.raises(ValueError, match="invalid target selection"):
        preflight(parse_manifest(json.dumps(document)), selection)


def test_disallowed_level(document: dict[str, Any]) -> None:
    target = document["targets"][0]
    target["ephemeral_rebuild"] = {
        "supported": False,
        "provider": None,
        "environment": None,
        "reason": "not_supported",
    }
    target["allowed_verification_levels"] = ["readiness"]
    target["validation_commands"][0]["levels"] = ["readiness"]
    target["cadence_overrides"] = []
    with pytest.raises(ValueError, match="unsupported verification level"):
        validation_plan(parse_manifest(json.dumps(document)).targets[0], "drill")


def test_evidence_binds_content(document: dict[str, Any]) -> None:
    before = preflight(parse_manifest(json.dumps(document)))
    document["targets"][0]["revision"] = "2" * 40
    after = preflight(parse_manifest(json.dumps(document)))
    assert before.checks[0].evidence != after.checks[0].evidence


def test_manifest_format_does_not_change_evidence(document: dict[str, Any]) -> None:
    compact = preflight(parse_manifest(json.dumps(document)))
    formatted = preflight(
        parse_manifest(json.dumps(document, indent=4, sort_keys=True))
    )
    assert compact == formatted


@pytest.mark.parametrize(
    ("states", "expected"),
    [
        ((), "unavailable"),
        (("passed",), "passed"),
        (("passed", "failed", "unavailable"), "failed"),
        (("passed", "unavailable"), "unavailable"),
        (("passed", "skipped"), "skipped"),
        (("not_applicable",), "not_applicable"),
        (("passed", "not_applicable"), "passed"),
    ],
)
def test_readiness(states: tuple[State, ...], expected: State) -> None:
    assert normalize_readiness(states) == expected


def test_cli(
    document: dict[str, Any], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(document))
    assert main([str(path), "--target", "example-linux"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["contract_state"] == "passed"
    assert result["readiness_state"] == "unavailable"
    assert main([str(path), "--target", "absent"]) == 2
    assert json.loads(capsys.readouterr().out)["code"] == "invalid_input"
    path.write_text('{"secret":"never-echo-me"}')
    assert main([str(path)]) == 2
    assert "never-echo-me" not in capsys.readouterr().out
    path.unlink()
    assert main([str(path)]) == 2
    assert json.loads(capsys.readouterr().out)["contract_state"] == "failed"
