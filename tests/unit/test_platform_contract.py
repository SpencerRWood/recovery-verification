"""Published schema and platform runtime wiring remain aligned with provider code."""

import json
import re
import tomllib
from pathlib import Path

import yaml

from recovery_verification.contract import Manifest

ROOT = Path(__file__).parents[2]


def test_schema_export_is_current() -> None:
    schema = json.loads((ROOT / "docs/recovery-manifest.v1.schema.json").read_text())
    assert schema == Manifest.model_json_schema()


def test_runtime_release_contract() -> None:
    contract = tomllib.loads((ROOT / ".github/release.toml").read_text())
    assert contract["dagster"] == {
        "runtime_validation": True,
        "smoke_job": "runtime_smoke_job",
    }
    assert contract["container"]["image_name"] == "recovery-verification"
    assert contract["container"]["publish"] is True
    docker = (ROOT / "Dockerfile").read_text()
    assert "recovery_verification.dagster.definitions" in docker
    assert "${DAGSTER_GRPC_PORT:?DAGSTER_GRPC_PORT is required}" in docker
    assert "USER app" in docker
    caller = (ROOT / ".github/workflows/release.yml").read_text()
    workflow = yaml.safe_load(caller)
    release = workflow["jobs"]["release"]
    assert "release-container.yml@" in release["uses"]
    assert release["with"]["workflow_revision"] == release["uses"].split("@")[-1]
    assert re.fullmatch(r"[0-9a-f]{40}", release["with"]["workflow_revision"])
    assert (
        release["with"]["require_approval"]
        == "${{ github.event_name == 'workflow_dispatch' }}"
    )
    assert release["permissions"]["packages"] == "write"
    inputs = workflow.get("on", workflow.get(True))["workflow_dispatch"]["inputs"]
    assert inputs["approved_revision"]["type"] == "string"
    assert inputs["approved_revision"]["required"] is True
    assert not inputs["approved_revision"].get("default")
    assert inputs["recovery_approved"]["type"] == "boolean"
    assert inputs["recovery_approved"]["required"] is True
    assert inputs["recovery_approved"]["default"] is False
    assert release["with"]["approved_revision"] == "${{ inputs.approved_revision }}"
    assert release["with"]["recovery_approved"] == "${{ inputs.recovery_approved }}"
    assert release["permissions"] == {
        "contents": "write",
        "packages": "write",
        "id-token": "write",
        "attestations": "write",
    }
