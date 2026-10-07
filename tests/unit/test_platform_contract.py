"""Published schema and platform runtime wiring remain aligned with provider code."""

import json
import tomllib
from pathlib import Path

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
    assert "release-container.yml@v3" in caller
    assert "packages: write" in caller
