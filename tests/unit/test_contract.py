"""Fail-closed wire parsing and consumer ownership contracts."""

import json
from copy import deepcopy
from typing import Any

import pytest
from pydantic import ValidationError

from recovery_verification.contract import (
    BackupSource,
    CadenceOverride,
    Command,
    ContractError,
    ContractModel,
    Dependency,
    Entrypoints,
    Manifest,
    ValidationCommand,
    parse_manifest,
)


def test_valid_contract(document: dict[str, Any]) -> None:
    manifest = parse_manifest(json.dumps(document))
    assert manifest.targets[0].owning_repository == "example/linux-consumer"
    assert parse_manifest(manifest.model_dump_json()) == manifest


@pytest.mark.parametrize("field", list(Manifest.model_fields))
def test_required_manifest_fields(document: dict[str, Any], field: str) -> None:
    del document[field]
    with pytest.raises(ContractError):
        parse_manifest(json.dumps(document))


@pytest.mark.parametrize(
    "field",
    [
        "id",
        "owning_repository",
        "revision",
        "platform",
        "adapter",
        "entrypoints",
        "validation_commands",
        "required_dependencies",
        "backup_sources",
        "allowed_verification_levels",
        "cadence_overrides",
    ],
)
def test_required_target_fields(document: dict[str, Any], field: str) -> None:
    del document["targets"][0][field]
    with pytest.raises(ContractError):
        parse_manifest(json.dumps(document))


@pytest.mark.parametrize("version", ["0", "2", 1, True, None])
def test_unsupported_versions(document: dict[str, Any], version: object) -> None:
    document["schema_version"] = version
    with pytest.raises(ContractError):
        parse_manifest(json.dumps(document))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("adapter", "provider-ansible"),
        ("platform", "specific-host"),
        ("allowed_verification_levels", ["production-restore"]),
        ("allowed_verification_levels", []),
        ("validation_commands", []),
        ("owning_repository", "https://user:secret@example.com/repo"),
        ("revision", "main"),
        ("required_dependencies", None),
        ("compose", {"services": {}}),
        ("ansible", "roles/provision"),
        ("restore_script", "embedded-secret"),
    ],
)
def test_unsupported_capabilities_and_inline_domain_logic(
    document: dict[str, Any], field: str, value: object
) -> None:
    document["targets"][0][field] = value
    with pytest.raises(ContractError, match=r"^invalid recovery manifest$") as error:
        parse_manifest(json.dumps(document))
    assert "embedded-secret" not in str(error.value)


@pytest.mark.parametrize(
    "entrypoint",
    ["/bin/sh", "../restore", "scripts/../restore", "sh -c", "a;b", "./recover"],
)
def test_entrypoints_stay_in_consumer(
    document: dict[str, Any], entrypoint: str
) -> None:
    document["targets"][0]["entrypoints"]["recovery"]["entrypoint"] = entrypoint
    with pytest.raises(ContractError):
        parse_manifest(json.dumps(document))


@pytest.mark.parametrize("timeout", [0, -1, 3601, "30", True])
def test_bounded_command_timeout(document: dict[str, Any], timeout: object) -> None:
    document["targets"][0]["entrypoints"]["bootstrap"]["timeout_seconds"] = timeout
    with pytest.raises(ContractError):
        parse_manifest(json.dumps(document))


@pytest.mark.parametrize(
    "field",
    [
        "validation_commands",
        "required_dependencies",
        "backup_sources",
        "cadence_overrides",
        "allowed_verification_levels",
    ],
)
def test_duplicate_declarations(document: dict[str, Any], field: str) -> None:
    document["targets"][0][field] *= 2
    with pytest.raises(ContractError):
        parse_manifest(json.dumps(document))


def test_duplicate_targets(document: dict[str, Any]) -> None:
    document["targets"] *= 2
    with pytest.raises(ContractError):
        parse_manifest(json.dumps(document))


@pytest.mark.parametrize("levels", [["drill"], ["readiness", "readiness"]])
def test_invalid_check_levels(document: dict[str, Any], levels: list[str]) -> None:
    document["targets"][0]["allowed_verification_levels"] = ["readiness"]
    document["targets"][0]["validation_commands"][0]["levels"] = levels
    with pytest.raises(ContractError):
        parse_manifest(json.dumps(document))


def test_cadence_requires_allowed_level(document: dict[str, Any]) -> None:
    document["targets"][0]["allowed_verification_levels"] = ["readiness"]
    document["targets"][0]["validation_commands"][0]["levels"] = ["readiness"]
    with pytest.raises(ContractError):
        parse_manifest(json.dumps(document))


def test_each_level_requires_validation(document: dict[str, Any]) -> None:
    document["targets"][0]["validation_commands"][0]["levels"] = ["readiness"]
    with pytest.raises(ContractError):
        parse_manifest(json.dumps(document))


@pytest.mark.parametrize(
    "raw",
    [
        "{",
        "[]",
        "null",
        '{"schema_version":"1","schema_version":"2"}',
        '{"secret":"do-not-echo"}',
    ],
)
def test_bad_json_is_sanitized(raw: str) -> None:
    with pytest.raises(ContractError) as error:
        parse_manifest(raw)
    assert str(error.value) == "invalid recovery manifest"


def test_selected_target_does_not_hide_invalid_consumer(
    document: dict[str, Any],
) -> None:
    invalid = deepcopy(document["targets"][0])
    invalid["id"] = "another"
    invalid["adapter"] = "unknown"
    document["targets"].append(invalid)
    with pytest.raises(ContractError):
        parse_manifest(json.dumps(document))


@pytest.mark.parametrize(
    ("model", "sample"),
    [
        (Command, {"entrypoint": "scripts/check", "args": [], "timeout_seconds": 60}),
        (
            Entrypoints,
            {
                "bootstrap": {
                    "entrypoint": "scripts/bootstrap",
                    "args": [],
                    "timeout_seconds": 60,
                },
                "recovery": {
                    "entrypoint": "scripts/recover",
                    "args": [],
                    "timeout_seconds": 60,
                },
            },
        ),
        (
            ValidationCommand,
            {
                "id": "check",
                "levels": ["readiness"],
                "command": {
                    "entrypoint": "scripts/check",
                    "args": [],
                    "timeout_seconds": 60,
                },
            },
        ),
        (Dependency, {"id": "repo", "kind": "repository", "reference": "example/repo"}),
        (
            BackupSource,
            {"id": "state", "reference": "backup://state", "max_age_seconds": 60},
        ),
        (CadenceOverride, {"level": "readiness", "interval_seconds": 60}),
    ],
)
def test_nested_required_fields_and_unknown_configuration(
    model: type[ContractModel],
    sample: dict[str, Any],
) -> None:
    for field in model.model_fields:
        missing = {key: value for key, value in sample.items() if key != field}
        with pytest.raises(ValidationError):
            model.model_validate_json(json.dumps(missing))
    sample["service_config"] = {"password": "do-not-echo"}
    with pytest.raises(ValidationError):
        model.model_validate_json(json.dumps(sample))


def test_explicit_empty_dependencies_and_backups(document: dict[str, Any]) -> None:
    document["targets"][0]["required_dependencies"] = []
    document["targets"][0]["backup_sources"] = []
    document["targets"][0]["cadence_overrides"] = []
    assert parse_manifest(json.dumps(document)).targets[0].backup_sources == ()
