"""Small, versioned wire contract; executable implementations stay with consumers."""

import json
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

Identifier = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")]
Reference = Annotated[str, Field(min_length=1, max_length=512, pattern=r"^\S+$")]
Level = Literal["readiness", "verification", "drill"]


class ContractModel(BaseModel):
    """Reject unknown fields/coercion and keep validated declarations immutable."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


class Command(ContractModel):
    """Repository-relative executable and argv, never shell text or inline code."""

    entrypoint: Annotated[
        str, Field(pattern=r"^(?:[a-zA-Z0-9_-]+/)*[a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]+)*$")
    ]
    args: tuple[Reference, ...]
    timeout_seconds: Annotated[int, Field(ge=1, le=3600)]


class Entrypoints(ContractModel):
    """Both interfaces are declared, even for readiness-only initial consumers."""

    bootstrap: Command
    recovery: Command


class ValidationCommand(ContractModel):
    id: Identifier
    levels: Annotated[tuple[Level, ...], Field(min_length=1)]
    command: Command


class Dependency(ContractModel):
    """References only; secret values and provider-specific config are forbidden."""

    id: Identifier
    kind: Literal["repository", "secret", "artifact", "storage", "tool", "runbook"]
    reference: Reference


class BackupSource(ContractModel):
    id: Identifier
    reference: Reference
    max_age_seconds: Annotated[int, Field(ge=1)]


class CadenceOverride(ContractModel):
    """Declarative interval override; interpreted by later cadence implementation."""

    level: Level
    interval_seconds: Annotated[int, Field(ge=1)]


def _unique(values: tuple[str, ...]) -> bool:
    return len(values) == len(set(values))


class Target(ContractModel):
    id: Identifier
    owning_repository: Annotated[
        str, Field(pattern=r"^[A-Za-z0-9_-]+/[A-Za-z0-9_.-]+$")
    ]
    revision: Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
    platform: Literal["linux"]
    adapter: Literal["consumer-command-v1"]
    entrypoints: Entrypoints
    validation_commands: Annotated[tuple[ValidationCommand, ...], Field(min_length=1)]
    required_dependencies: tuple[Dependency, ...]
    backup_sources: tuple[BackupSource, ...]
    allowed_verification_levels: Annotated[tuple[Level, ...], Field(min_length=1)]
    cadence_overrides: tuple[CadenceOverride, ...]

    @model_validator(mode="after")
    def consistent_capabilities(self) -> Self:
        """Reject ambiguous identities and declarations outside allowed levels."""
        groups = (
            self.allowed_verification_levels,
            tuple(item.id for item in self.validation_commands),
            tuple(item.id for item in self.required_dependencies),
            tuple(item.id for item in self.backup_sources),
            tuple(item.level for item in self.cadence_overrides),
        )
        if not all(_unique(group) for group in groups):
            raise ValueError("duplicate declaration")
        allowed = set(self.allowed_verification_levels)
        for check in self.validation_commands:
            if not _unique(check.levels) or not set(check.levels) <= allowed:
                raise ValueError("invalid validation levels")
        if not {item.level for item in self.cadence_overrides} <= allowed:
            raise ValueError("cadence outside allowed levels")
        if {
            level for item in self.validation_commands for level in item.levels
        } != allowed:
            raise ValueError("every allowed level requires a validation command")
        return self


class Manifest(ContractModel):
    schema_version: Literal["1"]
    targets: Annotated[tuple[Target, ...], Field(min_length=1)]

    @model_validator(mode="after")
    def unique_targets(self) -> Self:
        if not _unique(tuple(target.id for target in self.targets)):
            raise ValueError("duplicate target identity")
        return self


class ContractError(ValueError):
    """Safe public error; never includes submitted commands, values or JSON."""


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ContractError("duplicate JSON key")
        result[key] = value
    return result


def parse_manifest(raw: str) -> Manifest:
    """Parse strict JSON without legacy formats, schema fallbacks or input echoes."""
    try:
        document = json.loads(raw, object_pairs_hook=_reject_duplicate_keys)
        return Manifest.model_validate_json(json.dumps(document), strict=True)
    except ValueError, ValidationError, RecursionError:
        raise ContractError("invalid recovery manifest") from None
