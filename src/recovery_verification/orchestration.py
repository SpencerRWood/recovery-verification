"""Stopped-by-default cadence and proportional, commit-bound change requests."""

import json
import os
import subprocess
from pathlib import Path
from typing import Annotated

from dagster import RunRequest
from dagster._utils.schedules import is_valid_cron_schedule
from pydantic import Field, model_validator

from recovery_verification.contract import (
    Command,
    ContractModel,
    Level,
    Manifest,
    parse_manifest,
)
from recovery_verification.history import read_bounded
from recovery_verification.scorecard import ScorecardPolicy, ensure_no_runtime_secrets

JOBS: dict[Level, str] = {
    "readiness": "recovery_readiness_daily",
    "verification": "recovery_verification_weekly",
    "drill": "recovery_drill_monthly",
}
ASSETS: dict[Level, tuple[str, ...]] = {
    "readiness": ("recovery_readiness",),
    "verification": ("recovery_readiness", "recovery_verification"),
    "drill": ("recovery_readiness", "recovery_drill"),
}


class Cadence(ContractModel):
    daily: str
    weekly: str
    monthly: str

    @model_validator(mode="after")
    def valid_crons(self) -> Cadence:
        if not all(is_valid_cron_schedule(c) for c in self.model_dump().values()):
            raise ValueError("invalid_recovery_cadence")
        return self


def default_cadence() -> Cadence:
    return Cadence.model_validate_json(
        Path(__file__).with_name("cadence.json").read_bytes()
    )


class OrchestrationConfig(ContractModel):
    """Non-secret deployment inputs only; runtime tokens remain process-injected."""

    manifest_json: str
    checkouts: dict[str, str]
    state_directory: str
    environment: Annotated[str, Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")]
    cadence: Cadence = Field(default_factory=default_cadence)
    manifest_exports: dict[str, Command] = Field(default_factory=dict)
    isolated_test_mode: bool = False
    notifications_enabled: bool = False
    policy: ScorecardPolicy = Field(default_factory=ScorecardPolicy)
    probe_attempts: Annotated[int, Field(ge=1, le=3)] = 2

    @model_validator(mode="after")
    def explicit_inputs(self) -> OrchestrationConfig:
        manifest = parse_manifest(self.manifest_json)
        ids = {t.id for t in manifest.targets}
        if set(self.checkouts) != ids or set(self.manifest_exports) - ids:
            raise ValueError("invalid_operational_checkout_mapping")
        if not all(Path(p).is_absolute() for p in self.checkouts.values()):
            raise ValueError("absolute_checkouts_required")
        if not Path(self.state_directory).is_absolute():
            raise ValueError("absolute_state_directory_required")
        ensure_no_runtime_secrets(self.model_dump_json())
        return self


def load_config() -> OrchestrationConfig:
    source = os.environ.get("RECOVERY_ORCHESTRATION_CONFIG")
    if not source:
        raise ValueError("orchestration_configuration_not_provided")
    try:
        return OrchestrationConfig.model_validate_json(read_bounded(Path(source)))
    except OSError, ValueError:
        raise ValueError("orchestration_configuration_invalid") from None


def deployed_cadence() -> Cadence:
    # Invalid configured deployment fails code-location loading rather than
    # silently reverting to another schedule. Unconfigured definitions stay stopped.
    return (
        load_config().cadence
        if os.environ.get("RECOVERY_ORCHESTRATION_CONFIG")
        else default_cadence()
    )


def current_manifest(config: OrchestrationConfig) -> Manifest:
    declared = parse_manifest(config.manifest_json)
    targets = []
    for target in declared.targets:
        command = config.manifest_exports.get(target.id)
        if command is None:
            targets.append(target)
            continue
        root = Path(config.checkouts[target.id]).resolve()
        path = root / command.entrypoint
        if path.is_symlink() or not path.resolve().is_relative_to(root):
            raise ValueError("unsafe_manifest_export")
        result = subprocess.run(  # noqa: S603 -- Explicit trusted, repository-relative export declaration.
            [str(path), *command.args],
            cwd=root,
            capture_output=True,
            timeout=min(command.timeout_seconds, 15),
            check=True,
            env={
                "PATH": os.environ.get("PATH", os.defpath),
                "PYTHONDONTWRITEBYTECODE": "1",
            },
        )
        if len(result.stdout) > 65536:
            raise ValueError("manifest_export_limit")
        exported = parse_manifest(result.stdout.decode())
        if len(exported.targets) != 1 or (
            exported.targets[0].id,
            exported.targets[0].owning_repository,
        ) != (target.id, target.owning_repository):
            raise ValueError("manifest_export_identity_mismatch")
        targets.append(exported.targets[0])
    return declared.model_copy(update={"targets": tuple(targets)})


def execution_request(
    config: OrchestrationConfig,
    manifest: Manifest,
    tier: Level,
    execution_key: str,
    target_ids: tuple[str, ...] = (),
) -> RunRequest:
    selected = target_ids or tuple(t.id for t in manifest.targets)
    if set(selected) - {t.id for t in manifest.targets}:
        raise ValueError("invalid_operational_target_selection")
    inputs = {
        "manifest_json": manifest.model_dump_json(),
        "checkouts": config.checkouts,
        "target_ids": list(selected),
    }
    return RunRequest(
        run_key=execution_key,
        job_name=JOBS[tier],
        run_config={"ops": {a: {"config": inputs} for a in ASSETS[tier]}},
        tags={
            "recovery/execution_key": execution_key,
            "recovery/environment": config.environment,
            "recovery/tier": tier,
            "dagster/max_runtime": "3600",
            "recovery/operation": "verification",
        },
    )


def proportional_tier(
    paths: tuple[str, ...], *, isolated_test_mode: bool
) -> Level | None:
    """Conservative path classification; no consumer-specific branches."""
    lowered = tuple(p.lower() for p in paths)
    relevant = tuple(
        p
        for p in lowered
        if any(
            part in p
            for part in (
                "ansible/",
                "inventory",
                "inventories",
                "bootstrap",
                "recovery",
                "backup",
                "restore",
                "mount",
                "secret",
            )
        )
    )
    if not relevant:
        return None
    if any("bootstrap" in p for p in relevant) and isolated_test_mode:
        return "drill"
    if any(
        part in p
        for p in relevant
        for part in ("roles/", "playbook", "bootstrap", "recovery", "backup", "restore")
    ):
        return "verification"
    return "readiness"


def changed_paths(root: Path, previous: str, current: str) -> tuple[str, ...]:
    result = subprocess.run(  # noqa: S603 -- Git argument vector; revisions are schema-validated by callers.
        ["git", "diff", "--name-only", "-z", previous, current, "--"],  # noqa: S607
        cwd=root,
        capture_output=True,
        timeout=10,
        check=True,
    )
    if len(result.stdout) > 65536:
        raise ValueError("change_path_limit")
    return tuple(p for p in result.stdout.decode().split("\0") if p)


def revision_cursor(raw: str | None) -> dict[str, str]:
    values = json.loads(raw) if raw else {}
    if not isinstance(values, dict) or any(
        not isinstance(k, str)
        or not isinstance(v, str)
        or len(v) != 40
        or any(c not in "0123456789abcdef" for c in v)
        for k, v in values.items()
    ):
        raise ValueError("invalid_change_cursor")
    return values
