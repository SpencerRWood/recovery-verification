"""Stopped-by-default cadence and proportional, commit-bound change requests."""

import json
import os
import re
import subprocess
from pathlib import Path, PurePosixPath
from typing import Annotated
from urllib.parse import urlsplit

from dagster import RunRequest
from dagster._utils.schedules import is_valid_cron_schedule
from pydantic import AwareDatetime, Field, model_validator

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
    notification_start_time: AwareDatetime | None = None
    notification_dagster_url: Annotated[str, Field(max_length=512)] | None = None
    notification_code_location: Annotated[str, Field(min_length=1, max_length=128)] = (
        "recovery-verification"
    )
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
        if self.notifications_enabled:
            url = urlsplit(self.notification_dagster_url or "")
            if (
                self.notification_start_time is None
                or url.scheme != "https"
                or not url.hostname
                or url.username
                or url.password
                or url.query
                or url.fragment
            ):
                raise ValueError("explicit_terminal_notification_inputs_required")
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


# Configuration formats are dependency inputs regardless of consumer or filename.
CONFIG_SUFFIXES = frozenset(
    {".yml", ".yaml", ".json", ".toml", ".ini", ".cfg", ".conf", ".env"}
)
IMPLEMENTATION_PARTS = frozenset({"roles", "playbook", "playbooks"})
RECOVERY_PARTS = frozenset({"bootstrap", "recovery", "backup", "restore"})
DEPENDENCY_PARTS = frozenset(
    {
        "ansible",
        "inventory",
        "inventories",
        "compose",
        "docker",
        "environment",
        "environments",
        "config",
        "configuration",
        "mount",
        "mounts",
        "secret",
        "secrets",
    }
)
PASSIVE_SUFFIXES = frozenset({".md", ".rst", ".txt", ".png", ".jpg", ".svg"})


def _path_tier(path: str, *, isolated_test_mode: bool) -> Level | None:
    # Reject malformed/ambiguous diff inputs instead of consuming their revision.
    parts = path.split("/")
    if not path or any(p in {"", ".", ".."} for p in parts) or "\0" in path:
        raise ValueError("invalid_change_path")
    name = PurePosixPath(path.lower())
    tokens = frozenset(re.split(r"[/_.-]+", path.lower()))
    if "bootstrap" in tokens and isolated_test_mode:
        return "drill"
    if "bootstrap" in tokens or tokens & IMPLEMENTATION_PARTS:
        return "verification"
    if name.suffix in CONFIG_SUFFIXES or "env" in tokens or name.name == "dockerfile":
        return "readiness"
    if tokens & RECOVERY_PARTS:
        return "verification"
    # Only positively identified passive files may be ignored. Unknown formats
    # conservatively request readiness; no content/secret inspection is needed.
    if name.suffix in PASSIVE_SUFFIXES and not tokens & DEPENDENCY_PARTS:
        return None
    return "readiness"


def proportional_tier(
    paths: tuple[str, ...], *, isolated_test_mode: bool
) -> Level | None:
    """Select the strongest generic path tier; classify every path before return."""
    tiers = {_path_tier(p, isolated_test_mode=isolated_test_mode) for p in paths}
    return next(
        (tier for tier in ("drill", "verification", "readiness") if tier in tiers), None
    )


def changed_paths(root: Path, previous: str, current: str) -> tuple[str, ...]:
    if not all(
        re.fullmatch(r"[0-9a-f]{40}", revision) for revision in (previous, current)
    ):
        raise ValueError("invalid_change_revision")
    result = subprocess.run(  # noqa: S603 -- Git argument vector; revisions are schema-validated by callers.
        [  # noqa: S607 -- The operator's Git is required; diff helpers are disabled.
            "git",
            "diff",
            "--no-ext-diff",
            "--no-textconv",
            "--no-renames",
            "--name-only",
            "-z",
            previous,
            current,
            "--",
        ],
        cwd=root,
        capture_output=True,
        timeout=10,
        check=True,
    )
    if len(result.stdout) > 65536:
        raise ValueError("change_path_limit")
    if result.stdout and not result.stdout.endswith(b"\0"):
        raise ValueError("invalid_change_diff")
    paths = tuple(result.stdout.decode().split("\0")[:-1])
    if any(not p for p in paths):
        raise ValueError("invalid_change_diff")
    return paths


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
