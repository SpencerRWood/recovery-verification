"""Shared daily checks, revision-bound evidence and conservative readiness."""

import os
import subprocess
from collections.abc import Callable
from datetime import UTC, datetime
from functools import partial
from hashlib import sha256
from pathlib import Path
from time import monotonic
from typing import Annotated, Literal

from pydantic import Field, model_validator

from recovery_verification.contract import ContractModel, Identifier, Manifest, Target
from recovery_verification.invocation import InvocationError, _git, invoke_readiness
from recovery_verification.models import State

Category = Literal["none", "recovery_prerequisite", "verification_infrastructure"]


class Observation(ContractModel):
    state: State
    reason: Identifier
    category: Category = "none"


class ReadinessCheck(Observation):
    target_id: Identifier
    check_id: Annotated[str, Field(pattern=r"^[a-z][a-z0-9_-]{0,95}$")]
    tested_revision: Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
    timestamp: datetime
    duration_seconds: Annotated[float, Field(ge=0, allow_inf_nan=False)]


class ReadinessReport(ContractModel):
    schema_version: Literal["1"] = "1"
    manifest_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    timestamp: datetime
    checks: tuple[ReadinessCheck, ...]
    target_revisions: dict[str, str]
    readiness: Literal["READY", "NOT_READY", "UNKNOWN"]
    currency: Literal["current", "stale", "superseded"] = "current"

    @model_validator(mode="after")
    def consistent_evidence(self) -> ReadinessReport:
        identities = {(item.target_id, item.check_id) for item in self.checks}
        if len(identities) != len(self.checks) or self.timestamp.tzinfo is None:
            raise ValueError("invalid evidence identity")
        if any(
            item.timestamp.tzinfo is None
            or self.target_revisions.get(item.target_id) != item.tested_revision
            for item in self.checks
        ):
            raise ValueError("invalid evidence revision")
        expected = (
            readiness_state(self.checks) if self.currency == "current" else "UNKNOWN"
        )
        if self.readiness != expected:
            raise ValueError("inconsistent readiness")
        return self


def manifest_hash(manifest: Manifest) -> str:
    return sha256(manifest.model_dump_json().encode()).hexdigest()


def readiness_state(
    checks: tuple[ReadinessCheck, ...],
) -> Literal["READY", "NOT_READY", "UNKNOWN"]:
    if any(check.state == "failed" for check in checks):
        return "NOT_READY"
    if not checks or any(check.state in {"unavailable", "skipped"} for check in checks):
        return "UNKNOWN"
    return "READY" if any(check.state == "passed" for check in checks) else "UNKNOWN"


def assess_currency(
    report: ReadinessReport, manifest: Manifest, *, now: datetime, max_age_seconds: int
) -> ReadinessReport:
    """Historical success cannot attest to another manifest or a later cadence."""
    if now.tzinfo is None or max_age_seconds < 1:
        raise ValueError("invalid freshness policy")
    expected = {target.id: target.revision for target in manifest.targets}
    if report.manifest_sha256 != manifest_hash(manifest) or any(
        expected.get(key) != value for key, value in report.target_revisions.items()
    ):
        currency = "superseded"
    elif (
        any(
            check.timestamp.tzinfo is None
            or not 0 <= (now - check.timestamp).total_seconds() <= max_age_seconds
            for check in report.checks
        )
        or not 0 <= (now - report.timestamp).total_seconds() <= max_age_seconds
    ):
        currency = "stale"
    else:
        currency = "current"
    return report.model_copy(
        update={
            "currency": currency,
            "readiness": readiness_state(report.checks)
            if currency == "current"
            else "UNKNOWN",
        }
    )


Probe = Callable[[str, str, Path, int | None], Observation]


def _checkout(target: Target, root: Path) -> Observation:
    try:
        if (
            _git(root, "rev-parse", "--show-toplevel") != str(root.resolve())
            or _git(root, "rev-parse", "HEAD") != target.revision
            or _git(root, "status", "--porcelain", "--untracked-files=all")
        ):
            return Observation(
                state="unavailable",
                reason="checkout_not_current",
                category="verification_infrastructure",
            )
        if _git(root, "remote", "get-url", "origin") not in {
            f"https://github.com/{target.owning_repository}.git",
            f"https://github.com/{target.owning_repository}",
            f"git@github.com:{target.owning_repository}.git",
        }:
            return Observation(
                state="unavailable",
                reason="checkout_owner_mismatch",
                category="verification_infrastructure",
            )
    except OSError, ValueError, subprocess.SubprocessError:
        return Observation(
            state="unavailable",
            reason="checkout_unavailable",
            category="verification_infrastructure",
        )
    return Observation(state="passed", reason="exact_clean_checkout")


def _consumer(target: Target, root: Path, check_id: str) -> Observation:
    try:
        result = invoke_readiness(target, root, check_id, trusted=True)
    except InvocationError:
        return Observation(
            state="unavailable",
            reason="consumer_unavailable",
            category="verification_infrastructure",
        )
    states = tuple(check.state for check in result.checks)
    state: State = "passed"
    for candidate in ("failed", "unavailable", "skipped"):
        if candidate in states:
            state = candidate
            break
    if not states:
        state = "unavailable"
    return Observation(
        state=state,
        reason="consumer_local_checks",
        category="recovery_prerequisite"
        if state == "failed"
        else "verification_infrastructure"
        if state != "passed"
        else "none",
    )


def _interfaces(target: Target, root: Path) -> Observation:
    commands = (
        target.entrypoints.bootstrap,
        target.entrypoints.recovery,
        *(item.command for item in target.validation_commands),
    )
    for command in commands:
        executable = root / command.entrypoint
        if (
            not executable.resolve().is_relative_to(root.resolve())
            or any(
                part.is_symlink()
                for part in (executable, *executable.parents)
                if part != root
            )
            or not executable.is_file()
            or not os.access(executable, os.X_OK)
        ):
            return Observation(
                state="failed",
                reason="entrypoint_missing_or_unsafe",
                category="recovery_prerequisite",
            )
    return Observation(state="passed", reason="entrypoints_present")


def run_readiness(  # noqa: PLR0912 -- Explicit selection/boundary guards fail closed.
    manifest: Manifest,
    checkouts: dict[str, Path],
    probe: Probe,
    *,
    target_ids: tuple[str, ...] = (),
    check_ids: tuple[str, ...] = (),
) -> ReadinessReport:
    """One shared implementation for Dagster and independent diagnostic runs.

    Explicit checkout mapping grants readiness invocation trust. All omitted
    required checks remain skipped, so a diagnostic subset never claims READY.
    """
    selected = target_ids or tuple(target.id for target in manifest.targets)
    if len(set(selected)) != len(selected) or set(selected) - {
        target.id for target in manifest.targets
    }:
        raise ValueError("invalid target selection")
    if len(set(check_ids)) != len(check_ids):
        raise ValueError("invalid check selection")
    targets = tuple(target for target in manifest.targets if target.id in selected)
    if any("readiness" not in target.allowed_verification_levels for target in targets):
        raise ValueError("readiness capability not declared")
    selectable = {
        "manifest_contract",
        "checkout",
        "entrypoints",
        *(
            f"consumer_{item.id}"
            for target in targets
            for item in target.validation_commands
            if "readiness" in item.levels
        ),
        *(
            f"dependency_{item.id}"
            for target in targets
            for item in target.required_dependencies
        ),
        *(f"backup_{item.id}" for target in targets for item in target.backup_sources),
    }
    if set(check_ids) - selectable:
        raise ValueError("invalid check selection")
    checks: list[ReadinessCheck] = []
    revisions: dict[str, str] = {}
    for target in manifest.targets:
        if target.id not in selected:
            continue
        revisions[target.id] = target.revision
        root = checkouts.get(target.id)
        boundary = (
            _checkout(target, root)
            if root
            else Observation(
                state="unavailable",
                reason="checkout_not_configured",
                category="verification_infrastructure",
            )
        )
        plans: list[tuple[str, Callable[[], Observation]]] = [
            (
                "manifest_contract",
                partial(Observation, state="passed", reason="manifest_valid"),
            ),
            ("checkout", partial(Observation.model_validate, boundary)),
        ]
        if root is not None:
            plans.append(("entrypoints", partial(_interfaces, target, root)))
            for validation in target.validation_commands:
                if "readiness" in validation.levels:
                    plans.append(
                        (
                            f"consumer_{validation.id}",
                            partial(_consumer, target, root, validation.id),
                        )
                    )
            for dependency in target.required_dependencies:
                plans.append(
                    (
                        f"dependency_{dependency.id}",
                        partial(
                            probe, dependency.kind, dependency.reference, root, None
                        ),
                    )
                )
            for backup in target.backup_sources:
                plans.append(
                    (
                        f"backup_{backup.id}",
                        partial(
                            probe,
                            "backup",
                            backup.reference,
                            root,
                            backup.max_age_seconds,
                        ),
                    )
                )
        else:
            plans.append(("entrypoints", partial(Observation.model_validate, boundary)))
            plans.extend(
                (f"consumer_{item.id}", partial(Observation.model_validate, boundary))
                for item in target.validation_commands
                if "readiness" in item.levels
            )
            plans.extend(
                (f"dependency_{item.id}", partial(Observation.model_validate, boundary))
                for item in target.required_dependencies
            )
            plans.extend(
                (f"backup_{item.id}", partial(Observation.model_validate, boundary))
                for item in target.backup_sources
            )
        for check_id, action in plans:
            start = monotonic()
            if check_ids and check_id not in check_ids:
                observation = Observation(
                    state="skipped",
                    reason="not_selected",
                    category="verification_infrastructure",
                )
            elif (
                check_id not in {"manifest_contract", "checkout"}
                and boundary.state != "passed"
            ):
                observation = boundary
            else:
                observation = action()
            checks.append(
                ReadinessCheck(
                    **observation.model_dump(),
                    target_id=target.id,
                    check_id=check_id,
                    tested_revision=target.revision,
                    timestamp=datetime.now(UTC),
                    duration_seconds=monotonic() - start,
                )
            )
        if root is not None and boundary.state == "passed":
            final = _checkout(target, root)
            checks.append(
                ReadinessCheck(
                    **final.model_dump(),
                    target_id=target.id,
                    check_id="checkout_final",
                    tested_revision=target.revision,
                    timestamp=datetime.now(UTC),
                    duration_seconds=0.0,
                )
            )
    observations = tuple(checks)
    superseded = any(
        check.check_id == "checkout_final" and check.state != "passed"
        for check in observations
    )
    return ReadinessReport(
        manifest_sha256=manifest_hash(manifest),
        timestamp=datetime.now(UTC),
        checks=observations,
        target_revisions=revisions,
        readiness="UNKNOWN" if superseded else readiness_state(observations),
        currency="superseded" if superseded else "current",
    )
