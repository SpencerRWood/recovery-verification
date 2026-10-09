"""Weekly orchestration reuses daily prerequisites and consumer-owned recovery."""

from datetime import UTC, datetime
from pathlib import Path
from time import monotonic

from pydantic import model_validator

from recovery_verification.contract import Manifest
from recovery_verification.invocation import InvocationError, invoke_verification
from recovery_verification.models import ConsumerVerificationResult
from recovery_verification.readiness import (
    Probe,
    ReadinessCheck,
    ReadinessReport,
    _checkout,
    assess_currency,
    manifest_hash,
    readiness_state,
    run_readiness,
)


class WeeklyReport(ReadinessReport):
    """Daily evidence plus sanitized, phase-specific executable/cleanup evidence."""

    consumer_evidence: dict[str, ConsumerVerificationResult]

    @model_validator(mode="after")
    def bound_consumer_evidence(self) -> WeeklyReport:
        expected = {
            f"{check.target_id}/{check.check_id.removeprefix('weekly_')}": check.state
            for check in self.checks
            if check.reason == "consumer_executable_verification"
        }
        if expected != {
            key: value.readiness_state for key, value in self.consumer_evidence.items()
        }:
            raise ValueError("consumer evidence does not match observations")
        return self


def run_weekly(
    manifest: Manifest,
    checkouts: dict[str, Path],
    probe: Probe,
    *,
    target_ids: tuple[str, ...] = (),
) -> WeeklyReport:
    daily = run_readiness(manifest, checkouts, probe, target_ids=target_ids)
    return extend_weekly(manifest, checkouts, daily, target_ids=target_ids)


def extend_weekly(
    manifest: Manifest,
    checkouts: dict[str, Path],
    daily: ReadinessReport,
    *,
    target_ids: tuple[str, ...] = (),
) -> WeeklyReport:
    if len(set(target_ids)) != len(target_ids) or set(target_ids) - {
        target.id for target in manifest.targets
    }:
        raise ValueError("invalid target selection")
    targets = tuple(
        target
        for target in manifest.targets
        if not target_ids or target.id in target_ids
    )
    if any(
        "verification" not in target.allowed_verification_levels for target in targets
    ):
        raise ValueError("verification capability not declared")
    if (
        daily.manifest_sha256 != manifest_hash(manifest)
        or daily.target_revisions != {target.id: target.revision for target in targets}
        or daily.currency != "current"
        or assess_currency(
            daily, manifest, now=datetime.now(UTC), max_age_seconds=3600
        ).currency
        != "current"
    ):
        raise ValueError("daily evidence does not match weekly selection")
    checks = list(daily.checks)
    evidence: dict[str, ConsumerVerificationResult] = {}
    for target in targets:
        root = checkouts.get(target.id)
        for command in target.validation_commands:
            if "verification" not in command.levels:
                continue
            start = monotonic()
            try:
                if root is None:
                    raise InvocationError("checkout_not_configured")
                result = invoke_verification(target, root, command.id, trusted=True)
            except InvocationError as error:
                checks.append(
                    ReadinessCheck(
                        target_id=target.id,
                        check_id=f"weekly_{command.id}",
                        tested_revision=target.revision,
                        timestamp=datetime.now(UTC),
                        duration_seconds=monotonic() - start,
                        state="unavailable",
                        reason=str(error),
                        category="verification_infrastructure",
                    )
                )
                checks.append(
                    ReadinessCheck(
                        target_id=target.id,
                        check_id=f"weekly_cleanup_{command.id}",
                        tested_revision=target.revision,
                        timestamp=datetime.now(UTC),
                        duration_seconds=0.0,
                        state="unavailable",
                        reason="cleanup_not_observed",
                        category="verification_infrastructure",
                    )
                )
            else:
                evidence[f"{target.id}/{command.id}"] = result
                checks.append(
                    ReadinessCheck(
                        target_id=target.id,
                        check_id=f"weekly_{command.id}",
                        tested_revision=target.revision,
                        timestamp=datetime.now(UTC),
                        duration_seconds=monotonic() - start,
                        state=result.readiness_state,
                        reason="consumer_executable_verification",
                        category="recovery_prerequisite"
                        if result.readiness_state == "failed"
                        else "none"
                        if result.readiness_state == "passed"
                        else "verification_infrastructure",
                    )
                )
        if root is not None:
            final = _checkout(target, root)
            checks.append(
                ReadinessCheck(
                    **final.model_dump(),
                    target_id=target.id,
                    check_id="weekly_checkout_final",
                    tested_revision=target.revision,
                    timestamp=datetime.now(UTC),
                    duration_seconds=0.0,
                )
            )
    observations = tuple(checks)
    superseded = any(
        check.check_id == "weekly_checkout_final" and check.state != "passed"
        for check in observations
    )
    return WeeklyReport(
        manifest_sha256=daily.manifest_sha256,
        timestamp=datetime.now(UTC),
        target_revisions=daily.target_revisions,
        checks=observations,
        readiness="UNKNOWN" if superseded else readiness_state(observations),
        currency="superseded" if superseded else "current",
        consumer_evidence=evidence,
    )
