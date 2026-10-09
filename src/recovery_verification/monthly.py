"""Monthly drill tier extends shared daily evidence and falls back conservatively."""

from datetime import UTC, datetime
from pathlib import Path
from time import monotonic

from pydantic import model_validator

from recovery_verification.contract import Manifest, Target
from recovery_verification.invocation import InvocationError, invoke_drill
from recovery_verification.models import (
    ConsumerDrillResult,
    ConsumerVerificationResult,
    State,
)
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
from recovery_verification.weekly import extend_weekly


class MonthlyReport(ReadinessReport):
    """Full drill success remains distinct from weekly fallback success."""

    drill_evidence: dict[str, ConsumerDrillResult]
    fallback_evidence: dict[str, ConsumerVerificationResult]

    @model_validator(mode="after")
    def bound_evidence(self) -> MonthlyReport:
        for prefix, reason, evidence in (
            ("monthly_", "consumer_isolated_drill", self.drill_evidence),
            ("weekly_", "consumer_executable_verification", self.fallback_evidence),
        ):
            observed = {
                f"{check.target_id}/{check.check_id.removeprefix(prefix)}": check.state
                for check in self.checks
                if check.reason == reason
            }
            if observed != {
                key: value.readiness_state for key, value in evidence.items()
            }:
                raise ValueError("monthly evidence does not match observations")
        return self


def run_monthly(
    manifest: Manifest,
    checkouts: dict[str, Path],
    probe: Probe,
    *,
    target_ids: tuple[str, ...] = (),
) -> MonthlyReport:
    daily = run_readiness(manifest, checkouts, probe, target_ids=target_ids)
    return extend_monthly(manifest, checkouts, daily, target_ids=target_ids)


def extend_monthly(
    manifest: Manifest,
    checkouts: dict[str, Path],
    daily: ReadinessReport,
    *,
    target_ids: tuple[str, ...] = (),
) -> MonthlyReport:
    selected = target_ids or tuple(target.id for target in manifest.targets)
    if len(set(selected)) != len(selected) or set(selected) - {
        target.id for target in manifest.targets
    }:
        raise ValueError("invalid target selection")
    targets = tuple(target for target in manifest.targets if target.id in selected)
    if (
        daily.manifest_sha256 != manifest_hash(manifest)
        or daily.target_revisions != {target.id: target.revision for target in targets}
        or daily.currency != "current"
        or assess_currency(
            daily, manifest, now=datetime.now(UTC), max_age_seconds=3600
        ).currency
        != "current"
    ):
        raise ValueError("daily evidence does not match monthly selection")
    checks = list(daily.checks)
    drills: dict[str, ConsumerDrillResult] = {}
    fallback: dict[str, ConsumerVerificationResult] = {}

    for target in targets:

        def add(
            check_id: str,
            state: State,
            reason: str,
            duration: float = 0.0,
            *,
            observed_target: Target = target,
        ) -> None:
            checks.append(
                ReadinessCheck(
                    target_id=observed_target.id,
                    check_id=check_id,
                    tested_revision=observed_target.revision,
                    timestamp=datetime.now(UTC),
                    duration_seconds=duration,
                    state=state,
                    reason=reason,
                    category="recovery_prerequisite"
                    if state == "failed"
                    else "none"
                    if state == "passed"
                    else "verification_infrastructure",
                )
            )

        root = checkouts.get(target.id)
        if not target.ephemeral_rebuild.supported:
            if "verification" in target.allowed_verification_levels:
                subset_checks = tuple(
                    c for c in daily.checks if c.target_id == target.id
                )
                subset = ReadinessReport(
                    manifest_sha256=daily.manifest_sha256,
                    timestamp=daily.timestamp,
                    target_revisions={target.id: target.revision},
                    checks=subset_checks,
                    readiness=readiness_state(subset_checks),
                )
                weekly = extend_weekly(
                    manifest, checkouts, subset, target_ids=(target.id,)
                )
                checks.extend(weekly.checks[len(subset_checks) :])
                fallback.update(weekly.consumer_evidence)
            add("monthly_capability", "unavailable", target.ephemeral_rebuild.reason)
            continue
        for command in target.validation_commands:
            if "drill" not in command.levels:
                continue
            start = monotonic()
            try:
                if root is None:
                    raise InvocationError("checkout_not_configured")
                evidence = invoke_drill(target, root, command.id, trusted=True)
            except InvocationError as error:
                add(
                    f"monthly_{command.id}",
                    "unavailable",
                    str(error),
                    monotonic() - start,
                )
                add(
                    f"monthly_cleanup_{command.id}",
                    "unavailable",
                    "cleanup_not_observed",
                )
            else:
                drills[f"{target.id}/{command.id}"] = evidence
                add(
                    f"monthly_{command.id}",
                    evidence.readiness_state,
                    "consumer_isolated_drill",
                    monotonic() - start,
                )
                for index, snapshot in enumerate(evidence.snapshots):
                    source = next(
                        item
                        for item in target.backup_sources
                        if item.id == snapshot.source_id
                    )
                    fresh = (
                        evidence.started_at - snapshot.timestamp
                    ).total_seconds() <= source.max_age_seconds
                    add(
                        f"monthly_backup_{command.id}_{index}",
                        "passed" if fresh else "failed",
                        "restored_backup_current" if fresh else "restored_backup_stale",
                    )
        if root is not None:
            boundary = _checkout(target, root)
            add("monthly_checkout_final", boundary.state, boundary.reason)
    observations = tuple(checks)
    superseded = any(
        check.check_id in {"monthly_checkout_final", "weekly_checkout_final"}
        and check.state != "passed"
        for check in observations
    )
    return MonthlyReport(
        manifest_sha256=daily.manifest_sha256,
        timestamp=datetime.now(UTC),
        target_revisions=daily.target_revisions,
        checks=observations,
        readiness="UNKNOWN" if superseded else readiness_state(observations),
        currency="superseded" if superseded else "current",
        drill_evidence=drills,
        fallback_evidence=fallback,
    )
