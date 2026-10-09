"""Consumer SDK for ordered recovery in an approved disposable Linux provider.

Providers own provisioning and execution inside the sandbox. The coordinator never
accepts a host, inventory, production mode, or credentials. Register cleanup against
the supplied resource ID before provisioning, so partially created resources can be
destroyed. Cleanup must destroy hosts, containers, volumes, credentials and state.
"""

from datetime import UTC, datetime
from time import monotonic
from typing import Protocol
from uuid import uuid4

from recovery_verification.contract import ContractModel, Target
from recovery_verification.models import (
    DRILL_PHASES,
    ConsumerDrillResult,
    DrillCheck,
    DrillPhase,
    SnapshotEvidence,
    normalize_readiness,
)


class PhaseResult(ContractModel):
    """No raw output or credential values cross the provider boundary."""

    check: DrillCheck
    snapshots: tuple[SnapshotEvidence, ...] = ()


class LinuxProvider(Protocol):
    """Consumer-owned plugin; execution semantics are reviewed with its revision.

    provision obtains a blank isolated Linux environment. bootstrap executes
    target.entrypoints.bootstrap; dependencies retrieves declared repositories and
    artifacts; secrets uses their standard runtime mechanism; recovery executes
    target.entrypoints.recovery; restore loads representative state and returns
    backup timestamps/hashes; workloads starts services; validation runs declared
    deterministic drill commands. All mutable operations stay inside the environment.
    """

    name: str

    def execute(
        self, phase: DrillPhase, target: Target, resource_reference: str
    ) -> PhaseResult: ...


def execute_drill(target: Target, provider: LinuxProvider) -> ConsumerDrillResult:
    """Stop on first failure, always attempt independent cleanup, never retry."""
    approval = target.ephemeral_rebuild
    if not approval.supported or approval.provider != provider.name:
        raise ValueError("approved disposable provider required")
    resource = f"temporary://recovery-{uuid4().hex}"
    started_at = datetime.now(UTC)
    start = monotonic()
    checks: list[DrillCheck] = []
    snapshots: tuple[SnapshotEvidence, ...] = ()
    blocked = False

    def execute(phase: DrillPhase) -> PhaseResult:
        try:
            result = provider.execute(phase, target, resource)
            # Validate even Python plugin results, rather than trusting model_copy.
            result = PhaseResult.model_validate_json(result.model_dump_json())
            if (
                result.check.phase != phase
                or result.check.state == "not_applicable"
                or (result.snapshots and phase != "restore")
            ):
                raise ValueError("invalid provider phase")
            if len({item.source_id for item in result.snapshots}) != len(
                result.snapshots
            ) or not {item.source_id for item in result.snapshots} <= {
                item.id for item in target.backup_sources
            }:
                raise ValueError("invalid snapshot identities")
            if (
                phase == "restore"
                and result.check.state == "passed"
                and (
                    {item.source_id for item in result.snapshots}
                    != {item.id for item in target.backup_sources}
                    or any(
                        item.timestamp.tzinfo is None or item.timestamp > started_at
                        for item in result.snapshots
                    )
                )
            ):
                raise ValueError("invalid snapshot evidence")
        # Sanitize plugin exceptions and preserve the independent cleanup attempt.
        except Exception:
            return PhaseResult(
                check=DrillCheck(
                    id=phase,
                    phase=phase,
                    state="failed",
                    reason="provider_phase_failed",
                )
            )
        return result.model_copy(
            update={"check": result.check.model_copy(update={"id": phase})}
        )

    try:
        for phase in DRILL_PHASES[:-1]:
            if blocked:
                result = PhaseResult(
                    check=DrillCheck(
                        id=phase,
                        phase=phase,
                        state="skipped",
                        reason="prior_phase_failed",
                    )
                )
            else:
                result = execute(phase)
            checks.append(result.check)
            if phase == "restore":
                snapshots = result.snapshots
            blocked = blocked or result.check.state != "passed"
    finally:
        checks.append(execute("cleanup").check)
    return ConsumerDrillResult(
        provider=provider.name,
        resource_reference=resource,
        started_at=started_at,
        ended_at=datetime.now(UTC),
        duration_seconds=monotonic() - start,
        snapshots=snapshots,
        readiness_state=normalize_readiness(tuple(item.state for item in checks)),
        checks=tuple(checks),
    )
