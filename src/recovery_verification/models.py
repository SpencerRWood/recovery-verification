"""Reusable check/evidence models and conservative normalized readiness."""

from datetime import datetime
from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from recovery_verification.contract import ContractModel, Identifier, Reference

State = Literal["passed", "failed", "unavailable", "skipped", "not_applicable"]


class Evidence(ContractModel):
    """Content identity and method, with references rather than arbitrary output."""

    manifest_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    tested_revision: Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
    method: Literal["contract-schema-v1"]


class Check(ContractModel):
    target_id: Identifier
    check_id: Identifier
    state: State
    reason: Reference
    evidence: Evidence


def normalize_readiness(states: tuple[State, ...]) -> State:
    """Only affirmative applicable results can pass; missing checks never pass."""
    if not states:
        return "unavailable"
    for state in ("failed", "unavailable", "skipped"):
        if state in states:
            return state
    return "passed" if "passed" in states else "not_applicable"


class PreflightResult(ContractModel):
    """Contract validity and unproven recovery readiness are separate facts."""

    schema_version: Literal["1"] = "1"
    mode: Literal["preflight"] = "preflight"
    contract_state: Literal["passed"] = "passed"
    readiness_state: Literal["unavailable"] = "unavailable"
    checks: tuple[Check, ...]


class ConsumerCheck(ContractModel):
    """The only consumer output fields accepted by generic readiness invocation."""

    id: Identifier
    state: State
    reason: Identifier


class ConsumerResult(ContractModel):
    """No arbitrary logs, secrets, service configuration, or ambiguous success."""

    schema_version: Literal["1"]
    mode: Literal["preflight"]
    readiness_state: State
    checks: Annotated[tuple[ConsumerCheck, ...], Field(min_length=1, max_length=128)]

    @model_validator(mode="after")
    def consistent_state(self) -> Self:
        if len({check.id for check in self.checks}) != len(self.checks):
            raise ValueError("duplicate check")
        if (
            normalize_readiness(tuple(check.state for check in self.checks))
            != self.readiness_state
        ):
            raise ValueError("inconsistent readiness")
        return self


class VerificationCheck(ConsumerCheck):
    phase: Literal[
        "automation_check",
        "automation_idempotence",
        "restore",
        "schema",
        "integrity",
        "synthetic",
        "cleanup",
        "classification",
    ]
    artifact_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")] | None = None
    resource_reference: (
        Annotated[str, Field(pattern=r"^temporary://[A-Za-z0-9_-]{1,128}$")] | None
    ) = None


class ConsumerVerificationResult(ContractModel):
    """Executable weekly evidence; cleanup is an independent required result."""

    schema_version: Literal["1"]
    mode: Literal["verification"]
    readiness_state: State
    checks: Annotated[
        tuple[VerificationCheck, ...], Field(min_length=1, max_length=128)
    ]

    @model_validator(mode="after")
    def consistent_state(self) -> Self:
        required = {
            "automation_check",
            "automation_idempotence",
            "restore",
            "schema",
            "integrity",
            "synthetic",
            "cleanup",
        }
        if (
            len({check.id for check in self.checks}) != len(self.checks)
            or not required <= {check.phase for check in self.checks}
            or normalize_readiness(tuple(check.state for check in self.checks))
            != self.readiness_state
            or any(
                check.phase == "restore"
                and check.state == "passed"
                and check.artifact_sha256 is None
                for check in self.checks
            )
            or any(
                check.phase in required and check.state == "not_applicable"
                for check in self.checks
            )
        ):
            raise ValueError("invalid verification evidence")
        return self


DrillPhase = Literal[
    "provision",
    "bootstrap",
    "dependencies",
    "secrets",
    "recovery",
    "restore",
    "workloads",
    "validation",
    "cleanup",
]
DRILL_PHASES: tuple[DrillPhase, ...] = (
    "provision",
    "bootstrap",
    "dependencies",
    "secrets",
    "recovery",
    "restore",
    "workloads",
    "validation",
    "cleanup",
)


class DrillCheck(ConsumerCheck):
    phase: DrillPhase


class SnapshotEvidence(ContractModel):
    source_id: Identifier
    timestamp: datetime
    artifact_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class ConsumerDrillResult(ContractModel):
    """Complete phase evidence, including independently observed destruction."""

    schema_version: Literal["1"] = "1"
    mode: Literal["drill"] = "drill"
    provider: Identifier
    environment: Literal["disposable-linux"] = "disposable-linux"
    resource_reference: Annotated[
        str, Field(pattern=r"^temporary://[A-Za-z0-9_-]{1,128}$")
    ]
    started_at: datetime
    ended_at: datetime
    duration_seconds: Annotated[float, Field(ge=0, allow_inf_nan=False)]
    snapshots: Annotated[tuple[SnapshotEvidence, ...], Field(max_length=128)]
    readiness_state: State
    checks: tuple[DrillCheck, ...]

    @model_validator(mode="after")
    def complete_evidence(self) -> Self:
        if (
            tuple(check.phase for check in self.checks) != DRILL_PHASES
            or len({check.id for check in self.checks}) != len(self.checks)
            or normalize_readiness(tuple(check.state for check in self.checks))
            != self.readiness_state
            or any(check.state == "not_applicable" for check in self.checks)
            or self.started_at.tzinfo is None
            or self.ended_at.tzinfo is None
            or self.ended_at < self.started_at
            or abs(
                (self.ended_at - self.started_at).total_seconds()
                - self.duration_seconds
            )
            > 5
            or len({item.source_id for item in self.snapshots}) != len(self.snapshots)
            or any(
                item.timestamp.tzinfo is None or item.timestamp > self.started_at
                for item in self.snapshots
            )
        ):
            raise ValueError("invalid drill evidence")
        blocked = False
        for check in self.checks[:-1]:
            if blocked and check.state != "skipped":
                raise ValueError("drill continued after failure")
            blocked = blocked or check.state != "passed"
        return self
