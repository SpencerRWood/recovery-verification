"""Reusable check/evidence models and conservative normalized readiness."""

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
