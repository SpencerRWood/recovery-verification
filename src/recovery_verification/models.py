"""Reusable check/evidence models and conservative normalized readiness."""

from typing import Annotated, Literal

from pydantic import Field

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
