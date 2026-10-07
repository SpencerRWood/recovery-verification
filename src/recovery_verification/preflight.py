"""Pure deterministic orchestration: no command execution or external systems."""

from hashlib import sha256

from recovery_verification.contract import Manifest
from recovery_verification.models import Check, Evidence, PreflightResult


def preflight(manifest: Manifest, target_ids: tuple[str, ...] = ()) -> PreflightResult:
    """Validate all declarations before selecting a target subset for evidence."""
    known = {target.id for target in manifest.targets}
    if len(target_ids) != len(set(target_ids)) or not set(target_ids) <= known:
        raise ValueError("invalid target selection")
    digest = sha256(manifest.model_dump_json().encode()).hexdigest()
    checks = tuple(
        Check(
            target_id=target.id,
            check_id="manifest_contract",
            state="passed",
            reason="declarations_valid_execution_not_performed",
            evidence=Evidence(
                manifest_sha256=digest,
                tested_revision=target.revision,
                method="contract-schema-v1",
            ),
        )
        for target in manifest.targets
        if not target_ids or target.id in target_ids
    )
    return PreflightResult(checks=checks)
