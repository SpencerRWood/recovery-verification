"""Presentation preserves score semantics and every distinct actionable issue."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from recovery_verification.contract import parse_manifest
from recovery_verification.models import State
from recovery_verification.notification_presentation import (
    backup_health,
    findings,
    report_blocks,
    tier_result,
)
from recovery_verification.provenance import BackupProvenance
from recovery_verification.scorecard import CheckDiagnostic, Scorecard, build_scorecard
from tests.unit.test_scorecard import (  # noqa: F401 -- native evidence fixtures
    local,
    reports,
)


@pytest.fixture
def score(document: dict[str, Any], tmp_path: Path, local: None) -> Scorecard:  # noqa: ARG001, F811 -- native report fixture
    manifest = parse_manifest(json.dumps(document))
    return build_scorecard(
        manifest,
        reports(manifest, tmp_path),
        now=datetime.now(UTC) + timedelta(seconds=1),
    )


@pytest.mark.parametrize("state", ["READY", "DEGRADED", "NOT_READY", "UNKNOWN"])
@pytest.mark.parametrize("status", ["SUCCESS", "FAILURE", "CANCELED"])
def test_execution_and_readiness_never_conflated(
    score: Scorecard, state: str, status: str
) -> None:
    current = score.model_copy(update={"readiness": state})
    blocks, _ = report_blocks(
        current, status, "recovery_readiness_daily", datetime.now(UTC), historical=True
    )
    text = "\n".join(b["text"] for b in blocks)
    assert f"Recovery readiness: {state}" in text
    assert f"Dagster execution: {status}" in text
    assert "Verification: Daily" in text
    assert "Historical evidence" in text
    assert "Weekly verification:" in text


def with_diagnostics(
    score: Scorecard, diagnostics: tuple[CheckDiagnostic, ...]
) -> Scorecard:
    target = score.targets[0]
    tier = target.tiers[0]
    assert tier.latest_attempt
    changed = tier.model_copy(
        update={
            "status": "failed",
            "reasons": tuple(d.check_id + ":" + d.reason for d in diagnostics),
            "latest_attempt": tier.latest_attempt.model_copy(
                update={"diagnostics": diagnostics}
            ),
        }
    )
    return score.model_copy(
        update={
            "targets": (
                target.model_copy(
                    update={
                        "tiers": (changed, *target.tiers[1:]),
                        "reasons": changed.reasons,
                    }
                ),
            )
        }
    )


def diagnostic(check: str, reason: str, state: State = "failed") -> CheckDiagnostic:
    return CheckDiagnostic(
        check_id=check,
        state=state,
        reason=reason,
        failure_category="recovery_prerequisite",
    )


def test_every_distinct_diagnostic_preserved_unknown_and_informational(
    score: Scorecard,
) -> None:
    diagnostics = (
        diagnostic("backup_database_cutover", "backup_stale"),
        diagnostic(
            "backup_migration_snapshot", "backup_artifact_unproven", "unavailable"
        ),
        diagnostic("dependency_ci_ghcr", "secret_probe_unconfigured", "unavailable"),
        diagnostic("dependency_images", "artifact_catalog_checked", "passed"),
        *(diagnostic(f"unknown_{i}", "unknown_diagnostic") for i in range(60)),
    )
    items = findings(with_diagnostics(score, diagnostics))
    references = [d for f in items for d in f.diagnostics]
    assert len(items) == 63
    assert {d["check"] for d in references} == {
        d.check_id for d in diagnostics if d.state != "passed"
    }
    assert all(f.environments and f.action for f in items)
    assert len({f.category for f in items}) == 4
    assert "unknown_diagnostic" not in "\n".join(f.text() for f in items)


def test_shared_issue_merges_environments_distinct_sources_remain(
    score: Scorecard,
) -> None:
    changed = with_diagnostics(
        score,
        (
            diagnostic("backup_one", "storage_missing"),
            diagnostic("backup_two", "storage_missing"),
        ),
    )
    target = changed.targets[0]
    other = target.model_copy(
        update={"target_id": "second", "owning_repository": "example/second"}
    )
    items = findings(changed.model_copy(update={"targets": (target, other)}))
    assert len(items) == 2
    assert all(len(f.environments) == 2 for f in items)
    assert all(len(f.diagnostics) == 2 for f in items)


def test_zero_findings_and_severity_order(score: Scorecard) -> None:
    assert findings(score) == []
    blocks, _ = report_blocks(
        score, "SUCCESS", "other", datetime.now(UTC), historical=False
    )
    assert any(b["text"] == "No outstanding findings." for b in blocks)
    items = findings(
        with_diagnostics(
            score,
            (
                diagnostic("z", "unrecognized", "unavailable"),
                diagnostic("a", "unrecognized"),
            ),
        )
    )
    assert [f.severity for f in items] == [0, 1]


def test_fixture_and_custody_never_attest_production_recovery(score: Scorecard) -> None:
    weekly = score.targets[0].tiers[1]
    assert weekly.latest_attempt
    attempt = weekly.latest_attempt
    fixture = attempt.provenance.model_copy(
        update={
            "backups": (
                BackupProvenance(source_id="sample", kind="representative_fixture"),
            )
        }
    )
    weekly = weekly.model_copy(
        update={"latest_attempt": attempt.model_copy(update={"provenance": fixture})}
    )
    target = score.targets[0].model_copy(
        update={
            "owning_repository": "SpencerRWood/homelab",
            "tiers": (score.targets[0].tiers[0], weekly, score.targets[0].tiers[2]),
        }
    )
    current = score.model_copy(update={"targets": (target,)})
    assert any("OP-543" in f.action for f in findings(current))
    assert any("fixture" in tier_result(t).lower() for t in target.tiers)
    assert backup_health(target)[2] == "Unavailable"
    blocks, _ = report_blocks(
        current, "SUCCESS", "other", datetime.now(UTC), historical=False
    )
    assert any("Full recovery: Unverified" in b["text"] for b in blocks)


def test_stale_backup_is_not_failed_execution(score: Scorecard) -> None:
    target = score.targets[0]
    daily = target.tiers[0]
    assert daily.latest_attempt
    check = daily.latest_attempt.checks[0].model_copy(
        update={
            "check_id": "backup_database_cutover",
            "state": "failed",
            "reason": "backup_stale",
        }
    )
    stale = daily.model_copy(
        update={
            "latest_attempt": daily.latest_attempt.model_copy(
                update={"checks": (check,)}
            )
        }
    )
    outcome, freshness, integrity = backup_health(
        target.model_copy(update={"tiers": (stale, *target.tiers[1:])})
    )
    assert outcome.startswith("Execution unverified")
    assert freshness == "Stale"
    assert integrity == "Unavailable"


@pytest.mark.parametrize("state", ["passed", "failed", "unavailable"])
def test_integrity_binds_nested_real_artifacts_and_conflicting_readiness(
    score: Scorecard,
    state: str,
) -> None:
    target = score.targets[0]
    daily, weekly = target.tiers[:2]
    assert daily.latest_attempt
    assert weekly.latest_attempt
    backups = (
        BackupProvenance(
            source_id="database", artifact_sha256="a" * 64, timestamp=datetime.now(UTC)
        ),
    )
    provenance = daily.latest_attempt.provenance.model_copy(update={"backups": backups})
    check = daily.latest_attempt.checks[0].model_copy(
        update={
            "check_id": "backup_database_cutover",
            "state": "passed",
            "reason": "backup_fresh",
        }
    )
    daily = daily.model_copy(
        update={
            "status": "failed",
            "latest_attempt": daily.latest_attempt.model_copy(
                update={"checks": (check,), "provenance": provenance}
            ),
        }
    )
    weekly = weekly.model_copy(
        update={
            "latest_attempt": weekly.latest_attempt.model_copy(
                update={
                    "provenance": provenance,
                    "consumer_evidence": {
                        "target/weekly": {
                            "checks": [{"phase": "integrity", "state": state}]
                        }
                    },
                }
            )
        }
    )
    outcome, freshness, integrity = backup_health(
        target.model_copy(
            update={"readiness": "NOT_READY", "tiers": (daily, weekly, target.tiers[2])}
        )
    )
    assert outcome == "Execution unverified · evidence available"
    assert freshness == "Current"
    assert (
        integrity
        == {"passed": "Verified", "failed": "Failed", "unavailable": "Unavailable"}[
            state
        ]
    )
