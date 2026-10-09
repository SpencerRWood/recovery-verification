"""Monthly lifecycle, provider approval, fallback and evidence acceptance."""

import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from recovery_verification import monthly, readiness, weekly
from recovery_verification.cli import main
from recovery_verification.contract import (
    ContractError,
    Manifest,
    Target,
    parse_manifest,
)
from recovery_verification.drill import PhaseResult, execute_drill
from recovery_verification.invocation import InvocationError, invoke_drill
from recovery_verification.models import (
    DRILL_PHASES,
    ConsumerDrillResult,
    DrillCheck,
    DrillPhase,
    SnapshotEvidence,
)
from recovery_verification.probes import BoundedProbe, result
from tests.unit.test_invocation import repository
from tests.unit.test_weekly import consumer


class Provider:
    """Deterministic provider fixture; never evidence of an actual Linux rebuild."""

    name = "fixture-linux"

    def __init__(self, failure: DrillPhase | None = None) -> None:
        self.failure = failure
        self.calls: list[tuple[DrillPhase, str]] = []

    def execute(self, phase: DrillPhase, target: Target, resource: str) -> PhaseResult:
        self.calls.append((phase, resource))
        if phase == self.failure:
            raise RuntimeError("credential-do-not-echo")
        return PhaseResult(
            check=DrillCheck(
                id=phase, phase=phase, state="passed", reason="fixture_ok"
            ),
            snapshots=tuple(
                SnapshotEvidence(
                    source_id=source.id,
                    timestamp=datetime.now(UTC) - timedelta(hours=1),
                    artifact_sha256="a" * 64,
                )
                for source in target.backup_sources
            )
            if phase == "restore"
            else (),
        )


@pytest.fixture
def manifest(document: dict[str, Any]) -> Manifest:
    return parse_manifest(json.dumps(document))


@pytest.fixture
def local(monkeypatch: pytest.MonkeyPatch) -> None:
    for module, names in (
        (readiness, ("_checkout", "_interfaces", "_consumer")),
        (weekly, ("_checkout",)),
        (monthly, ("_checkout",)),
    ):
        for name in names:
            monkeypatch.setattr(module, name, lambda *_: result("passed", "fixture_ok"))


@pytest.mark.parametrize("failure", [None, *DRILL_PHASES])
def test_ordered_rebuild_and_failure_cleanup(
    manifest: Manifest, failure: DrillPhase | None
) -> None:
    provider = Provider(failure)
    observed = execute_drill(manifest.targets[0], provider)
    assert observed.readiness_state == ("passed" if failure is None else "failed")
    assert provider.calls[-1][0] == "cleanup"
    assert len({resource for _, resource in provider.calls}) == 1
    assert observed.duration_seconds >= 0
    assert observed.started_at <= observed.ended_at
    if failure and failure != "cleanup":
        assert [phase for phase, _ in provider.calls] == [
            *DRILL_PHASES[: DRILL_PHASES.index(failure) + 1],
            "cleanup",
        ]
    assert "credential-do-not-echo" not in observed.model_dump_json()
    assert (
        ConsumerDrillResult.model_validate_json(observed.model_dump_json()) == observed
    )


def test_provider_approval_and_partial_provision_cleanup(manifest: Manifest) -> None:
    provider = Provider()
    provider.name = "unapproved"
    with pytest.raises(ValueError, match="approved"):
        execute_drill(manifest.targets[0], provider)
    assert not provider.calls


@pytest.mark.parametrize("problem", ["missing", "production", "unapproved", "false"])
def test_ephemeral_contract_fails_closed(
    document: dict[str, Any], problem: str
) -> None:
    approval = document["targets"][0]["ephemeral_rebuild"]
    if problem == "missing":
        del document["targets"][0]["ephemeral_rebuild"]
    elif problem == "production":
        approval["environment"] = "production"
    elif problem == "unapproved":
        approval["provider"] = None
    else:
        approval["supported"] = False
    with pytest.raises(ContractError):
        parse_manifest(json.dumps(document))


@pytest.mark.parametrize(
    "problem",
    ["cleanup", "order", "duration", "future", "timezone", "duplicate", "extra"],
)
def test_drill_evidence_guards(manifest: Manifest, problem: str) -> None:
    value = execute_drill(manifest.targets[0], Provider()).model_dump(mode="json")
    if problem == "cleanup":
        value["checks"].pop()
    elif problem == "order":
        value["checks"][0]["state"] = "failed"
        value["readiness_state"] = "failed"
    elif problem == "duration":
        value["duration_seconds"] = 999
    elif problem == "future":
        value["snapshots"][0]["timestamp"] = (
            datetime.now(UTC) + timedelta(days=1)
        ).isoformat()
    elif problem == "timezone":
        value["started_at"] = "2026-10-09T00:00:00"
    elif problem == "duplicate":
        value["snapshots"] *= 2
    else:
        value["credentials"] = "do-not-echo"
    with pytest.raises(ValidationError):
        ConsumerDrillResult.model_validate_json(json.dumps(value))


@pytest.mark.usefixtures("local")
@pytest.mark.parametrize(
    "failure",
    [None, "bootstrap", "dependencies", "secrets", "restore", "validation", "cleanup"],
)
def test_monthly_reuses_daily_and_reports_drill(
    manifest: Manifest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: DrillPhase | None,
) -> None:
    observed = execute_drill(manifest.targets[0], Provider(failure))
    monkeypatch.setattr(monthly, "invoke_drill", lambda *_a, **_k: observed)
    report = monthly.run_monthly(
        manifest, {"example-linux": tmp_path}, lambda *_: result("passed", "fixture_ok")
    )
    assert report.readiness == ("READY" if failure is None else "NOT_READY")
    assert report.drill_evidence["example-linux/synthetic"] == observed
    assert monthly.MonthlyReport.model_validate_json(report.model_dump_json()) == report


def unsupported(document: dict[str, Any], *, verification: bool = True) -> Manifest:
    target = document["targets"][0]
    target["ephemeral_rebuild"] = {
        "supported": False,
        "provider": None,
        "environment": None,
        "reason": "isolated_bootstrap_not_supported",
    }
    levels = ["readiness", "verification"] if verification else ["readiness"]
    target["allowed_verification_levels"] = levels
    target["validation_commands"][0]["levels"] = levels
    target["cadence_overrides"] = []
    return parse_manifest(json.dumps(document))


@pytest.mark.usefixtures("local")
@pytest.mark.parametrize("verification", [True, False])
def test_unsupported_runs_strongest_level_without_claiming_drill(
    document: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    verification: bool,
) -> None:
    manifest = unsupported(document, verification=verification)
    monkeypatch.setattr(weekly, "invoke_verification", lambda *_a, **_k: consumer())
    report = monthly.run_monthly(
        manifest, {"example-linux": tmp_path}, lambda *_: result("passed", "fixture_ok")
    )
    assert report.readiness == "UNKNOWN"
    assert not report.drill_evidence
    assert bool(report.fallback_evidence) == verification
    assert any(c.reason == "isolated_bootstrap_not_supported" for c in report.checks)


@pytest.mark.usefixtures("local")
def test_monthly_timeout_selection_currency_and_missing_checkout(
    manifest: Manifest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail(*_a: object, **_k: object) -> ConsumerDrillResult:
        raise InvocationError("command_timeout")

    monkeypatch.setattr(monthly, "invoke_drill", fail)
    report = monthly.run_monthly(
        manifest, {"example-linux": tmp_path}, lambda *_: result("passed", "fixture_ok")
    )
    assert report.readiness == "UNKNOWN"
    assert {c.reason for c in report.checks} >= {
        "command_timeout",
        "cleanup_not_observed",
    }
    missing = monthly.run_monthly(
        manifest, {}, lambda *_: result("passed", "fixture_ok")
    )
    assert missing.readiness == "UNKNOWN"
    daily = readiness.run_readiness(
        manifest, {"example-linux": tmp_path}, lambda *_: result("passed", "fixture_ok")
    )
    for selected in (("absent",), ("example-linux", "example-linux")):
        with pytest.raises(ValueError, match="invalid target selection"):
            monthly.extend_monthly(manifest, {}, daily, target_ids=selected)
    for changed in (
        daily.model_copy(update={"manifest_sha256": "0" * 64}),
        daily.model_copy(update={"target_revisions": {}}),
        daily.model_copy(update={"timestamp": daily.timestamp - timedelta(days=2)}),
    ):
        with pytest.raises(ValueError, match="daily evidence"):
            monthly.extend_monthly(manifest, {}, changed)


def test_real_drill_command_and_provider_binding(
    tmp_path: Path, document: dict[str, Any]
) -> None:
    observed = execute_drill(
        parse_manifest(json.dumps(document)).targets[0], Provider()
    )
    body = f"import json\nprint(json.dumps({observed.model_dump(mode='json')!r}))\n"
    target = repository(tmp_path, document, body)
    assert invoke_drill(target, tmp_path, "validate", trusted=True) == observed
    with pytest.raises(InvocationError, match="explicit_trust"):
        invoke_drill(target, tmp_path, "validate")
    changed = target.model_copy(
        update={
            "ephemeral_rebuild": target.ephemeral_rebuild.model_copy(
                update={"provider": "other"}
            )
        }
    )
    with pytest.raises(InvocationError, match="provider_approval_mismatch"):
        invoke_drill(changed, tmp_path, "validate", trusted=True)


@pytest.mark.usefixtures("local")
def test_monthly_cli_scope(
    document: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    manifest = unsupported(document, verification=False)
    source = tmp_path / "manifest.json"
    source.write_text(manifest.model_dump_json())
    monkeypatch.setattr(
        BoundedProbe, "__call__", lambda *_: result("passed", "fixture_ok")
    )
    args = [
        str(source),
        "--monthly",
        "--target",
        "example-linux",
        "--checkout",
        str(tmp_path),
    ]
    assert main(args) == 4
    assert json.loads(capsys.readouterr().out)["readiness"] == "UNKNOWN"
    for extra in (
        ["--weekly"],
        ["--daily"],
        ["--check", "checkout"],
        ["--invoke-check", "synthetic"],
    ):
        assert main([*args, *extra]) == 2
        capsys.readouterr()
    assert main([str(source), "--monthly"]) == 2


@pytest.mark.usefixtures("local")
def test_stale_backup_and_superseded_source_never_pass(
    manifest: Manifest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed = execute_drill(manifest.targets[0], Provider())
    old = observed.model_copy(
        update={
            "snapshots": tuple(
                item.model_copy(
                    update={"timestamp": observed.started_at - timedelta(days=2)}
                )
                for item in observed.snapshots
            )
        }
    )
    monkeypatch.setattr(monthly, "invoke_drill", lambda *_a, **_k: old)
    report = monthly.run_monthly(
        manifest, {"example-linux": tmp_path}, lambda *_: result("passed", "fixture_ok")
    )
    assert report.readiness == "NOT_READY"
    assert any(c.reason == "restored_backup_stale" for c in report.checks)
    value = report.model_dump(mode="json")
    value["drill_evidence"] = {}
    with pytest.raises(ValidationError, match="monthly evidence"):
        monthly.MonthlyReport.model_validate_json(json.dumps(value))
    monkeypatch.setattr(
        monthly, "_checkout", lambda *_: result("unavailable", "changed")
    )
    changed = monthly.run_monthly(
        manifest, {"example-linux": tmp_path}, lambda *_: result("passed", "fixture_ok")
    )
    assert changed.currency == "superseded"
    assert changed.readiness == "UNKNOWN"


@pytest.mark.usefixtures("local")
def test_multiple_targets_keep_separate_fallback_and_rebuild_evidence(
    document: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    supported = deepcopy(document["targets"][0])
    supported["id"] = "supported"
    unsupported(document)
    document["targets"].append(supported)
    manifest = parse_manifest(json.dumps(document))
    observed = execute_drill(manifest.targets[1], Provider())
    monkeypatch.setattr(monthly, "invoke_drill", lambda *_a, **_k: observed)
    monkeypatch.setattr(weekly, "invoke_verification", lambda *_a, **_k: consumer())
    report = monthly.run_monthly(
        manifest,
        {"example-linux": tmp_path, "supported": tmp_path},
        lambda *_: result("passed", "fixture_ok"),
    )
    assert report.readiness == "UNKNOWN"
    assert set(report.drill_evidence) == {"supported/synthetic"}
    assert set(report.fallback_evidence) == {"example-linux/synthetic"}


@pytest.mark.parametrize("problem", ["phase", "snapshot", "not_applicable"])
def test_malformed_provider_result_still_destroys_resources(
    manifest: Manifest,
    problem: str,
) -> None:
    class InvalidProvider(Provider):
        def execute(
            self, phase: DrillPhase, target: Target, resource: str
        ) -> PhaseResult:
            observed = super().execute(phase, target, resource)
            if phase == "restore":
                if problem == "phase":
                    return observed.model_copy(
                        update={
                            "check": observed.check.model_copy(
                                update={"phase": "bootstrap"}
                            )
                        }
                    )
                if problem == "snapshot":
                    return observed.model_copy(update={"snapshots": ()})
                return observed.model_copy(
                    update={
                        "check": observed.check.model_copy(
                            update={"state": "not_applicable"}
                        )
                    }
                )
            return observed

    provider = InvalidProvider()
    observed = execute_drill(manifest.targets[0], provider)
    assert observed.readiness_state == "failed"
    assert provider.calls[-1][0] == "cleanup"
    assert next(c for c in observed.checks if c.phase == "restore").state == "failed"


def test_unsupported_invocation_and_unknown_backup_evidence(
    tmp_path: Path,
    document: dict[str, Any],
) -> None:
    manifest = parse_manifest(json.dumps(document))
    observed = execute_drill(manifest.targets[0], Provider())
    value = observed.model_dump(mode="json")
    value["snapshots"][0]["source_id"] = "unknown"
    body = f"import json\nprint(json.dumps({value!r}))\n"
    target = repository(tmp_path, document, body)
    with pytest.raises(InvocationError, match="backup_evidence_mismatch"):
        invoke_drill(target, tmp_path, "validate", trusted=True)
    with pytest.raises(InvocationError, match="ephemeral_rebuild_unsupported"):
        invoke_drill(
            unsupported(document).targets[0], tmp_path, "validate", trusted=True
        )
