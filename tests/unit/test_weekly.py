"""Weekly reuse, conservative state, subprocess deadlines and evidence guards."""

import json
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from recovery_verification import readiness, weekly
from recovery_verification.cli import main
from recovery_verification.contract import Manifest, parse_manifest
from recovery_verification.invocation import InvocationError, invoke_verification
from recovery_verification.models import ConsumerVerificationResult
from recovery_verification.probes import BoundedProbe, result
from tests.unit.test_invocation import repository

PHASES = (
    "automation_check",
    "automation_idempotence",
    "restore",
    "schema",
    "integrity",
    "synthetic",
    "cleanup",
)


def consumer(
    state: str = "passed", phase: str = "restore"
) -> ConsumerVerificationResult:
    checks: list[dict[str, Any]] = [
        {
            "id": item,
            "phase": item,
            "state": state if item == phase else "passed",
            "reason": "fixture_result",
            **({"artifact_sha256": "a" * 64} if item == "restore" else {}),
        }
        for item in PHASES
    ]
    return ConsumerVerificationResult.model_validate_json(
        json.dumps(
            {
                "schema_version": "1",
                "mode": "verification",
                "readiness_state": state,
                "checks": checks,
            }
        )
    )


@pytest.fixture
def manifest(document: dict[str, Any]) -> Manifest:
    return parse_manifest(json.dumps(document))


@pytest.fixture
def local(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("_checkout", "_interfaces", "_consumer"):
        monkeypatch.setattr(readiness, name, lambda *_: result("passed", "local_valid"))
    monkeypatch.setattr(weekly, "_checkout", lambda *_: result("passed", "local_valid"))


@pytest.mark.usefixtures("local")
@pytest.mark.parametrize(
    ("state", "phase", "expected"),
    [
        ("passed", "restore", "READY"),
        ("failed", "restore", "NOT_READY"),
        ("failed", "integrity", "NOT_READY"),
        ("failed", "cleanup", "NOT_READY"),
        ("unavailable", "restore", "UNKNOWN"),
        ("skipped", "synthetic", "UNKNOWN"),
    ],
)
def test_weekly_reuses_daily_and_retains_phase_evidence(  # noqa: PLR0913, PLR0917
    manifest: Manifest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    state: str,
    phase: str,
    expected: str,
) -> None:
    observed = consumer(state, phase)
    monkeypatch.setattr(weekly, "invoke_verification", lambda *_a, **_k: observed)
    report = weekly.run_weekly(
        manifest,
        {"example-linux": tmp_path},
        lambda *_: result("passed", "probe_valid"),
    )
    assert report.readiness == expected
    assert any(check.check_id == "consumer_synthetic" for check in report.checks)
    assert report.consumer_evidence["example-linux/synthetic"] == observed
    assert weekly.WeeklyReport.model_validate_json(report.model_dump_json()) == report
    assert all(
        check.tested_revision == manifest.targets[0].revision for check in report.checks
    )


@pytest.mark.usefixtures("local")
def test_invocation_failure_and_unobserved_cleanup_are_separate(
    manifest: Manifest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*_a: object, **_k: object) -> ConsumerVerificationResult:
        raise InvocationError("command_timeout")

    monkeypatch.setattr(weekly, "invoke_verification", fail)
    report = weekly.run_weekly(
        manifest,
        {"example-linux": tmp_path},
        lambda *_: result("passed", "probe_valid"),
    )
    assert report.readiness == "UNKNOWN"
    assert not report.consumer_evidence
    assert {check.reason for check in report.checks} >= {
        "command_timeout",
        "cleanup_not_observed",
    }
    missing = weekly.run_weekly(
        manifest, {}, lambda *_: result("passed", "probe_valid")
    )
    assert missing.readiness == "UNKNOWN"


@pytest.mark.usefixtures("local")
def test_capability_selection_source_and_daily_evidence_guards(
    document: dict[str, Any],
    manifest: Manifest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    daily = readiness.run_readiness(
        manifest,
        {"example-linux": tmp_path},
        lambda *_: result("passed", "probe_valid"),
    )
    for changed in (
        daily.model_copy(update={"manifest_sha256": "0" * 64}),
        daily.model_copy(update={"target_revisions": {}}),
        daily.model_copy(update={"currency": "stale", "readiness": "UNKNOWN"}),
        daily.model_copy(update={"timestamp": daily.timestamp - timedelta(days=2)}),
    ):
        with pytest.raises(ValueError, match="daily evidence"):
            weekly.extend_weekly(manifest, {"example-linux": tmp_path}, changed)
    for selected in (("absent",), ("example-linux", "example-linux")):
        with pytest.raises(ValueError, match="invalid target selection"):
            weekly.extend_weekly(manifest, {}, daily, target_ids=selected)
    target = document["targets"][0]
    target["allowed_verification_levels"] = ["readiness"]
    target["cadence_overrides"] = []
    target["validation_commands"][0]["levels"] = ["readiness"]
    with pytest.raises(ValueError, match="verification capability"):
        weekly.extend_weekly(parse_manifest(json.dumps(document)), {}, daily)
    monkeypatch.setattr(weekly, "invoke_verification", lambda *_a, **_k: consumer())
    monkeypatch.setattr(
        weekly, "_checkout", lambda *_: result("unavailable", "source_changed")
    )
    report = weekly.extend_weekly(manifest, {"example-linux": tmp_path}, daily)
    assert report.currency == "superseded"
    assert report.readiness == "UNKNOWN"


@pytest.mark.parametrize(
    "problem", ["cleanup", "hash", "duplicate", "state", "extra", "not_applicable"]
)
def test_wire_evidence_cannot_claim_false_success(problem: str) -> None:
    value = consumer().model_dump(mode="json")
    if problem == "cleanup":
        value["checks"] = value["checks"][:-1]
    elif problem == "hash":
        del value["checks"][2]["artifact_sha256"]
    elif problem == "duplicate":
        value["checks"].append(value["checks"][0])
    elif problem == "state":
        value["checks"][2]["state"] = "failed"
    elif problem == "extra":
        value["raw_output"] = "secret-do-not-echo"
    else:
        value["checks"][-1]["state"] = "not_applicable"
    with pytest.raises(ValidationError):
        ConsumerVerificationResult.model_validate_json(json.dumps(value))


def test_verification_adapter_real_output(
    tmp_path: Path,
    document: dict[str, Any],
) -> None:
    observed = consumer()
    body = f"import json\nprint(json.dumps({observed.model_dump(mode='json')!r}))\n"
    target = repository(tmp_path, document, body)
    assert invoke_verification(target, tmp_path, "validate", trusted=True) == observed
    with pytest.raises(InvocationError, match="explicit_trust"):
        invoke_verification(target, tmp_path, "validate")
    with pytest.raises(InvocationError, match="unsupported"):
        invoke_verification(target, tmp_path, "absent", trusted=True)


def test_verification_timeout_allows_consumer_cleanup(
    tmp_path: Path,
    document: dict[str, Any],
) -> None:
    marker = tmp_path / ".git/cleanup-marker"
    body = (
        "import signal, time\nfrom pathlib import Path\n"
        "def cleanup(*_):\n"
        f"    Path({str(marker)!r}).write_text('cleaned')\n"
        "    raise SystemExit(4)\n"
        "signal.signal(signal.SIGTERM, cleanup)\ntime.sleep(30)\n"
    )
    target = repository(tmp_path, document, body)
    with pytest.raises(InvocationError, match="command_timeout"):
        invoke_verification(target, tmp_path, "validate", trusted=True)
    assert marker.read_text() == "cleaned"


@pytest.mark.usefixtures("local")
def test_weekly_cli_explicit_scope_and_exit(
    manifest: Manifest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(weekly, "invoke_verification", lambda *_a, **_k: consumer())

    monkeypatch.setattr(
        BoundedProbe, "__call__", lambda *_: result("passed", "probe_valid")
    )
    source = tmp_path / "manifest.json"
    source.write_text(manifest.model_dump_json())
    args = [
        str(source),
        "--weekly",
        "--target",
        "example-linux",
        "--checkout",
        str(tmp_path),
    ]
    assert main(args) == 0
    assert json.loads(capsys.readouterr().out)["readiness"] == "READY"
    for extra in (
        ["--daily"],
        ["--check", "checkout"],
        ["--invoke-check", "synthetic"],
    ):
        assert main([*args, *extra]) == 2
        capsys.readouterr()
    assert main([str(source), "--weekly"]) == 2
    capsys.readouterr()


@pytest.mark.usefixtures("local")
def test_weekly_currency_preserves_executable_evidence(
    manifest: Manifest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(weekly, "invoke_verification", lambda *_a, **_k: consumer())
    report = weekly.run_weekly(
        manifest,
        {"example-linux": tmp_path},
        lambda *_: result("passed", "probe_valid"),
    )
    stale = readiness.assess_currency(
        report,
        manifest,
        now=report.timestamp + timedelta(days=8),
        max_age_seconds=604800,
    )
    assert stale.currency == "stale"
    assert stale.readiness == "UNKNOWN"
    assert isinstance(stale, weekly.WeeklyReport)
    assert stale.consumer_evidence == report.consumer_evidence
    value = report.model_dump(mode="json")
    value["consumer_evidence"] = {}
    with pytest.raises(ValidationError, match="consumer evidence"):
        weekly.WeeklyReport.model_validate_json(json.dumps(value))
