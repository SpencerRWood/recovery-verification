"""Tier separation, native evidence integrity, currency and operator diagnostics."""

import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from recovery_verification import monthly, readiness, weekly
from recovery_verification.cli import main
from recovery_verification.contract import Manifest, parse_manifest
from recovery_verification.drill import execute_drill
from recovery_verification.history import read_history, write_history
from recovery_verification.probes import result
from recovery_verification.scorecard import (
    History,
    Scorecard,
    ScorecardPolicy,
    build_scorecard,
    canonical,
    capture_evidence,
    human_summary,
    merge_history,
    parse_history,
)
from tests.unit.test_monthly import Provider, unsupported
from tests.unit.test_weekly import consumer


@pytest.fixture
def local(monkeypatch: pytest.MonkeyPatch) -> None:
    for module, names in (
        (readiness, ("_checkout", "_interfaces", "_consumer")),
        (weekly, ("_checkout",)),
        (monthly, ("_checkout",)),
    ):
        for name in names:
            monkeypatch.setattr(module, name, lambda *_: result("passed", "fixture_ok"))
    monkeypatch.setattr(weekly, "invoke_verification", lambda *_a, **_k: consumer())
    monkeypatch.setattr(
        monthly,
        "invoke_drill",
        lambda target, *_a, **_k: execute_drill(target, Provider()),
    )


def reports(manifest: Manifest, root: Path) -> History:
    checkouts = {t.id: root for t in manifest.targets}
    probe = lambda *_: result("passed", "fixture_ok")  # noqa: E731
    return History(
        entries=(
            capture_evidence(
                manifest,
                "readiness",
                readiness.run_readiness(manifest, checkouts, probe).model_dump_json(),
            ),
            capture_evidence(
                manifest,
                "verification",
                weekly.run_weekly(manifest, checkouts, probe).model_dump_json(),
            ),
            capture_evidence(
                manifest,
                "drill",
                monthly.run_monthly(manifest, checkouts, probe).model_dump_json(),
            ),
        )
    )


def observe(manifest: Manifest, history: History, **kwargs: Any) -> Scorecard:
    return build_scorecard(
        manifest, history, now=datetime.now(UTC) + timedelta(seconds=1), **kwargs
    )


@pytest.mark.usefixtures("local")
def test_all_tiers_success_provenance_and_order(
    document: dict[str, Any], tmp_path: Path
) -> None:
    manifest = parse_manifest(json.dumps(document))
    history = reports(manifest, tmp_path)
    now = datetime.now(UTC) + timedelta(seconds=1)
    score = build_scorecard(manifest, history, now=now)
    assert score.readiness == "READY"
    assert score.full_recovery_verified
    target = score.targets[0]
    assert target.strongest_current_level == "drill"
    assert target.strongest_successful_level == "drill"
    assert all(t.status == "passed" and t.last_success for t in target.tiers)
    drill = target.tiers[-1].latest_attempt
    assert drill
    assert drill.tested_revision == manifest.targets[0].revision
    assert drill.manifest_version == "1"
    assert drill.evidence_reference.startswith("sha256:")
    assert drill.consumer_evidence["example-linux/synthetic"]
    assert drill.bootstrap_entrypoint == "scripts/bootstrap"
    reverse = History(entries=tuple(reversed(history.entries)))
    assert canonical(build_scorecard(manifest, reverse, now=now)) == canonical(score)
    assert observe(manifest, merge_history(history, history.entries)).history_count == 3
    assert "monthly=passed" in human_summary(score)
    assert parse_history(canonical(history)) == history


@pytest.mark.usefixtures("local")
def test_missing_and_daily_only(document: dict[str, Any], tmp_path: Path) -> None:
    manifest = parse_manifest(json.dumps(document))
    history = reports(manifest, tmp_path)
    score = observe(manifest, History(entries=(history.entries[0],)))
    assert score.readiness == "DEGRADED"
    assert not score.full_recovery_verified
    assert [t.status for t in score.targets[0].tiers] == [
        "passed",
        "missing",
        "missing",
    ]
    assert observe(manifest, History()).readiness == "UNKNOWN"


@pytest.mark.usefixtures("local")
def test_monthly_fallback_does_not_attest_drill(
    document: dict[str, Any], tmp_path: Path
) -> None:
    manifest = unsupported(document)
    score = observe(manifest, reports(manifest, tmp_path))
    assert score.readiness == "DEGRADED"
    assert not score.full_recovery_verified
    assert score.targets[0].strongest_current_level == "verification"
    tier = score.targets[0].tiers[-1]
    assert tier.status == "unsupported"
    assert tier.last_success is None
    assert tier.reasons == ("isolated_bootstrap_not_supported",)
    assert "add_approved_isolated_bootstrap" in tier.next_actions


@pytest.mark.usefixtures("local")
@pytest.mark.parametrize("tier", ["readiness", "verification", "drill"])
def test_failure_preserves_independent_success(
    document: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tier: str
) -> None:
    manifest = parse_manifest(json.dumps(document))
    good = reports(manifest, tmp_path)
    if tier == "readiness":
        monkeypatch.setattr(
            readiness, "_interfaces", lambda *_: result("failed", "entrypoint_missing")
        )
    elif tier == "verification":
        monkeypatch.setattr(
            weekly, "invoke_verification", lambda *_a, **_k: consumer("failed")
        )
    else:
        monkeypatch.setattr(
            monthly,
            "invoke_drill",
            lambda target, *_a, **_k: execute_drill(target, Provider("restore")),
        )
    failed = reports(manifest, tmp_path)
    history = merge_history(good, failed.entries)
    score = observe(manifest, history)
    assert score.readiness == "NOT_READY"
    assert not score.full_recovery_verified
    selected = next(t for t in score.targets[0].tiers if t.tier == tier)
    assert selected.status == "failed"
    assert selected.last_success
    assert selected.latest_attempt
    assert selected.last_success.timestamp < selected.latest_attempt.timestamp
    assert selected.reasons
    assert selected.next_actions
    assert score.history_count == 6


@pytest.mark.usefixtures("local")
@pytest.mark.parametrize("problem", ["expired", "future", "superseded", "override"])
def test_freshness_and_historical_success(
    document: dict[str, Any], tmp_path: Path, problem: str
) -> None:
    manifest = parse_manifest(json.dumps(document))
    history = reports(manifest, tmp_path)
    now = datetime.now(UTC) + timedelta(days=2)
    if problem == "future":
        now = datetime.now(UTC) - timedelta(days=2)
    if problem == "superseded":
        document["targets"][0]["revision"] = "2" * 40
        manifest = parse_manifest(json.dumps(document))
    if problem == "override":
        # The existing target cadence controls evidence age, not a second policy.
        document["targets"][0]["cadence_overrides"] = [
            {"level": "readiness", "interval_seconds": 1}
        ]
        manifest = parse_manifest(json.dumps(document))
        history = reports(manifest, tmp_path)
    score = build_scorecard(manifest, history, now=now)
    assert score.readiness == "UNKNOWN"
    assert score.targets[0].tiers[0].last_success
    assert score.targets[0].tiers[0].status == (
        "superseded" if problem == "superseded" else "expired"
    )


@pytest.mark.usefixtures("local")
@pytest.mark.parametrize(
    "problem",
    [
        "missing_check",
        "timestamp",
        "identity",
        "contradictory",
        "wrong_tier",
        "forged_drill",
    ],
)
def test_malformed_or_contradictory_fails_closed(
    document: dict[str, Any], tmp_path: Path, problem: str
) -> None:
    manifest = parse_manifest(json.dumps(document))
    history = reports(manifest, tmp_path)
    value = history.model_dump(mode="json")
    report = value["entries"][0]["report"]
    if problem == "missing_check":
        report["checks"] = [
            c for c in report["checks"] if c["check_id"] != "entrypoints"
        ]
    elif problem == "timestamp":
        report["checks"][0]["timestamp"] = (
            datetime.now(UTC) + timedelta(days=1)
        ).isoformat()
    elif problem == "identity":
        report["target_revisions"]["absent"] = "1" * 40
    elif problem == "contradictory":
        duplicate = deepcopy(value["entries"][0])
        duplicate["report"]["checks"][0]["reason"] = "contradiction"
        value["entries"].append(duplicate)
    elif problem == "wrong_tier":
        value["entries"][0]["tier"] = "verification"
    else:
        unsupported(document)
        manifest = parse_manifest(json.dumps(document))
        value["entries"][-1]["report"]["manifest_sha256"] = readiness.manifest_hash(
            manifest
        )
    with pytest.raises(ValueError, match=r"evidence|contract|tier|drill|identity"):
        observe(manifest, parse_history(json.dumps(value)))


@pytest.mark.usefixtures("local")
def test_multiple_targets_partial_availability_and_dependencies(
    document: dict[str, Any], tmp_path: Path
) -> None:
    upstream = deepcopy(document["targets"][0])
    upstream.update(id="upstream", owning_repository="example/upstream")
    upstream["required_dependencies"] = []
    document["targets"].append(upstream)
    manifest = parse_manifest(json.dumps(document))
    history = reports(manifest, tmp_path)
    assert observe(manifest, history).readiness == "READY"
    partial = readiness.run_readiness(
        manifest, {"example-linux": tmp_path}, lambda *_: result("passed", "fixture_ok")
    )
    history = merge_history(
        history, (capture_evidence(manifest, "readiness", partial.model_dump_json()),)
    )
    score = observe(
        manifest,
        history,
        policy=ScorecardPolicy(upstream_targets={"example-linux": ("upstream",)}),
    )
    assert score.readiness == "UNKNOWN"
    assert score.targets[0].readiness == "UNKNOWN"
    assert "upstream_not_ready:upstream" in score.targets[0].reasons
    missing = observe(
        manifest,
        reports(manifest, tmp_path),
        policy=ScorecardPolicy(upstream_targets={"example-linux": ("absent",)}),
    )
    assert missing.targets[0].readiness == "UNKNOWN"
    invalid_links: tuple[dict[str, tuple[str, ...]], ...] = (
        {"example-linux": ("upstream",), "upstream": ("example-linux",)},
        {"absent": ("upstream",)},
        {"example-linux": ("upstream", "upstream")},
    )
    for links in invalid_links:
        with pytest.raises(ValueError, match=r"dependencies|dependency"):
            observe(manifest, history, policy=ScorecardPolicy(upstream_targets=links))


@pytest.mark.usefixtures("local")
def test_native_secret_boundaries_and_safe_errors(
    document: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    manifest = parse_manifest(json.dumps(document))
    history = reports(manifest, tmp_path)
    value = history.model_dump(mode="json")
    value["entries"][0]["report"]["credentials"] = "do-not-echo"
    with pytest.raises(ValueError, match="invalid_evidence_history"):
        parse_history(json.dumps(value))
    monkeypatch.setenv("RECOVERY_EVENTS_TOKEN", "fixture_ok")
    with pytest.raises(ValueError, match="invalid_evidence_history"):
        observe(manifest, history)
    monkeypatch.delenv("RECOVERY_EVENTS_TOKEN")
    path = tmp_path / "manifest.json"
    path.write_text(manifest.model_dump_json())
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps(value))
    assert (
        main(
            [
                str(path),
                "--scorecard",
                "--as-of",
                datetime.now(UTC).isoformat(),
                "--evidence",
                str(bad),
            ]
        )
        == 2
    )
    assert "do-not-echo" not in capsys.readouterr().out
    for raw in (
        '{"schema_version":"1","schema_version":"1","entries":[]}',
        '{"schema_version":"9"}',
        "not-json",
    ):
        with pytest.raises(ValueError, match="invalid_evidence_history"):
            parse_history(raw)


@pytest.mark.usefixtures("local")
def test_history_cli_and_human_output(
    document: dict[str, Any], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    manifest = parse_manifest(json.dumps(document))
    history = reports(manifest, tmp_path)
    path, archive, incoming = (
        tmp_path / name for name in ("manifest.json", "history.json", "incoming.json")
    )
    path.write_text(manifest.model_dump_json())
    write_history(incoming, history)
    args = [
        str(path),
        "--scorecard",
        "--as-of",
        (datetime.now(UTC) + timedelta(seconds=1)).isoformat(),
        "--history",
        str(archive),
        "--evidence",
        str(incoming),
    ]
    assert main(args) == 0
    score = Scorecard.model_validate_json(capsys.readouterr().out)
    assert score.full_recovery_verified
    assert len(read_history(archive).entries) == 3
    assert main([*args, "--human"]) == 0
    assert "example-linux" in capsys.readouterr().out
    for extra in (
        ["--daily"],
        ["--target", "example-linux"],
        ["--checkout", str(tmp_path)],
    ):
        assert main([*args, *extra]) == 2
        capsys.readouterr()
    assert main([str(path), "--scorecard"]) == 2
    capsys.readouterr()
    assert main([str(path), "--human"]) == 2
    capsys.readouterr()
    with pytest.raises(ValueError, match="timezone_required"):
        build_scorecard(manifest, history, now=datetime(2026, 10, 11))  # noqa: DTZ001 -- Invalid fixture.
