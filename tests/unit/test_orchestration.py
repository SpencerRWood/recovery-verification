"""Operational policy tests use simulated observations, never production assurance."""

import json
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
from dagster import (
    DefaultScheduleStatus,
    DefaultSensorStatus,
    SensorResult,
    SkipReason,
    build_sensor_context,
)

from recovery_verification import orchestration, readiness
from recovery_verification.contract import Level, parse_manifest
from recovery_verification.dagster.definitions import defs
from recovery_verification.dagster.triggers import recovery_relevant_changes, schedules
from recovery_verification.execution import (
    Execution,
    RetryingProbe,
    execution_session,
    read_state,
)
from recovery_verification.health import main as health
from recovery_verification.history import read_history
from recovery_verification.orchestration import (
    Cadence,
    OrchestrationConfig,
    changed_paths,
    current_manifest,
    execution_request,
    load_config,
    proportional_tier,
    revision_cursor,
)
from recovery_verification.probes import result
from recovery_verification.scorecard import Entry, Scorecard
from tests.unit.test_scorecard import (  # noqa: F401 -- Shared synthetic boundary fixture.
    local,
    reports,
)


@pytest.fixture
def config(document: dict[str, Any], tmp_path: Path) -> OrchestrationConfig:
    return OrchestrationConfig.model_validate_json(
        json.dumps(
            {
                "manifest_json": json.dumps(document),
                "checkouts": {"example-linux": str(tmp_path)},
                "state_directory": str(tmp_path / "state"),
                "environment": "test",
            }
        )
    )


def test_explicit_configuration_and_health(
    config: OrchestrationConfig,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("RECOVERY_ORCHESTRATION_CONFIG", raising=False)
    with pytest.raises(ValueError, match="not_provided"):
        load_config()
    path = tmp_path / "config.json"
    monkeypatch.setenv("RECOVERY_ORCHESTRATION_CONFIG", str(path))
    path.write_text(config.model_dump_json())
    assert load_config() == config
    assert len(schedules()) == 3
    assert health() == 0
    assert json.loads(capsys.readouterr().out)["scope"] == "registration_only"
    for s in schedules():
        assert s.execution_timezone == "America/New_York"
        assert s.default_status == DefaultScheduleStatus.STOPPED
    assert (
        defs.resolve_sensor_def("recovery_relevant_changes").default_status
        == DefaultSensorStatus.STOPPED
    )
    changed = config.model_copy(
        update={
            "cadence": Cadence(
                daily="1 2 * * *", weekly="2 3 * * 2", monthly="3 4 2 * *"
            )
        }
    )
    path.write_text(changed.model_dump_json())
    assert schedules()[0].cron_schedule == "1 2 * * *"
    path.write_text("{}")
    with pytest.raises(ValueError, match="configuration_invalid"):
        schedules()


@pytest.mark.parametrize(
    "update",
    [
        {"checkouts": {}},
        {"checkouts": {"example-linux": "relative"}},
        {"state_directory": "relative"},
        {
            "manifest_exports": {
                "absent": {"entrypoint": "export", "args": [], "timeout_seconds": 1}
            }
        },
        {
            "cadence": {
                "daily": "invalid",
                "weekly": "0 0 * * 0",
                "monthly": "0 0 1 * *",
            }
        },
        {"probe_attempts": 4},
        {"environment": "unsafe.example"},
        {"token": "forbidden"},
    ],
)
def test_invalid_configuration_fails_closed(
    config: OrchestrationConfig, update: dict[str, Any]
) -> None:
    document = config.model_dump(mode="json") | update
    with pytest.raises(ValueError, match="validation error"):
        OrchestrationConfig.model_validate_json(json.dumps(document))


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("README.md", None),
        ("ansible/inventories/dev", "readiness"),
        ("mounts.yml", "readiness"),
        ("secret-references.json", "readiness"),
        ("ansible/roles/host/tasks/main.yml", "verification"),
        ("ansible/playbooks/dev.yml", "verification"),
        ("backup/config.yml", "readiness"),
        ("scripts/restore.py", "verification"),
        ("recovery/consumer.v1.json", "readiness"),
        ("scripts/bootstrap.py", "verification"),
    ],
)
def test_proportional_changes(path: str, expected: str | None) -> None:
    assert proportional_tier((path,), isolated_test_mode=False) == expected
    assert (
        proportional_tier(("scripts/bootstrap.py",), isolated_test_mode=True) == "drill"
    )


@pytest.mark.parametrize(
    "path", ["compose/dagster/compose.yml", "environments/dev.yml"]
)
def test_f1_configuration_sensor_regression(
    path: str, config: OrchestrationConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "recovery_verification.dagster.triggers.load_config", lambda: config
    )
    monkeypatch.setattr(
        "recovery_verification.dagster.triggers.changed_paths", lambda *_: (path,)
    )
    context = build_sensor_context(cursor=json.dumps({"example-linux": "b" * 40}))
    result = recovery_relevant_changes(context)
    assert isinstance(result, SensorResult)
    assert result.run_requests
    assert result.run_requests[0].job_name == "recovery_readiness_daily"
    assert result.cursor != context.cursor


@pytest.mark.parametrize(
    ("paths", "isolated", "tier"),
    [
        (("config/backup-paths.yml",), False, "readiness"),
        (("volumes.conf",), False, "readiness"),
        (("credentials.json",), False, "readiness"),
        ((".env",), False, "readiness"),
        (("Dockerfile",), False, "readiness"),
        (("settings/custom.format",), False, "readiness"),
        (("scripts/recovery_verify.py",), False, "verification"),
        (("scripts/bootstrap.py",), False, "verification"),
        (("ansible/bootstrap.yml",), False, "verification"),
        (("scripts/bootstrap.py",), True, "drill"),
        (("README.md", "docs/guide.txt"), True, None),
        (("compose/compose.yaml", "scripts/restore.py"), False, "verification"),
        (("scripts/restore.py", "scripts/bootstrap.py"), True, "drill"),
    ],
)
def test_sensor_classification_requests_and_duplicate_suppression(
    paths: tuple[str, ...],
    isolated: bool,
    tier: Level | None,
    config: OrchestrationConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = config.model_copy(update={"isolated_test_mode": isolated})
    monkeypatch.setattr(
        "recovery_verification.dagster.triggers.load_config", lambda: config
    )
    diff = Mock(return_value=paths)
    monkeypatch.setattr("recovery_verification.dagster.triggers.changed_paths", diff)
    old = "b" * 40
    revision = parse_manifest(config.manifest_json).targets[0].revision
    context = build_sensor_context(cursor=json.dumps({"example-linux": old}))
    result = recovery_relevant_changes(context)
    replay = recovery_relevant_changes(context)
    assert isinstance(result, SensorResult)
    assert isinstance(replay, SensorResult)
    assert result.run_requests == replay.run_requests
    assert result.cursor == json.dumps({"example-linux": revision}, sort_keys=True)
    assert len(result.run_requests or []) == (1 if tier else 0)
    if tier:
        assert result.run_requests
        request = result.run_requests[0]
        assert request.run_key == f"change:test:example-linux:{old}:{revision}:{tier}"
        assert request.job_name == orchestration.JOBS[tier]
        for asset in request.run_config["ops"].values():
            assert asset["config"]["target_ids"] == ["example-linux"]
            bound = parse_manifest(asset["config"]["manifest_json"])
            assert bound.targets[0].revision == revision
    diff.reset_mock()
    consumed = recovery_relevant_changes(build_sensor_context(cursor=result.cursor))
    assert isinstance(consumed, SensorResult)
    assert not consumed.run_requests
    diff.assert_not_called()


@pytest.mark.parametrize(
    ("operation", "error"),
    [
        ("proportional_tier", ValueError("classification_failed")),
        ("changed_paths", subprocess.CalledProcessError(1, ["git", "diff"])),
        ("changed_paths", subprocess.TimeoutExpired(["git", "diff"], 10)),
        ("changed_paths", UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid")),
        ("current_manifest", ValueError("export_failed")),
        ("current_manifest", OSError("export_unavailable")),
    ],
)
def test_sensor_failures_retain_cursor(
    operation: str,
    error: Exception,
    config: OrchestrationConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "recovery_verification.dagster.triggers.load_config", lambda: config
    )
    monkeypatch.setattr(
        "recovery_verification.dagster.triggers.changed_paths",
        Mock(return_value=("compose/dagster/compose.yml",)),
    )
    monkeypatch.setattr(
        f"recovery_verification.dagster.triggers.{operation}", Mock(side_effect=error)
    )
    context = build_sensor_context(cursor=json.dumps({"example-linux": "b" * 40}))
    result = recovery_relevant_changes.evaluate_tick(context)
    assert not result.run_requests
    assert result.skip_message == "recovery_change_detection_unavailable"
    assert result.cursor == context.cursor


@pytest.mark.parametrize(
    "path", ["", "../config.yml", "/config.yml", "a//b", "a/./b", "a\0b"]
)
def test_ambiguous_classification_fails_closed(path: str) -> None:
    with pytest.raises(ValueError, match="invalid_change_path"):
        proportional_tier(("scripts/bootstrap.py", path), isolated_test_mode=True)


@pytest.mark.parametrize("output", [b"unterminated", b"a\0\0", b"\xff\0"])
def test_malformed_git_diff_fails_closed(
    output: bytes, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "recovery_verification.orchestration.subprocess.run",
        Mock(return_value=subprocess.CompletedProcess([], 0, stdout=output)),
    )
    with pytest.raises(ValueError, match=r"invalid_change_diff|decode"):
        changed_paths(tmp_path, "a" * 40, "b" * 40)


def test_invalid_git_revision_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="invalid_change_revision"):
        changed_paths(tmp_path, "--unsafe", "b" * 40)


def test_requests_bind_target_and_revision(config: OrchestrationConfig) -> None:
    manifest = current_manifest(config)
    for tier, names in orchestration.ASSETS.items():
        request = execution_request(config, manifest, tier, "key")
        assert request.job_name == orchestration.JOBS[tier]
        assert set(request.run_config["ops"]) == set(names)
        assert request.tags["recovery/environment"] == "test"
        assert request.tags["recovery/execution_key"] == "key"
        assert request.tags["recovery/operation"] == "verification"
    with pytest.raises(ValueError, match="selection"):
        execution_request(config, manifest, "readiness", "key", ("absent",))


def test_export_and_changed_paths(
    config: OrchestrationConfig, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    export = {"entrypoint": "manifest", "args": [], "timeout_seconds": 1}
    exported = OrchestrationConfig.model_validate_json(
        json.dumps(
            config.model_dump(mode="json")
            | {"manifest_exports": {"example-linux": export}}
        )
    )
    run = Mock(
        return_value=subprocess.CompletedProcess(
            [], 0, stdout=config.manifest_json.encode()
        )
    )
    monkeypatch.setattr("recovery_verification.orchestration.subprocess.run", run)
    assert current_manifest(exported) == parse_manifest(config.manifest_json)
    assert "RECOVERY_EVENTS_TOKEN" not in run.call_args.kwargs["env"]
    run.return_value.stdout = b"x" * 65537
    with pytest.raises(ValueError, match="limit"):
        current_manifest(exported)
    doc = json.loads(config.manifest_json)
    doc["targets"][0]["id"] = "different"
    run.return_value.stdout = json.dumps(doc).encode()
    with pytest.raises(ValueError, match="identity"):
        current_manifest(exported)
    (tmp_path / "manifest").symlink_to(tmp_path / "elsewhere")
    with pytest.raises(ValueError, match="unsafe"):
        current_manifest(exported)
    run.return_value.stdout = b"roles/a\0inventory\0"
    assert changed_paths(tmp_path, "a" * 40, "b" * 40) == ("roles/a", "inventory")
    run.return_value.stdout = b"x" * 65537
    with pytest.raises(ValueError, match="limit"):
        changed_paths(tmp_path, "a" * 40, "b" * 40)


@pytest.mark.parametrize(
    "raw", ["[]", '{"target":"wrong"}', '{"target":1}', '{"target":"' + "x" * 40 + '"}']
)
def test_invalid_cursor(raw: str) -> None:
    with pytest.raises(ValueError, match="cursor"):
        revision_cursor(raw)


def test_change_sensor_cursor_and_retry(
    config: OrchestrationConfig, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "config.json"
    path.write_text(config.model_dump_json())
    monkeypatch.setenv("RECOVERY_ORCHESTRATION_CONFIG", str(path))
    first = recovery_relevant_changes(build_sensor_context())
    assert isinstance(first, SensorResult)
    assert not first.run_requests
    assert first.cursor
    current = json.loads(first.cursor)
    assert (
        current["example-linux"]
        == parse_manifest(config.manifest_json).targets[0].revision
    )
    monkeypatch.setattr(
        "recovery_verification.dagster.triggers.changed_paths",
        lambda *_: ("scripts/restore.py",),
    )
    context = build_sensor_context(cursor=json.dumps({"example-linux": "b" * 40}))
    changed = recovery_relevant_changes(context)
    assert isinstance(changed, SensorResult)
    assert changed.run_requests
    assert changed.run_requests[0].job_name == "recovery_verification_weekly"
    replayed = recovery_relevant_changes(context)
    assert isinstance(replayed, SensorResult)
    assert replayed.run_requests
    assert changed.run_requests[0].run_key == replayed.run_requests[0].run_key
    monkeypatch.setattr(
        "recovery_verification.dagster.triggers.changed_paths",
        Mock(side_effect=ValueError("unavailable")),
    )
    unavailable = recovery_relevant_changes(context)
    assert isinstance(unavailable, SkipReason)
    assert unavailable.skip_message == "recovery_change_detection_unavailable"
    assert context.cursor == json.dumps({"example-linux": "b" * 40})


def test_transient_probe_retry_only(tmp_path: Path) -> None:
    transient = result("unavailable", "probe_unavailable")
    passed = result("passed", "probe_available")
    probe = Mock(side_effect=[transient, passed])
    assert RetryingProbe(2, probe)("tool", "ansible", tmp_path, None) == passed
    assert probe.call_count == 2
    failed = Mock(return_value=result("failed", "missing"))
    assert RetryingProbe(3, failed)("tool", "ansible", tmp_path, None).state == "failed"
    assert failed.call_count == 1
    probe = Mock(return_value=transient)
    assert RetryingProbe(1, probe)("tool", "ansible", tmp_path, None) == transient


@pytest.mark.usefixtures("local")
def test_restart_replay_exclusion_and_interruption(
    config: OrchestrationConfig, tmp_path: Path
) -> None:
    manifest = parse_manifest(config.manifest_json)
    daily = reports(manifest, tmp_path).entries[0].report.model_dump_json()
    with execution_session(config, "key", manifest) as session:
        assert session.report("readiness", lambda: daily) == daily
        with (
            pytest.raises(ValueError, match="already_running"),
            execution_session(config, "other", manifest),
        ):
            pass
    before = read_history(Path(config.state_directory) / "history.json")
    with execution_session(config, "key", manifest) as session:
        assert (
            session.report(
                "readiness", Mock(side_effect=AssertionError("must not rerun"))
            )
            == daily
        )
    assert read_history(Path(config.state_directory) / "history.json") == before
    with (
        pytest.raises(RuntimeError),
        execution_session(config, "interrupted", manifest),
    ):
        raise RuntimeError("simulated interruption")
    state = read_state(Path(config.state_directory) / "execution.json")
    assert any(
        a.state == "interrupted" and a.finished_at for a in state.attempts.values()
    )
    assert read_history(Path(config.state_directory) / "history.json") == before
    altered = manifest.model_copy(
        update={
            "targets": (manifest.targets[0].model_copy(update={"revision": "b" * 40}),)
        }
    )
    with (
        pytest.raises(ValueError, match="manifest_mismatch"),
        execution_session(config, "key", altered),
    ):
        pass


@pytest.mark.usefixtures("local")
def test_outbox_ack_replay_and_unchanged_conditions(
    config: OrchestrationConfig, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = config.model_copy(update={"notifications_enabled": True})
    manifest = parse_manifest(config.manifest_json)
    root = Path(config.state_directory)
    root.mkdir()
    good = reports(manifest, tmp_path).entries[0].report.model_dump_json()
    session = Execution(config, "good", manifest)
    session.report("readiness", lambda: good)
    session.finish(
        sender=Mock(side_effect=AssertionError("initial success stays quiet"))
    )
    monkeypatch.setattr(
        readiness, "_interfaces", lambda *_: result("failed", "entrypoint_missing")
    )
    bad = reports(manifest, tmp_path).entries[0].report.model_dump_json()
    session = Execution(config, "bad", manifest)
    session.report("readiness", lambda: bad)
    session.finish(sender=lambda _: False)
    state = read_state(root / "execution.json")
    assert state.notification_state == "unavailable"
    assert len(state.pending_events) == 1
    event = state.pending_events[0]
    data = event["data"]
    assert isinstance(data, dict)
    assert data["environment"] == "test"
    assert data["evidence_reference"]
    sender = Mock(return_value=True)
    session = Execution(config, "retry", manifest)
    session.finish(sender=sender)
    assert sender.call_args.args[0] == event
    assert not read_state(root / "execution.json").pending_events
    session = Execution(config, "same-condition-new-attempt", manifest)
    session.report(
        "readiness",
        lambda: reports(manifest, tmp_path).entries[0].report.model_dump_json(),  # noqa: PLW0108 -- Deferred execution factory.
    )
    session.finish(sender=Mock(side_effect=AssertionError("unchanged condition")))
    # A different failed check with the same overall state/reason is actionable.
    monkeypatch.setattr(
        readiness, "_interfaces", lambda *_: result("passed", "fixture_ok")
    )
    monkeypatch.setattr(
        readiness, "_consumer", lambda *_: result("failed", "entrypoint_missing")
    )
    session = Execution(config, "different-failure", manifest)

    def changed_report() -> str:
        return reports(manifest, tmp_path).entries[0].report.model_dump_json()

    session.report("readiness", changed_report)
    changed_failure = Mock(return_value=True)
    session.finish(sender=changed_failure)
    assert changed_failure.call_count == 1


def test_journal_tampering_and_state_permissions(
    config: OrchestrationConfig, tmp_path: Path
) -> None:
    root = Path(config.state_directory)
    root.mkdir()
    session = Execution(config, "key", parse_manifest(config.manifest_json))
    session.finish()
    assert (root / "execution.json").stat().st_mode & 0o777 == 0o600
    data = json.loads((root / "execution.json").read_text())
    data["sha256"] = "wrong"
    (root / "execution.json").write_text(json.dumps(data))
    with pytest.raises(ValueError, match="invalid_execution_journal"):
        read_state(root / "execution.json")
    other = tmp_path / "symlink"
    other.symlink_to(root)
    with (
        pytest.raises(ValueError, match="unsafe_execution"),
        execution_session(
            config.model_copy(update={"state_directory": str(other)}),
            "key",
            parse_manifest(config.manifest_json),
        ),
    ):
        pass


@pytest.mark.usefixtures("local")
@pytest.mark.parametrize("tier", ["readiness", "verification", "drill"])
def test_interrupted_attempt_is_in_scorecard_without_losing_success(
    config: OrchestrationConfig,
    tmp_path: Path,
    tier: Level,
) -> None:
    manifest = parse_manifest(config.manifest_json)
    success = reports(manifest, tmp_path)
    with execution_session(config, "good", manifest, expected_tier=tier) as session:
        for entry in success.entries:

            def native_report(value: Entry = entry) -> str:
                return value.report.model_dump_json()

            session.report(entry.tier, native_report)
    with execution_session(config, "interrupted", manifest, expected_tier=tier):
        pass
    state = read_state(Path(config.state_directory) / "execution.json")
    assert state.previous_scorecard
    score = Scorecard.model_validate_json(state.previous_scorecard)
    selected = next(t for t in score.targets[0].tiers if t.tier == tier)
    assert selected.status == "unavailable"
    assert selected.last_success
    assert selected.latest_attempt
    assert selected.latest_attempt.readiness == "UNKNOWN"
    assert not score.full_recovery_verified
    assert any(a.interrupted_evidence_reference for a in state.attempts.values())


@pytest.mark.usefixtures("local")
def test_abandoned_attempt_reconciled_after_restart(
    config: OrchestrationConfig,
) -> None:
    manifest = parse_manifest(config.manifest_json)
    Path(config.state_directory).mkdir()
    Execution(config, "abandoned", manifest, requested_tier="readiness")
    with execution_session(config, "next", manifest, expected_tier="readiness"):
        pass
    history = read_history(Path(config.state_directory) / "history.json")
    assert len(history.entries) == 2
    assert all(e.report.readiness == "UNKNOWN" for e in history.entries)
