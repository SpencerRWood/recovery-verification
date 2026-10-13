"""Terminal coverage, honest missing evidence and durable replay boundaries."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from dagster import (
    DagsterEvent,
    DagsterInstance,
    DagsterRunStatus,
    build_sensor_context,
    job,
)
from dagster._core.storage.dagster_run import DagsterRun

from recovery_verification import readiness as readiness_checks
from recovery_verification.contract import parse_manifest
from recovery_verification.dagster.notifications import recovery_run_notifications
from recovery_verification.history import write_history
from recovery_verification.notifications import EventReceipt
from recovery_verification.orchestration import OrchestrationConfig
from recovery_verification.probes import result
from recovery_verification.scorecard import History
from recovery_verification.terminal_notifications import (
    brief,
    process_terminal_runs,
    read_outbox,
    terminal_event,
)
from tests.unit.test_scorecard import (  # noqa: F401 -- Native report fixtures.
    local,
    reports,
)


@pytest.fixture
def config(document: dict[str, Any], tmp_path: Path) -> OrchestrationConfig:
    return OrchestrationConfig(
        manifest_json=json.dumps(document),
        checkouts={"example-linux": str(tmp_path)},
        state_directory=str(tmp_path / "state"),
        environment="test",
    )


@pytest.fixture
def enabled(config: OrchestrationConfig) -> OrchestrationConfig:
    return OrchestrationConfig.model_validate(
        config.model_dump()
        | {
            "notifications_enabled": True,
            "notification_start_time": datetime.now(UTC) - timedelta(seconds=60),
            "notification_dagster_url": "https://dagster.example.test",
        }
    )


@job(name="recovery_readiness_daily")
def observed_job() -> None:
    pass


def receipt(event: dict[str, Any], *, duplicate: bool = False) -> EventReceipt:
    return EventReceipt(
        record_id=UUID(event["event_id"]),
        correlation_id=UUID(event["correlation_id"]),
        accepted_at=datetime.now(UTC),
        duplicate=duplicate,
    )


@pytest.mark.parametrize("status", ["SUCCESS", "FAILURE", "CANCELED"])
def test_each_terminal_status_has_honest_missing_evidence(
    enabled: OrchestrationConfig,
    status: str,
) -> None:
    run = DagsterRun(job_name="recovery_readiness_daily", run_id=str(uuid4()))
    event = terminal_event(enabled, run, status, datetime.now(UTC))
    assert event["data"]["execution_status"] == status
    assert event["data"]["recovery_readiness"] != "READY"
    assert not event["data"]["full_recovery_verified"]
    assert "backup UNAVAILABLE" in event["data"]["message"]
    assert "Daily / weekly / monthly" in event["data"]["message"]
    assert event["data"]["evidence_link"].endswith(run.run_id)
    assert "Run ID: " + run.run_id in event["data"]["message"]
    assert "Independent key custody: not attested" in event["data"]["message"]


@pytest.mark.usefixtures("local")
@pytest.mark.parametrize("readiness", ["READY", "DEGRADED", "NOT_READY"])
def test_success_message_separates_execution_from_readiness(
    enabled: OrchestrationConfig,
    tmp_path: Path,
    readiness: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = parse_manifest(enabled.manifest_json)
    if readiness == "NOT_READY":
        monkeypatch.setattr(
            readiness_checks,
            "_interfaces",
            lambda *_: result("failed", "entrypoint_missing"),
        )
    history = reports(manifest, tmp_path)
    if readiness == "DEGRADED":
        history = History(entries=(history.entries[0],))
    root = Path(enabled.state_directory)
    root.mkdir()
    write_history(root / "history.json", history)
    event = terminal_event(
        enabled,
        DagsterRun(job_name="recovery_readiness_daily", run_id=str(uuid4())),
        "SUCCESS",
        datetime.now(UTC) + timedelta(seconds=1),
    )
    assert event["data"]["recovery_readiness"] == readiness
    assert event["data"]["execution_status"] == "SUCCESS"
    assert "Independent key custody: not attested" in event["data"]["message"]


@pytest.mark.usefixtures("local")
def test_success_is_not_readiness_and_fixture_is_not_backup_integrity(
    enabled: OrchestrationConfig,
    tmp_path: Path,
) -> None:
    root = Path(enabled.state_directory)
    root.mkdir()
    write_history(
        root / "history.json", reports(parse_manifest(enabled.manifest_json), tmp_path)
    )
    run = DagsterRun(job_name="recovery_readiness_daily", run_id=str(uuid4()))
    event = terminal_event(
        enabled, run, "SUCCESS", datetime.now(UTC) + timedelta(seconds=1)
    )
    assert "integrity: UNAVAILABLE" in event["data"]["message"]
    assert event["data"]["scorecard_reference"].startswith("sha256:")


def test_terminal_event_survives_unreadable_archive(
    enabled: OrchestrationConfig,
) -> None:
    root = Path(enabled.state_directory)
    root.mkdir()
    (root / "history.json").write_text("invalid")
    event = terminal_event(
        enabled,
        DagsterRun(job_name="recovery_readiness_daily", run_id=str(uuid4())),
        "FAILURE",
        datetime.now(UTC),
    )
    assert event["data"]["recovery_readiness"] == "UNKNOWN"
    assert event["data"]["unavailable_reason"] == "recovery_evidence_unavailable"


def test_terminal_scan_covers_all_statuses_and_new_unchanged_successes(
    enabled: OrchestrationConfig,
) -> None:
    sent: list[dict[str, Any]] = []

    def sender(event: dict[str, Any]) -> EventReceipt:
        sent.append(event)
        return receipt(event)

    with DagsterInstance.ephemeral() as instance:
        for status in (
            DagsterRunStatus.SUCCESS,
            DagsterRunStatus.SUCCESS,
            DagsterRunStatus.FAILURE,
            DagsterRunStatus.CANCELED,
        ):
            run = instance.create_run_for_job(
                observed_job, tags={"recovery/environment": "test"}
            )
            if status == DagsterRunStatus.SUCCESS:
                instance.report_dagster_event(
                    DagsterEvent("PIPELINE_SUCCESS", run.job_name), run.run_id
                )
            elif status == DagsterRunStatus.FAILURE:
                instance.report_run_failed(run)
            else:
                instance.report_run_canceled(run)
        # Identical job names in another unbound location are excluded.
        other = instance.create_run_for_job(observed_job)
        instance.report_dagster_event(
            DagsterEvent("PIPELINE_SUCCESS", other.job_name), other.run_id
        )
        state = process_terminal_runs(enabled, instance, sender)
        assert len(sent) == 2
        assert len(state.receipts) == 2
        assert len(state.pending) == 2
        state = process_terminal_runs(enabled, instance, sender)
        assert len(state.receipts) == 4
        process_terminal_runs(enabled, instance, sender)
        assert len(sent) == 4
        assert (
            Path(enabled.state_directory) / "terminal-notifications.json"
        ).stat().st_mode & 0o777 == 0o600


def test_historical_terminal_run_is_not_notified_after_recovery(
    enabled: OrchestrationConfig,
) -> None:
    sent: list[dict[str, Any]] = []

    def sender(event: dict[str, Any]) -> EventReceipt:
        sent.append(event)
        return receipt(event)

    with DagsterInstance.ephemeral() as instance:
        run = instance.create_run_for_job(
            observed_job, tags={"recovery/environment": "test"}
        )
        instance.report_dagster_event(
            DagsterEvent("PIPELINE_SUCCESS", run.job_name), run.run_id
        )
        recovered = enabled.model_copy(
            update={"notification_start_time": datetime.now(UTC) + timedelta(seconds=1)}
        )
        process_terminal_runs(recovered, instance, sender)
        process_terminal_runs(recovered, instance, sender)
    assert not sent


def test_ack_lost_after_acceptance_replays_exact_frozen_event(
    enabled: OrchestrationConfig,
) -> None:
    accepted: list[dict[str, Any]] = []
    with DagsterInstance.ephemeral() as instance:
        run = instance.create_run_for_job(
            observed_job, tags={"recovery/environment": "test"}
        )
        instance.report_dagster_event(
            DagsterEvent("PIPELINE_SUCCESS", run.job_name), run.run_id
        )

        def lost_ack(event: dict[str, Any]) -> None:
            accepted.append(event)

        state = process_terminal_runs(enabled, instance, lost_ack)
        assert len(state.pending) == 1

        def duplicate_ack(event: dict[str, Any]) -> EventReceipt:
            assert event == accepted[0]
            return receipt(event, duplicate=True)

        state = process_terminal_runs(enabled, instance, duplicate_ack)
        assert not state.pending
        assert len(state.receipts) == 1


def test_invalid_receipt_and_changed_cutoff_fail_closed(
    enabled: OrchestrationConfig,
) -> None:
    with DagsterInstance.ephemeral() as instance:
        run = instance.create_run_for_job(
            observed_job, tags={"recovery/environment": "test"}
        )
        instance.report_dagster_event(
            DagsterEvent("PIPELINE_SUCCESS", run.job_name), run.run_id
        )

        def wrong(event: dict[str, Any]) -> EventReceipt:
            return receipt(event).model_copy(update={"record_id": uuid4()})

        with pytest.raises(ValueError, match="receipt_mismatch"):
            process_terminal_runs(enabled, instance, wrong)
    path = Path(enabled.state_directory) / "terminal-notifications.json"
    with pytest.raises(ValueError, match="start_time_changed"):
        read_outbox(path, datetime.now(UTC))
    document = json.loads(path.read_text())
    document["sha256"] = "invalid"
    path.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="invalid_terminal"):
        read_outbox(path, enabled.notification_start_time)  # type: ignore[arg-type]


def test_disabled_and_invalid_inputs_are_explicit(config: OrchestrationConfig) -> None:
    with pytest.raises(ValueError, match="explicit_terminal"):
        OrchestrationConfig.model_validate(
            config.model_dump() | {"notifications_enabled": True}
        )
    with (
        DagsterInstance.ephemeral() as instance,
        pytest.raises(ValueError, match="not_enabled"),
    ):
        process_terminal_runs(config, instance)
    assert brief(["a" * 1000], 50).endswith("(more in evidence)")
    with pytest.raises(ValueError, match="terminal_run_status"):
        terminal_event(
            config,
            DagsterRun(job_name="job", run_id=str(uuid4())),
            "STARTED",
            datetime.now(UTC),
        )


def test_sensor_disabled_without_network(
    config: OrchestrationConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "recovery_verification.dagster.notifications.load_config", lambda: config
    )
    with build_sensor_context() as context:
        assert (
            recovery_run_notifications.evaluate_tick(context).skip_message
            == "terminal_notifications_disabled"
        )
