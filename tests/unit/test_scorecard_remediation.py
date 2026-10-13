"""Acceptance regression tests for provenance, retention and broker delivery."""

import json
from datetime import UTC, datetime, timedelta
from email.message import Message
from http.client import RemoteDisconnected
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from uuid import uuid4

import pytest

from recovery_verification import monthly, notifications, readiness, weekly
from recovery_verification.contract import parse_manifest
from recovery_verification.drill import execute_drill
from recovery_verification.history import read_history, write_history
from recovery_verification.provenance import ExecutionProvenance
from recovery_verification.scorecard import History, build_scorecard, capture_evidence
from tests.unit.test_monthly import Provider
from tests.unit.test_scorecard import (  # noqa: F401 -- Shared safe fixture.
    local,
    observe,
    reports,
)
from tests.unit.test_weekly import consumer


@pytest.mark.usefixtures("local")
def test_available_and_unavailable_provenance(
    document: dict[str, Any], tmp_path: Path
) -> None:
    manifest = parse_manifest(json.dumps(document))
    evidence = reports(manifest, tmp_path)
    attempt = observe(manifest, evidence).targets[0].tiers[0].latest_attempt
    assert attempt
    assert attempt.provenance.playbooks is None
    assert "playbooks" in attempt.unavailable_provenance
    assert (
        attempt.provenance.backups[0].source_id
        == manifest.targets[0].backup_sources[0].id
    )
    daily = evidence.entries[0].report.model_copy(
        update={
            "provenance": {
                "example-linux": ExecutionProvenance(
                    playbooks=("ansible/recover.yml",),
                    inventories=("ansible/inventory.yml",),
                )
            }
        }
    )
    captured = capture_evidence(manifest, "readiness", daily.model_dump_json())
    supplied = (
        observe(manifest, History(entries=(captured,)))
        .targets[0]
        .tiers[0]
        .latest_attempt
    )
    assert supplied
    assert supplied.provenance.playbooks == ("ansible/recover.yml",)
    assert "playbooks" not in supplied.unavailable_provenance
    drill = observe(manifest, evidence).targets[0].tiers[2].latest_attempt
    assert drill
    assert drill.provenance.backups[0].timestamp
    assert drill.provenance.backups[0].artifact_sha256
    for paths in ((), ("../unsafe",), ("ansible/a.yml", "ansible/a.yml")):
        with pytest.raises(ValueError, match=r"identifier|entrypoint|path"):
            ExecutionProvenance(playbooks=paths)


@pytest.mark.usefixtures("local")
def test_archive_regression_recurring_failures_and_restart(
    document: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = parse_manifest(json.dumps(document))
    good = reports(manifest, tmp_path)
    archive = tmp_path / "history.json"
    write_history(archive, good)
    monkeypatch.setattr(
        readiness,
        "_interfaces",
        lambda *_: readiness.Observation(state="failed", reason="missing_entrypoint"),
    )
    first = reports(manifest, tmp_path)
    second = reports(manifest, tmp_path)
    # A stale writer's subset cannot erase an earlier writer's attempts.
    write_history(archive, first)
    write_history(archive, second)
    restored = read_history(archive)
    assert len(restored.entries) == 9
    score = observe(manifest, restored)
    tier = score.targets[0].tiers[0]
    assert tier.regression
    assert tier.historical_failure_count == 6
    assert tier.consecutive_failures == 6
    assert "entrypoints" in tier.recurring_failed_checks
    assert tier.last_success
    document = json.loads(archive.read_text())
    document["history"]["entries"].pop()
    archive.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="invalid_evidence_archive"):
        read_history(archive)


@pytest.mark.usefixtures("local")
def test_transition_payload_identity_and_secret_boundary(
    document: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = parse_manifest(json.dumps(document))
    history = reports(manifest, tmp_path)
    previous = observe(manifest, history)
    stale = previous.model_copy(
        update={"observed_at": previous.observed_at + timedelta(days=2)}
    )
    stale = build_scorecard(manifest, history, now=stale.observed_at)
    first = notifications.notification_events(stale, previous)
    assert first == notifications.notification_events(stale, previous)
    later = stale.model_copy(
        update={"observed_at": stale.observed_at + timedelta(seconds=1)}
    )
    assert (
        notifications.notification_events(later, previous)[0]["event_id"]
        != first[0]["event_id"]
    )
    assert not notifications.notification_events(later, stale)
    with pytest.raises(ValueError, match="baseline_in_future"):
        notifications.notification_events(previous, stale)
    monkeypatch.setenv("ANOTHER_RUNTIME_SECRET", manifest.targets[0].revision)
    with pytest.raises(ValueError, match="credential_bearing"):
        notifications.notification_events(stale, previous)


@pytest.mark.parametrize(
    "outcome", ["transient", "permanent", "malformed", "mismatch", "disconnect"]
)
def test_broker_retry_and_receipt(
    outcome: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RECOVERY_EVENTS_URL", "http://127.0.0.1:8000")
    monkeypatch.setenv("RECOVERY_EVENTS_TOKEN", "safe-test-token")
    event: dict[str, object] = {
        "event_id": str(uuid4()),
        "correlation_id": str(uuid4()),
    }
    attempts: list[bytes | None] = []
    delays: list[float] = []

    class Response:
        status = 201

        def __enter__(self) -> Response:
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def read(self, _: int) -> bytes:
            if outcome == "malformed":
                return b"{}"
            return json.dumps(
                {
                    "schema_version": "1",
                    "record_id": str(uuid4())
                    if outcome == "mismatch"
                    else event["event_id"],
                    "correlation_id": event["correlation_id"],
                    "accepted_at": datetime.now(UTC).isoformat(),
                    "duplicate": len(attempts) > 1,
                }
            ).encode()

    class Opener:
        def open(self, request: Any, **_: Any) -> Response:
            attempts.append(request.data)
            if outcome == "permanent" or (
                outcome == "transient" and len(attempts) == 1
            ):
                raise HTTPError(
                    request.full_url,
                    403 if outcome == "permanent" else 503,
                    "",
                    Message(),
                    None,
                )
            if outcome == "disconnect":
                raise RemoteDisconnected("connection_closed")
            return Response()

    monkeypatch.setattr(notifications, "build_opener", lambda *_: Opener())
    monkeypatch.setattr(notifications, "sleep", delays.append)
    assert bool(notifications.send_event(event)) is (outcome == "transient")
    assert len(attempts) == (
        2 if outcome == "transient" else 3 if outcome == "disconnect" else 1
    )
    assert len(set(attempts)) == 1
    assert delays == (
        [0.1]
        if outcome == "transient"
        else [0.1, 0.2]
        if outcome == "disconnect"
        else []
    )


@pytest.mark.usefixtures("local")
def test_all_failure_tiers_and_bounded_payload(
    document: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = parse_manifest(json.dumps(document))
    good = observe(manifest, reports(manifest, tmp_path))
    monkeypatch.setattr(
        readiness,
        "_interfaces",
        lambda *_: readiness.Observation(state="failed", reason="missing_entrypoint"),
    )
    monkeypatch.setattr(
        weekly, "invoke_verification", lambda *_a, **_k: consumer("failed")
    )
    monkeypatch.setattr(
        monthly,
        "invoke_drill",
        lambda target, *_a, **_k: execute_drill(target, Provider("restore")),
    )
    bad = observe(manifest, reports(manifest, tmp_path))
    event = notifications.notification_events(bad, good)[0]
    data = event["data"]
    assert isinstance(data, dict)
    assert {f["verification_tier"] for f in data["failed_verifications"]} == {
        "readiness",
        "verification",
        "drill",
    }
    noisy_target = bad.targets[0].model_copy(
        update={"reasons": tuple("reason_" + str(i) + "x" * 90 for i in range(100))}
    )
    noisy = bad.model_copy(update={"targets": (noisy_target,)})
    bounded = notifications.notification_events(noisy, good)[0]["data"]
    assert isinstance(bounded, dict)
    assert bounded["details_omitted"]
    assert len(json.dumps(bounded)) < 4096


@pytest.mark.usefixtures("local")
def test_interrupted_archive_write_preserves_last_complete_record(
    document: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = parse_manifest(json.dumps(document))
    archive = tmp_path / "history.json"
    write_history(archive, reports(manifest, tmp_path))
    previous = archive.read_bytes()

    def fail_replace(*_: object) -> None:
        raise OSError("interrupted_write")

    monkeypatch.setattr(Path, "replace", fail_replace)
    with pytest.raises(OSError, match="interrupted_write"):
        write_history(archive, reports(manifest, tmp_path))
    assert archive.read_bytes() == previous
    assert not tuple(tmp_path.glob(".recovery-history-*"))
