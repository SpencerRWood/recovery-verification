"""Meaningful change events and sanitized standardized event transport."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request
from uuid import uuid4

import pytest

from recovery_verification import notifications, readiness
from recovery_verification.cli import main
from recovery_verification.contract import parse_manifest
from recovery_verification.history import write_history
from recovery_verification.scorecard import (
    History,
    ScorecardPolicy,
    build_scorecard,
    canonical,
    capture_evidence,
    merge_history,
)
from tests.unit.test_scorecard import (  # noqa: F401 -- Reuse safe orchestration fixture.
    local,
    observe,
    reports,
)


@pytest.mark.usefixtures("local")
def test_success_quiet_failure_replay_and_recovery(
    document: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = parse_manifest(json.dumps(document))
    good_history = reports(manifest, tmp_path)
    good = observe(manifest, good_history)
    assert not notifications.notification_events(good)
    assert not notifications.notification_events(
        observe(manifest, reports(manifest, tmp_path)), good
    )
    monkeypatch.setattr(
        readiness,
        "_interfaces",
        lambda *_: readiness.Observation(
            state="failed",
            reason="missing_entrypoint",
            category="recovery_prerequisite",
        ),
    )
    bad_history = merge_history(good_history, reports(manifest, tmp_path).entries)
    bad = observe(manifest, bad_history)
    events = notifications.notification_events(bad, good)
    assert len(events) == 1
    event = events[0]
    assert event["event_type"] == "recovery.verification-failed"
    data = event["data"]
    assert isinstance(data, dict)
    assert data["normalized_status"] == "NOT_READY"
    assert data["failed_checks"] == ["entrypoints"]
    assert data["last_successful_relevant_verification"]
    assert data["tested_commit"] == manifest.targets[0].revision
    assert not notifications.notification_events(bad, bad)
    repeated = observe(manifest, bad_history)
    assert notifications.notification_events(repeated, good) == events
    monkeypatch.setattr(
        readiness,
        "_interfaces",
        lambda *_: readiness.Observation(state="passed", reason="fixture_ok"),
    )
    recovered = observe(
        manifest, merge_history(bad_history, reports(manifest, tmp_path).entries)
    )
    recovered_events = notifications.notification_events(recovered, bad)
    assert recovered_events[0]["severity"] == "success"
    assert recovered_events[0]["event_type"] == "recovery.readiness-changed"


@pytest.mark.usefixtures("local")
def test_stale_and_upstream_transitions_alert(
    document: dict[str, Any], tmp_path: Path
) -> None:
    manifest = parse_manifest(json.dumps(document))
    history = reports(manifest, tmp_path)
    good = observe(manifest, history)
    stale = build_scorecard(
        manifest, history, now=datetime.now(UTC) + timedelta(days=2)
    )
    assert notifications.notification_events(stale, good)[0]["severity"] == "warning"
    unknown = observe(
        manifest,
        history,
        policy=ScorecardPolicy(upstream_targets={"example-linux": ("absent",)}),
    )
    event = notifications.notification_events(unknown, good)[0]
    assert event["severity"] == "warning"
    assert not notifications.notification_events(unknown, unknown)


@pytest.mark.parametrize(
    "url",
    [
        "",
        "http://external.invalid",
        "file:///tmp/secret",
        "https://user:pass@example.test",
        "https://example.test?token=x",
        "https://example.test#secret",
    ],
)
def test_transport_rejects_unsafe_destination(
    url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RECOVERY_EVENTS_URL", url)
    monkeypatch.setenv("RECOVERY_EVENTS_TOKEN", "test-credential")
    assert not notifications.send_event({"schema_version": "1"})


@pytest.mark.parametrize(
    "outcome", ["success", "status", "oversize", "outage", "secret", "missing_token"]
)
def test_transport_bounded_auth_and_sanitized_failure(
    outcome: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    requests: list[Request] = []
    identity = str(uuid4())
    monkeypatch.setenv("RECOVERY_EVENTS_URL", "https://broker.example.test")
    monkeypatch.setenv("RECOVERY_EVENTS_TOKEN", "test-credential")

    class Response:
        status = 201 if outcome != "status" else 500

        def __enter__(self) -> Response:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self, maximum: int) -> bytes:
            assert maximum == 65537
            return (
                b"x" * 65537
                if outcome == "oversize"
                else json.dumps(
                    {
                        "schema_version": "1",
                        "record_id": identity,
                        "correlation_id": identity,
                        "accepted_at": datetime.now(UTC).isoformat(),
                        "duplicate": False,
                    }
                ).encode()
            )

    class Opener:
        def open(self, request: Request, *, timeout: int) -> Response:
            assert timeout == 5
            requests.append(request)
            if outcome == "outage":
                raise URLError("test-credential")
            return Response()

    monkeypatch.setattr(notifications, "build_opener", lambda *_: Opener())
    monkeypatch.setattr(notifications, "sleep", lambda _: None)
    event: dict[str, object] = {
        "data": "test-credential" if outcome == "secret" else "safe",
        "event_id": identity,
        "correlation_id": identity,
    }
    if outcome == "missing_token":
        monkeypatch.delenv("RECOVERY_EVENTS_TOKEN")
    assert notifications.send_event(event) == (outcome == "success")
    assert len(requests) == (
        3
        if outcome in {"outage", "status"}
        else 0
        if outcome in {"secret", "missing_token"}
        else 1
    )
    if requests:
        assert requests[0].get_header("Authorization") == "Bearer test-credential"
        assert requests[0].full_url == "https://broker.example.test/v1/events"
        payload = requests[0].data
        assert isinstance(payload, bytes)
        assert b"test-credential" not in payload
    notifications._NoRedirect().redirect_request()
    assert notifications.publish_events((event,), lambda _: False) is False


@pytest.mark.usefixtures("local")
def test_cli_notification_failure_disclosed_without_readiness_rewrite(
    document: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    manifest = parse_manifest(json.dumps(document))
    history = reports(manifest, tmp_path)
    good = observe(manifest, history)
    prior = tmp_path / "previous.json"
    prior.write_text(canonical(good))
    monkeypatch.setattr(
        readiness,
        "_interfaces",
        lambda *_: readiness.Observation(state="failed", reason="missing_entrypoint"),
    )
    history = merge_history(history, reports(manifest, tmp_path).entries)
    archive = tmp_path / "history.json"
    write_history(archive, history)
    source = tmp_path / "manifest.json"
    source.write_text(manifest.model_dump_json())
    monkeypatch.setattr("recovery_verification.cli.publish_events", lambda _: False)
    assert (
        main(
            [
                str(source),
                "--scorecard",
                "--history",
                str(archive),
                "--as-of",
                (datetime.now(UTC) + timedelta(seconds=1)).isoformat(),
                "--previous-scorecard",
                str(prior),
                "--notify",
            ]
        )
        == 4
    )
    output = capsys.readouterr()
    assert json.loads(output.out)["readiness"] == "NOT_READY"
    assert output.err == "notification_delivery_unavailable\n"


@pytest.mark.usefixtures("local")
def test_cli_native_report_ingestion(
    document: dict[str, Any], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    manifest = parse_manifest(json.dumps(document))
    history = reports(manifest, tmp_path)
    source = tmp_path / "manifest.json"
    source.write_text(manifest.model_dump_json())
    args = [
        str(source),
        "--scorecard",
        "--as-of",
        (datetime.now(UTC) + timedelta(seconds=1)).isoformat(),
    ]
    for option, entry in zip(
        ("--daily-evidence", "--weekly-evidence", "--monthly-evidence"),
        history.entries,
        strict=True,
    ):
        path = tmp_path / (entry.tier + ".json")
        path.write_text(entry.report.model_dump_json())
        args.extend((option, str(path)))
    assert main(args) == 0
    assert json.loads(capsys.readouterr().out)["history_count"] == 3
    changed = manifest.model_copy(update={"targets": ()})
    with pytest.raises(ValueError, match="invalid_native_evidence"):
        capture_evidence(
            changed, "readiness", history.entries[0].report.model_dump_json()
        )
    assert History().entries == ()
