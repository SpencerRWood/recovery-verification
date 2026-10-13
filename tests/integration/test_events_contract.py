"""Validate actual events-service v1 models and event-to-notification routing."""

import importlib
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from dagster._core.storage.dagster_run import DagsterRun
from tests.unit.test_scorecard import (  # noqa: F401 -- Safe fixture.
    local,
    observe,
    reports,
)

from recovery_verification import notifications, readiness
from recovery_verification.contract import parse_manifest
from recovery_verification.orchestration import OrchestrationConfig
from recovery_verification.terminal_notifications import terminal_event


@pytest.mark.usefixtures("local")
def test_actual_events_service_contract(
    document: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = os.environ.get("RECOVERY_EVENTS_CHECKOUT")
    if not source:
        if os.environ.get("RECOVERY_REQUIRE_EVENTS_CONTRACT") == "1":
            pytest.fail(
                "RECOVERY_EVENTS_CHECKOUT must select the events-service source"
            )
        pytest.skip("External schema checkout explicitly configured by repo verify")
    assert source
    root = Path(source)
    assert (root / "src/events_service/contracts.py").is_file()
    monkeypatch.syspath_prepend(str(root / "src"))
    contracts = importlib.import_module("events_service.contracts")
    routing = importlib.import_module("events_service.notification_policy")
    manifest = parse_manifest(json.dumps(document))
    good = observe(manifest, reports(manifest, tmp_path))
    monkeypatch.setattr(
        readiness,
        "_interfaces",
        lambda *_: readiness.Observation(state="failed", reason="missing_entrypoint"),
    )
    bad = observe(manifest, reports(manifest, tmp_path))
    events = notifications.notification_events(bad, good)
    router = routing.NotificationRouter(
        [
            routing.NotificationPolicy(
                name="recovery",
                channels=["operator"],
                sources={"recovery-verification"},
                event_types={
                    "recovery.verification-failed",
                    "recovery.readiness-changed",
                },
            )
        ]
    )
    for event in events:
        envelope = contracts.EventEnvelope.model_validate(event)
        request = router.from_event(envelope)
        assert router.channels(request, envelope.event_type) == ["operator"]
        assert request.context["event"]["data"]["target"] == manifest.targets[0].id
        assert len(request.message) <= 4096
        assert request.idempotency_key == f"event-{envelope.event_id}"
        receipt = contracts.Receipt(
            record_id=envelope.event_id,
            correlation_id=envelope.correlation_id,
            accepted_at=envelope.occurred_at,
            duplicate=True,
        )
        assert notifications.EventReceipt.model_validate_json(receipt.model_dump_json())
    assert notifications.notification_events(bad, bad) == ()
    terminal_config = OrchestrationConfig(
        manifest_json=json.dumps(document),
        checkouts={"example-linux": str(tmp_path)},
        state_directory=str(tmp_path / "state"),
        environment="test",
        notification_dagster_url="https://dagster.example.test",
    )
    terminal_router = routing.NotificationRouter(
        [
            routing.NotificationPolicy(
                name="recovery-scorecards",
                channels=["telegram"],
                sources={"recovery-verification"},
                event_types={"recovery.run-completed"},
            )
        ]
    )
    for status in ("SUCCESS", "FAILURE", "CANCELED"):
        event = terminal_event(
            terminal_config,
            DagsterRun(job_name="recovery_readiness_daily", run_id=str(uuid4())),
            status,
            datetime.now(UTC),
        )
        envelope = contracts.EventEnvelope.model_validate(event)
        request = terminal_router.from_event(envelope)
        assert request.title == "Backup & Recovery Scorecard"
        assert request.message == envelope.data["message"]
        assert terminal_router.channels(request, envelope.event_type) == ["telegram"]
        assert "backup UNAVAILABLE" in request.message
        assert "recovery UNKNOWN" in request.message
    # Models loaded from another checkout must not leak into unrelated tests.
    for name in tuple(sys.modules):
        if name == "events_service" or name.startswith("events_service."):
            sys.modules.pop(name)
