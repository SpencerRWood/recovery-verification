"""Validate actual events-service v1 models and event-to-notification routing."""

import importlib
import json
import os
import sys
from pathlib import Path
from typing import Any

import pytest
from tests.unit.test_scorecard import (  # noqa: F401 -- Safe fixture.
    local,
    observe,
    reports,
)

from recovery_verification import notifications, readiness
from recovery_verification.contract import parse_manifest


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
    # Models loaded from another checkout must not leak into unrelated tests.
    for name in tuple(sys.modules):
        if name == "events_service" or name.startswith("events_service."):
            sys.modules.pop(name)
