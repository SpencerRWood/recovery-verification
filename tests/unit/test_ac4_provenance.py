"""Consumer-produced metadata survives the complete native reporting boundary."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import yaml

from recovery_verification import readiness, weekly
from recovery_verification.contract import parse_manifest
from recovery_verification.models import ConsumerResult, ConsumerVerificationResult
from recovery_verification.provenance import BackupProvenance, ExecutionProvenance
from recovery_verification.scorecard import History, capture_evidence
from tests.unit.test_scorecard import (  # noqa: F401 -- Safe boundary fixture.
    local,
    observe,
)
from tests.unit.test_weekly import consumer

CONSUMER_OBSERVATION = readiness._consumer


@pytest.mark.usefixtures("local")
def test_native_producer_to_scorecard_provenance(
    document: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = parse_manifest(json.dumps(document))
    preflight = ConsumerResult.model_validate_json(
        json.dumps(
            {
                "schema_version": "1",
                "mode": "preflight",
                "readiness_state": "passed",
                "checks": [{"id": "syntax", "state": "passed", "reason": "checked"}],
                "provenance": {
                    "playbooks": ["ansible/recover.yml"],
                    "inventories": ["ansible/inventory"],
                },
            }
        )
    )
    monkeypatch.setattr(readiness, "_consumer", CONSUMER_OBSERVATION)
    monkeypatch.setattr(readiness, "invoke_readiness", lambda *_a, **_k: preflight)
    result = consumer()
    digest = next(c.artifact_sha256 for c in result.checks if c.phase == "restore")
    metadata = ExecutionProvenance(
        playbooks=("recovery/weekly.yml",),
        inline_inventory="localhost",
        backups=(
            BackupProvenance(
                source_id="representative",
                kind="representative_fixture",
                artifact_sha256=digest,
            ),
        ),
    )
    result = result.model_copy(update={"provenance": metadata})
    monkeypatch.setattr(weekly, "invoke_verification", lambda *_a, **_k: result)
    report = weekly.run_weekly(
        manifest,
        {t.id: tmp_path for t in manifest.targets},
        lambda *_: readiness.Observation(state="passed", reason="fixture_probe"),
    )
    evidence = capture_evidence(manifest, "verification", report.model_dump_json())
    score = observe(manifest, History(entries=(evidence,)))
    daily, deeper = (s.latest_attempt for s in score.targets[0].tiers[:2])
    assert daily
    assert deeper
    assert daily.provenance.playbooks == ("ansible/recover.yml",)
    assert daily.provenance.inventories == ("ansible/inventory",)
    assert deeper.provenance.playbooks == ("recovery/weekly.yml",)
    assert deeper.provenance.inline_inventory == "localhost"
    assert "inventories" not in deeper.unavailable_provenance
    fixture = next(
        b for b in deeper.provenance.backups if b.kind == "representative_fixture"
    )
    assert fixture.artifact_sha256 == digest
    assert fixture.timestamp is None
    assert "backup:representative:timestamp" in deeper.unavailable_provenance
    assert not score.full_recovery_verified


def test_metadata_cannot_invent_restore_artifact_or_credentials() -> None:
    raw = consumer().model_dump(mode="json")
    raw["provenance"] = {
        "backups": [
            {
                "source_id": "representative",
                "kind": "representative_fixture",
                "artifact_sha256": "0" * 64,
            }
        ]
    }
    with pytest.raises(ValueError, match="unbound_backup_provenance"):
        ConsumerVerificationResult.model_validate_json(json.dumps(raw))
    raw["provenance"] = {"credentials": "must-not-persist"}
    with pytest.raises(ValueError, match="extra_forbidden"):
        ConsumerVerificationResult.model_validate_json(json.dumps(raw))


def test_future_check_metadata_rejected(
    document: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = parse_manifest(json.dumps(document))
    monkeypatch.setattr(
        readiness,
        "_checkout",
        lambda *_: readiness.Observation(state="passed", reason="fixture_checkout"),
    )
    monkeypatch.setattr(
        readiness,
        "_interfaces",
        lambda *_: readiness.Observation(state="passed", reason="fixture_interfaces"),
    )
    metadata = ExecutionProvenance(
        backups=(
            BackupProvenance(
                source_id="representative",
                timestamp=datetime.now(UTC) + timedelta(days=1),
            ),
        )
    )
    monkeypatch.setattr(
        readiness,
        "_consumer",
        lambda *_: readiness.Observation(
            state="passed", reason="checked", provenance=metadata
        ),
    )
    with pytest.raises(ValueError, match="invalid check provenance timestamp"):
        readiness.run_readiness(
            manifest,
            {t.id: tmp_path for t in manifest.targets},
            lambda *_: readiness.Observation(state="passed", reason="fixture_probe"),
        )


def test_publication_requires_explicit_main_dispatch() -> None:
    root = Path(__file__).parents[2]
    workflow = yaml.safe_load((root / ".github/workflows/release.yml").read_text())
    triggers = workflow.get("on", workflow.get(True))
    assert set(triggers) == {"workflow_dispatch"}
    assert (
        triggers["workflow_dispatch"]["inputs"]["release_approved"]["default"] is False
    )
    guard = workflow["jobs"]["release"]["if"]
    assert "github.ref == 'refs/heads/main'" in guard
    assert "github.event_name == 'workflow_dispatch'" in guard
    assert "inputs.release_approved == true" in guard
