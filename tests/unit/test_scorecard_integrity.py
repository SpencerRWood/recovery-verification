"""Native provenance, archive limits and downstream recovery failure protection."""

import json
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from recovery_verification import history as storage
from recovery_verification import readiness
from recovery_verification.contract import parse_manifest
from recovery_verification.scorecard import (
    History,
    capture_evidence,
    merge_history,
    parse_history,
)
from tests.unit.test_scorecard import (  # noqa: F401 -- Shared safe fixture.
    local,
    observe,
    reports,
)


@pytest.mark.usefixtures("local")
@pytest.mark.parametrize(
    "problem",
    [
        "owner",
        "entrypoint",
        "missing_owner",
        "revision",
        "missing_execution",
        "unsupported_weekly",
        "weekly_as_daily",
        "monthly_as_weekly",
        "backup",
        "backup_state",
        "provider",
        "drill_timestamp",
    ],
)
def test_native_provenance_guards(  # noqa: PLR0912 -- Enumerate distinct contract violations.
    document: dict[str, Any], tmp_path: Path, problem: str
) -> None:
    manifest = parse_manifest(json.dumps(document))
    history = reports(manifest, tmp_path)
    value = history.model_dump(mode="json")
    daily, weekly, monthly = value["entries"]
    if problem == "owner":
        daily["consumers"]["example-linux"]["owning_repository"] = "other/consumer"
    elif problem == "entrypoint":
        daily["consumers"]["example-linux"]["bootstrap_entrypoint"] = "../unsafe"
    elif problem == "missing_owner":
        daily["consumers"] = {}
    elif problem == "revision":
        daily["report"]["target_revisions"]["example-linux"] = "2" * 40
        for check in daily["report"]["checks"]:
            check["tested_revision"] = "2" * 40
    elif problem == "missing_execution":
        weekly["report"]["consumer_evidence"] = {}
        for check in weekly["report"]["checks"]:
            if check["reason"] == "consumer_executable_verification":
                check["reason"] = "forged_success"
    elif problem == "unsupported_weekly":
        document["targets"][0]["allowed_verification_levels"] = ["readiness", "drill"]
        document["targets"][0]["validation_commands"][0]["levels"] = [
            "readiness",
            "drill",
        ]
        manifest = parse_manifest(json.dumps(document))
        weekly["report"]["manifest_sha256"] = readiness.manifest_hash(manifest)
    elif problem == "weekly_as_daily":
        weekly["tier"] = "readiness"
        del weekly["report"]["consumer_evidence"]
    elif problem == "monthly_as_weekly":
        monthly["tier"] = "verification"
        monthly["report"]["consumer_evidence"] = {}
        del monthly["report"]["drill_evidence"]
        del monthly["report"]["fallback_evidence"]
    elif problem == "backup":
        monthly["report"]["drill_evidence"]["example-linux/synthetic"]["snapshots"] = []
    elif problem == "backup_state":
        monthly["report"]["checks"] = [
            c
            for c in monthly["report"]["checks"]
            if not c["check_id"].startswith("monthly_backup_")
        ]
    elif problem == "provider":
        monthly["report"]["drill_evidence"]["example-linux/synthetic"]["provider"] = (
            "unapproved"
        )
    else:
        monthly["report"]["timestamp"] = daily["report"]["timestamp"]
        for check in monthly["report"]["checks"]:
            check["timestamp"] = daily["report"]["timestamp"]
    with pytest.raises(
        ValueError,
        match=r"evidence|identity|contract|provenance|tier|freshness|timestamp",
    ):
        observe(manifest, parse_history(json.dumps(value)))


@pytest.mark.usefixtures("local")
def test_upstream_failure_blocks_current_downstream_success(
    document: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    upstream = deepcopy(document["targets"][0])
    upstream.update(
        id="upstream", owning_repository="example/upstream", required_dependencies=[]
    )
    document["targets"].append(upstream)
    document["targets"][0]["required_dependencies"].append(
        {
            "id": "upstream-recovery",
            "kind": "repository",
            "reference": "git://example/upstream",
        }
    )
    manifest = parse_manifest(json.dumps(document))
    history = reports(manifest, tmp_path)
    monkeypatch.setattr(
        readiness,
        "_interfaces",
        lambda target, *_: readiness.Observation(
            state="failed" if target.id == "upstream" else "passed",
            reason="fixture_result",
        ),
    )
    failed = readiness.run_readiness(
        manifest,
        {t.id: tmp_path for t in manifest.targets},
        lambda *_: readiness.Observation(state="passed", reason="fixture_ok"),
    )
    history = merge_history(
        history, (capture_evidence(manifest, "readiness", failed.model_dump_json()),)
    )
    score = observe(manifest, history)
    assert score.readiness == "NOT_READY"
    assert score.targets[0].readiness == "NOT_READY"
    assert score.targets[0].upstream_targets == ("upstream",)
    assert score.targets[0].full_recovery_verified is False


def test_archive_limits_preserve_prior_contents(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "history.json"
    storage.write_history(path, History())
    previous = path.read_bytes()
    assert path.stat().st_mode & 0o777 == 0o600
    monkeypatch.setattr(storage, "MAX_HISTORY_BYTES", 1)
    with pytest.raises(ValueError, match="evidence_history_too_large"):
        storage.read_history(path)
    with pytest.raises(ValueError, match="evidence_history_too_large"):
        storage.write_history(path, History())
    assert path.read_bytes() == previous


@pytest.mark.usefixtures("local")
def test_historical_consumer_identity_is_not_rewritten(
    document: dict[str, Any], tmp_path: Path
) -> None:
    manifest = parse_manifest(json.dumps(document))
    history = reports(manifest, tmp_path)
    document["targets"][0]["owning_repository"] = "new/consumer"
    document["targets"][0]["entrypoints"]["bootstrap"]["entrypoint"] = (
        "scripts/new-bootstrap"
    )
    current = parse_manifest(json.dumps(document))
    score = observe(current, history)
    attempted = score.targets[0].tiers[0].latest_attempt
    assert attempted
    assert attempted.owning_repository == "example/linux-consumer"
    assert attempted.bootstrap_entrypoint == "scripts/bootstrap"
    assert attempted.currency == "superseded"
    assert score.targets[0].owning_repository == "new/consumer"
    assert datetime.now(UTC).tzinfo
