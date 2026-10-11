"""The manual Dagster operator surface consumes the same pure scorecard."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from tests.unit.test_scorecard import (  # noqa: F401 -- Shared safe fixture.
    local,
    reports,
)

from recovery_verification.contract import parse_manifest
from recovery_verification.dagster.definitions import defs
from recovery_verification.history import write_history


@pytest.mark.usefixtures("local")
def test_manual_scorecard_job(document: dict[str, Any], tmp_path: Path) -> None:
    manifest = parse_manifest(json.dumps(document))
    path = tmp_path / "history.json"
    write_history(path, reports(manifest, tmp_path))
    run = defs.resolve_job_def("recovery_scorecard").execute_in_process(
        run_config={
            "ops": {
                "recovery_readiness_scorecard": {
                    "config": {
                        "manifest_json": manifest.model_dump_json(),
                        "history_path": str(path),
                        "observed_at": (
                            datetime.now(UTC) + timedelta(seconds=1)
                        ).isoformat(),
                    }
                }
            }
        },
    )
    assert run.success
    score = json.loads(run.output_for_node("recovery_readiness_scorecard"))
    assert score["readiness"] == "READY"
    assert score["full_recovery_verified"] is True
