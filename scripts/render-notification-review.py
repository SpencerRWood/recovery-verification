"""Offline real-evidence review; never mutates history or publishes events."""

import argparse
import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from dagster._core.storage.dagster_run import DagsterRun

from recovery_verification.notification_presentation import DESCRIPTIONS
from recovery_verification.orchestration import OrchestrationConfig
from recovery_verification.terminal_notifications import (
    scorecard_for_run,
    terminal_event,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs", required=True, type=Path)
    parser.add_argument("--before", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    inputs = json.loads(args.inputs.read_text())
    previous = json.loads(args.before.read_text())["event"]
    source = inputs["run"]
    run = DagsterRun(
        job_name=source["job_name"],
        run_id=source["run_id"],
        run_config=source["run_config"],
    )
    at = datetime.fromtimestamp(source["end_time"], UTC)
    with tempfile.TemporaryDirectory(prefix="op544-review-") as directory:
        Path(directory, "history.json").write_text(
            json.dumps(inputs["provider"]["archive"])
        )
        config = OrchestrationConfig.model_validate_json(
            json.dumps(inputs["provider"]["config"])
        ).model_copy(
            update={
                "state_directory": directory,
                "notification_dagster_url": "https://dev-dagster.woodhost.cloud",
            }
        )
        current = terminal_event(config, run, source["status"], at)
        score = scorecard_for_run(config, run, at)
        expected = {
            (
                t.target_id,
                s.tier,
                d.check_id.removeprefix(t.target_id + "/"),
                d.reason,
                d.state,
            )
            for t in score.targets
            for s in t.tiers
            if s.latest_attempt
            for d in s.latest_attempt.diagnostics
            if d.state not in {"passed", "not_applicable"}
        }
        represented = {
            (d["target"], d["tier"], d["check"], d["reason"], d["state"])
            for f in current["data"]["findings"]
            for d in f["diagnostics"]
        }
        review = {
            "scope": "offline retained real run; "
            "no current runtime or delivery attestation",
            "original_diagnostic_count": len(expected),
            "preserved_diagnostic_count": len(expected & represented),
            "missing_diagnostics": sorted(expected - represented),
            "grouped_finding_count": len(current["data"]["findings"]),
            "old_characters": len(previous["data"]["message"]),
            "new_characters": len(current["data"]["message"]),
            "event_identity_preserved": previous["event_id"] == current["event_id"],
            "readiness_preserved": previous["data"]["recovery_readiness"]
            == current["data"]["recovery_readiness"],
            "descriptions": DESCRIPTIONS,
            "before": previous["data"]["message"],
            "after": current["data"]["message"],
            "event": current,
            "synthetic_events": [
                terminal_event(config, run, status, at, synthetic=True)
                for status in ("SUCCESS", "FAILURE")
            ],
        }
        args.output.write_text(json.dumps(review, indent=2) + "\n")
        if (
            expected - represented
            or not review["event_identity_preserved"]
            or not review["readiness_preserved"]
        ):
            raise SystemExit(
                "Notification review lost diagnostics or changed identity/readiness"
            )


if __name__ == "__main__":
    main()
