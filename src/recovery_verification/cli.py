"""Bounded local preflight interface; no consumer commands are executed."""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from recovery_verification.contract import parse_manifest
from recovery_verification.history import read_bounded, read_history, write_history
from recovery_verification.invocation import invoke_readiness
from recovery_verification.monthly import run_monthly
from recovery_verification.notifications import notification_events, publish_events
from recovery_verification.preflight import preflight
from recovery_verification.probes import BoundedProbe
from recovery_verification.readiness import run_readiness
from recovery_verification.scorecard import (
    History,
    Scorecard,
    ScorecardPolicy,
    build_scorecard,
    canonical,
    capture_evidence,
    human_summary,
    merge_history,
)
from recovery_verification.weekly import run_weekly


def main(argv: list[str] | None = None) -> int:  # noqa: PLR0911, PLR0912, PLR0915 -- Existing diagnostic modes share one input/error boundary.
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--target", action="append", default=[], dest="targets")
    parser.add_argument("--checkout", type=Path)
    parser.add_argument("--daily", action="store_true")
    parser.add_argument("--weekly", action="store_true")
    parser.add_argument("--monthly", action="store_true")
    parser.add_argument("--scorecard", action="store_true")
    parser.add_argument("--history", type=Path)
    parser.add_argument("--evidence", action="append", type=Path, default=[])
    parser.add_argument("--daily-evidence", action="append", type=Path, default=[])
    parser.add_argument("--weekly-evidence", action="append", type=Path, default=[])
    parser.add_argument("--monthly-evidence", action="append", type=Path, default=[])
    parser.add_argument(
        "--as-of", help="Explicit timezone-aware scorecard observation time"
    )
    parser.add_argument("--policy", type=Path)
    parser.add_argument("--human", action="store_true")
    parser.add_argument("--previous-scorecard", type=Path)
    parser.add_argument("--notify", action="store_true")
    parser.add_argument("--check", action="append", default=[], dest="checks")
    parser.add_argument(
        "--invoke-check", help="Explicitly trust and invoke one readiness check"
    )
    args = parser.parse_args(argv)
    try:
        manifest = parse_manifest(args.manifest.read_text(encoding="utf-8"))
        if args.scorecard:
            if (
                args.daily
                or args.weekly
                or args.monthly
                or args.checks
                or args.invoke_check
                or args.checkout
                or args.targets
                or not args.as_of
            ):
                raise ValueError("scorecard_requires_explicit_observation_time")
            history = (
                read_history(args.history)
                if args.history and args.history.exists()
                else History()
            )
            for path in args.evidence:
                history = merge_history(history, read_history(path).entries)
            for path in args.daily_evidence:
                history = merge_history(
                    history,
                    (capture_evidence(manifest, "readiness", read_bounded(path)),),
                )
            for path in args.weekly_evidence:
                history = merge_history(
                    history,
                    (capture_evidence(manifest, "verification", read_bounded(path)),),
                )
            for path in args.monthly_evidence:
                history = merge_history(
                    history,
                    (capture_evidence(manifest, "drill", read_bounded(path)),),
                )
            policy = (
                ScorecardPolicy.model_validate_json(args.policy.read_bytes())
                if args.policy
                else None
            )
            scorecard = build_scorecard(
                manifest, history, now=datetime.fromisoformat(args.as_of), policy=policy
            )
            previous = (
                Scorecard.model_validate_json(args.previous_scorecard.read_bytes())
                if args.previous_scorecard
                else None
            )
            if args.history:
                write_history(args.history, history)
            events = notification_events(scorecard, previous)
            delivered = publish_events(events) if args.notify else True
            sys.stdout.write(
                human_summary(scorecard) if args.human else canonical(scorecard) + "\n"
            )
            if not delivered:
                sys.stderr.write("notification_delivery_unavailable\n")
                return 4
            return {"READY": 0, "DEGRADED": 3, "NOT_READY": 1, "UNKNOWN": 4}[
                scorecard.readiness
            ]
        if (
            args.history
            or args.evidence
            or args.daily_evidence
            or args.weekly_evidence
            or args.monthly_evidence
            or args.as_of
            or args.policy
            or args.human
            or args.previous_scorecard
            or args.notify
        ):
            raise ValueError("scorecard_options_require_scorecard")
        result = preflight(manifest, tuple(args.targets))
        if args.weekly or args.monthly:
            if (
                args.daily
                or (args.weekly and args.monthly)
                or args.checks
                or args.invoke_check
                or args.checkout is None
                or len(args.targets) != 1
            ):
                raise ValueError("weekly requires explicit target and checkout")
            weekly = (run_monthly if args.monthly else run_weekly)(
                manifest,
                {args.targets[0]: args.checkout},
                BoundedProbe(),
                target_ids=tuple(args.targets),
            )
            sys.stdout.write(weekly.model_dump_json() + "\n")
            return {"READY": 0, "NOT_READY": 1, "UNKNOWN": 4}[weekly.readiness]
        if args.daily:
            if args.checkout is None or len(args.targets) != 1 or args.invoke_check:
                raise ValueError("daily requires explicit target and checkout")
            report = run_readiness(
                manifest,
                {args.targets[0]: args.checkout},
                BoundedProbe(),
                target_ids=tuple(args.targets),
                check_ids=tuple(args.checks),
            )
            sys.stdout.write(report.model_dump_json() + "\n")
            return {"READY": 0, "NOT_READY": 1, "UNKNOWN": 4}[report.readiness]
        if args.checks:
            raise ValueError("check selection requires daily")
        if args.checkout is not None or args.invoke_check is not None:
            if (
                args.checkout is None
                or args.invoke_check is None
                or len(args.targets) != 1
            ):
                raise ValueError("explicit checkout/check/target required")
            target = next(
                target for target in manifest.targets if target.id == args.targets[0]
            )
            consumer = invoke_readiness(
                target, args.checkout, args.invoke_check, trusted=True
            )
            sys.stdout.write(
                json.dumps(
                    {
                        "target_id": target.id,
                        "owning_repository": target.owning_repository,
                        "tested_revision": target.revision,
                        "check_id": args.invoke_check,
                        "result": consumer.model_dump(mode="json"),
                    }
                )
                + "\n"
            )
            return {
                "passed": 0,
                "failed": 1,
                "unavailable": 4,
                "skipped": 3,
                "not_applicable": 3,
            }[consumer.readiness_state]
    except OSError, ValueError:
        error: dict[str, str] = {"contract_state": "failed", "code": "invalid_input"}
        if args.scorecard:
            error["next_action"] = (
                "Check readable inputs, timezone-aware --as-of, matching manifest, "
                "complete native report contracts and nonconflicting evidence; "
                "the existing history was retained."
            )
        sys.stdout.write(json.dumps(error))
        sys.stdout.write("\n")
        return 2
    sys.stdout.write(result.model_dump_json() + "\n")
    return 0
