"""Verify actual consumer source snapshots through the public generic interface.

Temporary Git commits below are test fixtures, not delivery commits. Only Git
source inputs are copied; ignored runtime files, credentials and .venv are excluded.
"""

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from recovery_verification.contract import parse_manifest
from recovery_verification.invocation import (
    invoke_drill,
    invoke_readiness,
    invoke_verification,
)
from recovery_verification.monthly import extend_monthly
from recovery_verification.readiness import (
    Observation,
    ReadinessCheck,
    readiness_state,
    run_readiness,
)
from recovery_verification.scorecard import History, build_scorecard, capture_evidence
from recovery_verification.weekly import WeeklyReport


def git(root: Path, *args: str) -> str:
    result = subprocess.run(  # noqa: S603
        ["git", *args],  # noqa: S607 -- The operator's Git is a required test tool.
        cwd=root,
        capture_output=True,
        text=True,
        timeout=15,
        check=True,
    )
    return result.stdout


def verify(  # noqa: PLR0912, PLR0915 -- Explicit local consumer and provenance acceptance gates.
    source: Path, snapshot: Path, *, weekly: bool = False, monthly: bool = False
) -> dict[str, object]:
    names = git(source, "ls-files", "--cached", "--others", "--exclude-standard", "-z")
    for name in set(names.split("\x00")) - {""}:
        path = source / name
        if not path.exists() and not path.is_symlink():
            continue
        destination = snapshot / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, destination, follow_symlinks=False)
    declaration = json.loads((snapshot / "recovery/consumer.v1.json").read_text())
    owner = declaration["target"]["owning_repository"]
    for args in (
        ("init", "-q"),
        ("config", "user.name", "Consumer verification fixture"),
        ("config", "user.email", "fixture@example.invalid"),
        ("remote", "add", "origin", f"https://github.com/{owner}.git"),
        ("add", "."),
        ("commit", "-qm", "test: disposable consumer source snapshot"),
    ):
        git(snapshot, *args)
    # Reuse each consumer's declared development tool environment without copying it.
    tool_path = source / ".venv/bin"
    previous_path = os.environ.get("PATH", os.defpath)
    os.environ["PATH"] = str(tool_path) + os.pathsep + previous_path
    try:
        export = subprocess.run(  # noqa: S603
            [str(snapshot / "scripts/recovery.py"), "manifest"],
            cwd=snapshot,
            capture_output=True,
            text=True,
            check=True,
            timeout=15,
        )
        manifest = parse_manifest(export.stdout)
        target = manifest.targets[0]
        if weekly and "verification" not in target.allowed_verification_levels:
            raise ValueError("verification_capability_required")
        result = invoke_readiness(target, snapshot, "consumer_preflight", trusted=True)
        verification = (
            invoke_verification(target, snapshot, "consumer_weekly", trusted=True)
            if (weekly or monthly)
            and "verification" in target.allowed_verification_levels
            else None
        )
        drills = [
            invoke_drill(target, snapshot, command.id, trusted=True)
            for command in target.validation_commands
            if monthly and "drill" in command.levels
        ]
        scorecard = None
        monthly_report = None
        if (
            result.provenance is None
            or not result.provenance.playbooks
            or not result.provenance.inventories
        ):
            raise ValueError("consumer_preflight_provenance_required")
        if verification:
            if (
                verification.provenance is None
                or not verification.provenance.playbooks
                or verification.provenance.inline_inventory != "localhost"
            ):
                raise ValueError("consumer_weekly_provenance_required")
            if not any(
                b.kind == "representative_fixture" and b.artifact_sha256
                for b in verification.provenance.backups
            ):
                raise ValueError("consumer_restore_provenance_required")
            # External runtime prerequisites are explicitly unavailable in this
            # local contract check; no production readiness is manufactured.
            daily = run_readiness(
                manifest,
                {target.id: snapshot},
                lambda *_: Observation(
                    state="unavailable",
                    reason="local_contract_scope",
                    category="verification_infrastructure",
                ),
            )
            if any(check.state == "failed" for check in daily.checks):
                raise ValueError("consumer_native_readiness_failed")
            check = ReadinessCheck(
                target_id=target.id,
                check_id="weekly_consumer_weekly",
                tested_revision=target.revision,
                timestamp=datetime.now(UTC),
                duration_seconds=0,
                state=verification.readiness_state,
                reason="consumer_executable_verification",
                provenance=verification.provenance,
            )
            checks = (*daily.checks, check)
            report = WeeklyReport(
                **daily.model_dump(exclude={"checks", "timestamp", "readiness"}),
                timestamp=datetime.now(UTC),
                checks=checks,
                readiness=readiness_state(checks),
                consumer_evidence={f"{target.id}/consumer_weekly": verification},
            )
            if monthly:
                monthly_report = extend_monthly(
                    manifest, {target.id: snapshot}, daily, target_ids=(target.id,)
                )
            scorecard = build_scorecard(
                manifest,
                History(
                    entries=(
                        capture_evidence(
                            manifest,
                            "drill" if monthly_report else "verification",
                            monthly_report.model_dump_json()
                            if monthly_report
                            else report.model_dump_json(),
                        ),
                    )
                ),
                now=datetime.now(UTC),
            )
            latest = scorecard.targets[0].tiers[1].latest_attempt
            if (
                latest is None
                or latest.provenance
                != verification.provenance.model_copy(
                    update={"backups": latest.provenance.backups}
                )
            ):
                raise ValueError("consumer_scorecard_provenance_mismatch")
    finally:
        os.environ["PATH"] = previous_path
    local = [check for check in result.checks if check.id != "external_prerequisites"]
    if not local or any(check.state != "passed" for check in local):
        raise ValueError("consumer_local_preflight_failed")
    if result.readiness_state != "passed" or any(
        check.id == "external_prerequisites" for check in result.checks
    ):
        raise ValueError("consumer_local_scope_mismatch")
    if verification is not None and verification.readiness_state != "passed":
        raise ValueError("consumer_executable_verification_failed")
    if any(drill.readiness_state != "passed" for drill in drills):
        raise ValueError("consumer_isolated_drill_failed")
    return {
        "repository": owner,
        "source_revision": git(source, "rev-parse", "HEAD").strip(),
        "test_snapshot_revision": target.revision,
        "source_changes_included": bool(git(source, "status", "--porcelain")),
        "checks": [check.model_dump() for check in result.checks],
        "readiness_state": result.readiness_state,
        "scope": "local_configuration_and_representative_recovery"
        if weekly or monthly
        else "local_configuration_only",
        "weekly": verification.model_dump(mode="json") if verification else None,
        "preflight_provenance": result.provenance.model_dump(mode="json"),
        "scorecard": scorecard.model_dump(mode="json") if scorecard else None,
        "monthly": monthly_report.model_dump(mode="json") if monthly_report else None,
        "ephemeral_rebuild": target.ephemeral_rebuild.model_dump() if monthly else None,
        "drills": [drill.model_dump(mode="json") for drill in drills],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("checkout", type=Path, nargs="*")
    parser.add_argument("--weekly", action="store_true")
    parser.add_argument("--monthly", action="store_true")
    parser.add_argument("--configured-checkouts", action="store_true")
    args = parser.parse_args()
    results = []
    previous_alarm = signal.getsignal(signal.SIGALRM)

    def timed_out(_signum: int, _frame: object) -> None:
        raise TimeoutError("local_verification_deadline")

    signal.signal(signal.SIGALRM, timed_out)
    signal.alarm(40)
    try:
        if args.weekly and args.monthly:
            raise ValueError("ambiguous_verification_level")
        if args.configured_checkouts:
            if args.checkout:
                raise ValueError("ambiguous_consumer_checkouts")
            paths = json.loads(os.environ["RECOVERY_CONSUMER_CHECKOUTS"])
            if (
                not isinstance(paths, list)
                or len(paths) != 2
                or not all(isinstance(path, str) and path for path in paths)
            ):
                raise ValueError("invalid_consumer_checkouts")
            args.checkout = [Path(path) for path in paths]
        if not args.checkout:
            raise ValueError("consumer_checkouts_required")
        with tempfile.TemporaryDirectory(
            prefix="recovery-consumer-fixtures-"
        ) as directory:
            for index, source in enumerate(args.checkout):
                snapshot = Path(directory).resolve() / str(index)
                snapshot.mkdir()
                results.append(
                    verify(
                        source.resolve(),
                        snapshot,
                        weekly=args.weekly,
                        monthly=args.monthly,
                    )
                )
    except OSError, ValueError, KeyError, subprocess.SubprocessError:
        sys.stdout.write(
            json.dumps({"state": "failed", "reason": "consumer_verification_failed"})
            + "\n"
        )
        return 1
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous_alarm)
    sys.stdout.write(
        json.dumps({"state": "passed", "consumers": results}, sort_keys=True) + "\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
