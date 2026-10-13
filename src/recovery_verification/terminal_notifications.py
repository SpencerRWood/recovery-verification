"""One durable, evidence-derived broker event for each terminal Dagster run."""

import fcntl
import hashlib
import json
import os
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast
from uuid import NAMESPACE_URL, UUID, uuid5

from dagster import DagsterEventType, DagsterInstance, RunStatusChangeRecordsFilter
from dagster._core.storage.dagster_run import DagsterRun
from pydantic import AwareDatetime, Field

from recovery_verification.contract import ContractModel, parse_manifest
from recovery_verification.execution import write_state
from recovery_verification.history import read_bounded, read_history
from recovery_verification.notifications import EventReceipt, send_event
from recovery_verification.orchestration import JOBS, OrchestrationConfig
from recovery_verification.scorecard import (
    History,
    Scorecard,
    TargetScore,
    build_scorecard,
    canonical,
    ensure_no_runtime_secrets,
    reference,
)

TERMINAL_EVENTS: dict[
    str,
    Literal[
        DagsterEventType.RUN_SUCCESS,
        DagsterEventType.RUN_FAILURE,
        DagsterEventType.RUN_CANCELED,
    ],
] = {
    "SUCCESS": DagsterEventType.RUN_SUCCESS,
    "FAILURE": DagsterEventType.RUN_FAILURE,
    "CANCELED": DagsterEventType.RUN_CANCELED,
}
PROVIDER_JOBS = (
    *JOBS.values(),
    "recovery_scorecard",
    "contract_preflight_job",
    "runtime_smoke_job",
    "__ASSET_JOB",
)
TITLE = "Backup & Recovery Scorecard"


def brief(values: list[str] | tuple[str, ...], limit: int) -> str:
    text = "; ".join(values) or "none"
    return text if len(text) <= limit else text[:limit] + " (more in evidence)"


class TerminalOutbox(ContractModel):
    schema_version: Literal["1"] = "1"
    start_time: AwareDatetime
    cursors: dict[str, int] = Field(default_factory=dict)
    pending: dict[str, dict[str, Any]] = Field(default_factory=dict)
    receipts: dict[str, EventReceipt] = Field(default_factory=dict)


def read_outbox(path: Path, start_time: AwareDatetime) -> TerminalOutbox:
    if not path.exists():
        return TerminalOutbox(start_time=start_time)
    document = json.loads(read_bounded(path))
    state = TerminalOutbox.model_validate_json(json.dumps(document["state"]))
    digest = hashlib.sha256(canonical(state).encode()).hexdigest()
    if set(document) != {"state", "sha256"} or document["sha256"] != digest:
        raise ValueError("invalid_terminal_notification_journal")
    if state.start_time != start_time:
        raise ValueError("notification_start_time_changed_requires_review")
    ensure_no_runtime_secrets(canonical(state))
    return state


def backup_summary(target: TargetScore) -> tuple[str, str, str]:
    daily, weekly = target.tiers[:2]
    attempt = daily.latest_attempt
    backups = (
        [c for c in attempt.checks if c.check_id.startswith("backup_")]
        if attempt
        else []
    )
    status = (
        "FAILED"
        if any(c.state == "failed" for c in backups)
        else "PASSED"
        if backups
        and all(c.state == "passed" for c in backups)
        and daily.status == "passed"
        else "UNAVAILABLE"
    )
    freshness = (
        "STALE"
        if any(c.reason == "backup_stale" for c in backups)
        else "CURRENT"
        if status == "PASSED"
        else "UNAVAILABLE"
    )
    verified = weekly.latest_attempt
    real = (
        [
            b
            for b in verified.provenance.backups
            if b.kind == "recovery_backup" and b.artifact_sha256 and b.timestamp
        ]
        if verified
        else []
    )
    checks = (
        cast("list[dict[str, Any]]", verified.consumer_evidence.get("checks", []))
        if verified
        else []
    )
    expected = (
        {
            b.source_id
            for b in verified.provenance.backups
            if b.kind == "recovery_backup"
        }
        if verified
        else set()
    )
    integrity = (
        "VERIFIED"
        if real
        and {b.source_id for b in real} == expected
        and weekly.status == "passed"
        and any(
            c.get("phase") == "integrity" and c.get("state") == "passed" for c in checks
        )
        else "UNAVAILABLE"
    )
    return status, freshness, integrity


def scorecard_for_run(
    config: OrchestrationConfig, run: DagsterRun, terminal_at: datetime
) -> Scorecard:
    ops = cast("dict[str, Any]", run.run_config).get("ops", {})
    inputs = ops.get("recovery_readiness", ops.get("contract_preflight", {})).get(
        "config", {}
    )
    manifest = parse_manifest(inputs.get("manifest_json", config.manifest_json))
    archive = Path(config.state_directory) / "history.json"
    history = read_history(archive) if archive.exists() else History()
    # A later run must not make this run's notification appear more ready.
    history = History(
        entries=tuple(e for e in history.entries if e.report.timestamp <= terminal_at)
    )
    return build_scorecard(manifest, history, now=terminal_at, policy=config.policy)


def terminal_event(
    config: OrchestrationConfig,
    run: DagsterRun,
    status: str,
    terminal_at: datetime,
) -> dict[str, Any]:
    if status not in TERMINAL_EVENTS:
        raise ValueError("terminal_run_status_required")
    identity = uuid5(NAMESPACE_URL, "recovery-verification:terminal:" + run.run_id)
    link = (
        (config.notification_dagster_url or "").rstrip("/")
        + "/runs/"
        + str(UUID(run.run_id))
    )
    lines = [
        f"Dagster: {run.job_name} — {status}",
        f"Run ID: {run.run_id}",
        f"Environment: {config.environment}",
    ]
    score: Scorecard | None = None
    unavailable: str | None = None
    targets: list[dict[str, Any]] = []
    try:
        score = scorecard_for_run(config, run, terminal_at)
    except OSError, ValueError:
        unavailable = "recovery_evidence_unavailable"
    lines.append("Recovery readiness: " + (score.readiness if score else "UNKNOWN"))
    lines.append(
        "Full recovery verified: "
        + ("yes" if score and score.full_recovery_verified else "no")
    )
    if score:
        for target in score.targets:
            backup, freshness, integrity = backup_summary(target)
            targets.append(
                {
                    "target": target.target_id,
                    "backup_status": backup,
                    "backup_freshness": freshness,
                    "backup_integrity": integrity,
                    "recovery_readiness": target.readiness,
                    "verification": {t.tier: t.status for t in target.tiers},
                    "reasons": list(target.reasons),
                    "prerequisites": list(target.next_actions),
                }
            )
            lines.extend(
                [
                    f"\n{target.target_id}: backup {backup}; "
                    f"recovery {target.readiness}",
                    f"Freshness: {freshness}; integrity: {integrity}",
                    "Daily / weekly / monthly: "
                    + " / ".join(t.status.upper() for t in target.tiers),
                    "Evidence times: "
                    + " / ".join(
                        t.latest_attempt.evidence_timestamp.isoformat()
                        if t.latest_attempt
                        else "missing"
                        for t in target.tiers
                    ),
                ]
            )
            for tier in target.tiers:
                attempt = tier.latest_attempt
                if attempt and any(
                    b.kind == "representative_fixture"
                    for b in attempt.provenance.backups
                ):
                    lines.append(tier.tier + ": representative fixture evidence only")
            reasons = list(
                dict.fromkeys(
                    [*target.reasons, *(r for t in target.tiers for r in t.reasons)]
                )
            )
            lines.append("Failed or unavailable: " + brief(reasons, 350))
            lines.append("Prerequisites: " + brief(target.next_actions, 180))
    else:
        for target_id in sorted(config.checkouts):
            lines.append(f"\n{target_id}: backup UNAVAILABLE; recovery UNKNOWN")
            lines.append(
                "Freshness / integrity / daily / weekly / monthly: UNAVAILABLE"
            )
        lines.append("Failed or unavailable: " + str(unavailable))
        lines.append("Prerequisites: restore readable verification evidence")
    if status != "SUCCESS":
        lines.append("Execution failed or canceled; inspect the Dagster run evidence.")
    lines.extend(
        [
            "\nDagster execution status does not establish recovery readiness.",
            "Independent key custody: not attested by this run; "
            "separate custody evidence is required.",
            "Evidence: " + link,
        ]
    )
    message = "\n".join(lines)
    if len(message) > 3400:
        message = (
            message[:2800]
            + "\nAdditional checks omitted; inspect linked evidence."
            + "\nIndependent key custody: not attested by this run; "
            "separate custody evidence is required.\nEvidence: " + link
        )
    event = {
        "schema_version": "1",
        "event_id": str(identity),
        "event_type": "recovery.run-completed",
        "source": "recovery-verification",
        "severity": "error"
        if status == "FAILURE" or (score and score.readiness == "NOT_READY")
        else "success"
        if status == "SUCCESS" and score and score.readiness == "READY"
        else "warning",
        "occurred_at": terminal_at.isoformat(),
        "correlation_id": run.run_id,
        "causation_id": None,
        "idempotency_key": "recovery-run-" + run.run_id,
        "metadata": {},
        "data": {
            "title": TITLE,
            "message": message,
            "dagster_job": run.job_name,
            "execution_status": status,
            "environment": config.environment,
            "recovery_readiness": score.readiness if score else "UNKNOWN",
            "full_recovery_verified": bool(score and score.full_recovery_verified),
            "evidence_link": link,
            "scorecard_reference": reference(score) if score else None,
            "unavailable_reason": unavailable,
            "targets": targets,
        },
    }
    ensure_no_runtime_secrets(json.dumps(event))
    return event


def process_terminal_runs(
    config: OrchestrationConfig,
    instance: DagsterInstance,
    sender: Callable[[dict[str, Any]], EventReceipt | None] = send_event,
) -> TerminalOutbox:
    if not config.notifications_enabled or config.notification_start_time is None:
        raise ValueError("terminal_notifications_not_enabled")
    root = Path(config.state_directory)
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if root.is_symlink():
        raise ValueError("unsafe_terminal_notification_directory")
    path = root / "terminal-notifications.json"
    descriptor = os.open(
        root / "terminal-notifications.lock", os.O_CREAT | os.O_RDWR, 0o600
    )
    with os.fdopen(descriptor, "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state = read_outbox(path, config.notification_start_time)
        delivery_budget = 2

        def flush() -> None:
            nonlocal delivery_budget
            for key, event in tuple(state.pending.items()):
                if delivery_budget == 0:
                    break
                delivery_budget -= 1
                receipt = sender(event)
                if receipt is not None:
                    if (
                        str(receipt.record_id) != key
                        or str(receipt.correlation_id) != event["correlation_id"]
                    ):
                        raise ValueError("terminal_notification_receipt_mismatch")
                    state.receipts[key] = receipt
                    del state.pending[key]
                    write_state(path, state)

        # A recovered broker can drain a full outbox before scanning more events.
        flush()
        for status, event_type in TERMINAL_EVENTS.items():
            records = instance.fetch_run_status_changes(
                RunStatusChangeRecordsFilter(
                    event_type,
                    after_timestamp=state.start_time.timestamp(),
                    after_storage_id=state.cursors.get(status, 0),
                ),
                limit=5,
                ascending=True,
            )
            for record in records.records:
                run = instance.get_run_by_id(record.run_id)
                if run is None:
                    raise ValueError("terminal_run_record_unavailable")
                origin = run.remote_job_origin
                own_location = (
                    origin.repository_origin.code_location_origin.location_name
                    == config.notification_code_location
                    if origin
                    else run.tags.get("recovery/environment") == config.environment
                )
                if own_location and run.job_name in PROVIDER_JOBS:
                    event = terminal_event(
                        config,
                        run,
                        status,
                        datetime.fromtimestamp(record.timestamp, UTC),
                    )
                    key = str(event["event_id"])
                    if key not in state.pending and key not in state.receipts:
                        if len(state.pending) >= 100 or len(state.receipts) >= 10000:
                            raise ValueError("terminal_notification_retention_limit")
                        state.pending[key] = event
                state.cursors[status] = record.storage_id
                # Enqueue and advance atomically before any network request.
                write_state(path, state)
        flush()
        return state
