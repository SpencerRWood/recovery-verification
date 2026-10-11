"""Run-wide exclusion, durable attempts and a restart-safe broker outbox."""

import fcntl
import hashlib
import json
import os
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from time import sleep
from typing import Any, Literal, cast

from pydantic import AwareDatetime, Field

from recovery_verification.contract import ContractModel, Level, Manifest
from recovery_verification.history import read_bounded, read_history, write_history
from recovery_verification.monthly import MonthlyReport
from recovery_verification.notifications import notification_events, send_event
from recovery_verification.orchestration import OrchestrationConfig
from recovery_verification.probes import BoundedProbe
from recovery_verification.readiness import (
    Observation,
    ReadinessCheck,
    ReadinessReport,
    manifest_hash,
    readiness_state,
)
from recovery_verification.scorecard import (
    History,
    Scorecard,
    TargetScore,
    _expected_daily,
    build_scorecard,
    canonical,
    capture_evidence,
    ensure_no_runtime_secrets,
    reference,
)
from recovery_verification.weekly import WeeklyReport


class AttemptJournal(ContractModel):
    manifest_reference: str
    requested_tier: Level | None = None
    target_ids: tuple[str, ...] = ()
    interrupted_evidence_reference: str | None = None
    started_at: AwareDatetime = Field(default_factory=lambda: datetime.now(UTC))
    finished_at: AwareDatetime | None = None
    state: Literal["running", "interrupted", "complete"] = "running"
    reports: dict[Level, str] = Field(default_factory=dict)


class ExecutionState(ContractModel):
    schema_version: Literal["1"] = "1"
    attempts: dict[str, AttemptJournal] = Field(default_factory=dict)
    previous_scorecard: str | None = None
    pending_events: tuple[dict[str, object], ...] = ()
    notification_state: Literal["disabled", "acknowledged", "unavailable"] = "disabled"


def condition(target: TargetScore) -> tuple[object, ...]:
    return (
        target.readiness,
        target.full_recovery_verified,
        target.strongest_current_level,
        tuple(
            (
                tier.status,
                tier.reasons,
                tuple(
                    (d.check_id, d.state, d.reason, d.failure_category)
                    for d in tier.latest_attempt.diagnostics
                )
                if tier.latest_attempt
                else (),
            )
            for tier in target.tiers
        ),
    )


def interrupted_evidence(
    manifest: Manifest, attempt: AttemptJournal
) -> tuple[Level, str]:
    """Unavailable attempt observations are never represented as executed checks."""
    tier = attempt.requested_tier or "readiness"
    selected = tuple(t for t in manifest.targets if t.id in attempt.target_ids)
    observed_at = attempt.finished_at or datetime.now(UTC)
    daily = (
        ReadinessReport.model_validate_json(attempt.reports["readiness"])
        if "readiness" in attempt.reports
        else None
    )
    checks = list(daily.checks) if daily and tier != "readiness" else []
    for target in selected:
        names = set() if daily and tier != "readiness" else _expected_daily(target)
        if tier == "verification" or (
            tier == "drill" and not target.ephemeral_rebuild.supported
        ):
            names.update(
                "weekly_" + c.id
                for c in target.validation_commands
                if "verification" in c.levels
            )
        if tier == "drill":
            names.update(
                "monthly_" + c.id
                for c in target.validation_commands
                if "drill" in c.levels
            )
            if not target.ephemeral_rebuild.supported:
                names.add("monthly_capability")
        checks.extend(
            ReadinessCheck(
                target_id=target.id,
                check_id=name,
                tested_revision=target.revision,
                timestamp=observed_at,
                duration_seconds=0.0,
                state="unavailable",
                reason="operational_attempt_interrupted",
                category="verification_infrastructure",
            )
            for name in sorted(names)
        )
    fields: dict[str, Any] = {
        "manifest_sha256": manifest_hash(manifest),
        "timestamp": observed_at,
        "checks": tuple(checks),
        "target_revisions": {t.id: t.revision for t in selected},
        "readiness": readiness_state(tuple(checks)),
        "provenance": daily.provenance if daily else {},
    }
    report: ReadinessReport
    if tier == "verification":
        report = WeeklyReport(**fields, consumer_evidence={})
    elif tier == "drill":
        report = MonthlyReport(**fields, drill_evidence={}, fallback_evidence={})
    else:
        report = ReadinessReport(**fields)
    return tier, report.model_dump_json()


def _digest(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


def read_state(path: Path) -> ExecutionState:
    if not path.exists():
        return ExecutionState()
    document = json.loads(read_bounded(path))
    state = ExecutionState.model_validate_json(json.dumps(document["state"]))
    if set(document) != {"state", "sha256"} or document["sha256"] != _digest(
        canonical(state)
    ):
        raise ValueError("invalid_execution_journal")
    ensure_no_runtime_secrets(canonical(state))
    return state


def write_state(path: Path, state: ExecutionState) -> None:
    """Atomic/fsynced state; never expose raw exception or response text."""
    raw = json.dumps(
        {"state": state.model_dump(mode="json"), "sha256": _digest(canonical(state))},
        sort_keys=True,
    )
    ensure_no_runtime_secrets(raw)
    if len(raw.encode()) > 10 * 1024 * 1024:
        raise ValueError("execution_journal_limit")
    descriptor, temporary = tempfile.mkstemp(
        prefix=".recovery-execution-", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "w") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        Path(temporary).replace(path)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        Path(temporary).unlink(missing_ok=True)


class RetryingProbe:
    """Only retry unavailable infrastructure observations; confirmed failures stand."""

    def __init__(
        self, attempts: int, probe: Callable[..., Observation] | None = None
    ) -> None:
        self.attempts = attempts
        self.probe = probe or BoundedProbe()

    def __call__(
        self, kind: str, reference: str, root: Path, max_age: int | None
    ) -> Observation:
        for attempt in range(self.attempts):
            observed = self.probe(kind, reference, root, max_age)
            if (
                observed.state != "unavailable"
                or observed.category != "verification_infrastructure"
            ):
                return observed
            if attempt + 1 < self.attempts:
                sleep(0.2)
        return observed


class Execution:
    def __init__(
        self,
        config: OrchestrationConfig,
        key: str,
        manifest: Manifest,
        requested_tier: Level | None = None,
        target_ids: tuple[str, ...] = (),
    ) -> None:
        self.config = config
        self.manifest = manifest
        self.key = _digest(key)
        self.path = Path(config.state_directory) / "execution.json"
        self.archive = Path(config.state_directory) / "history.json"
        self.state = read_state(self.path)
        prior = self.state.attempts.get(self.key)
        if prior and prior.manifest_reference != reference(manifest):
            raise ValueError("execution_key_manifest_mismatch")
        selected = target_ids or tuple(t.id for t in manifest.targets)
        if prior and (
            prior.target_ids != selected or prior.requested_tier != requested_tier
        ):
            raise ValueError("execution_key_selection_mismatch")
        self.attempt = prior or AttemptJournal(
            manifest_reference=reference(manifest),
            requested_tier=requested_tier,
            target_ids=selected,
        )
        # A running record after acquiring the exclusive lock was interrupted.
        attempts = {
            k: self.record_interruption(
                a.model_copy(
                    update={"state": "interrupted", "finished_at": datetime.now(UTC)}
                )
            )
            if a.state == "running"
            else a
            for k, a in self.state.attempts.items()
        }
        attempts[self.key] = self.attempt.model_copy(update={"state": "running"})
        self.state = self.state.model_copy(update={"attempts": attempts})
        write_state(self.path, self.state)

    def record_interruption(self, attempt: AttemptJournal) -> AttemptJournal:
        if (
            attempt.requested_tier
            and attempt.requested_tier not in attempt.reports
            and attempt.manifest_reference == reference(self.manifest)
        ):
            tier, raw = interrupted_evidence(self.manifest, attempt)
            entry = capture_evidence(self.manifest, tier, raw)
            write_history(self.archive, History(entries=(entry,)))
            return attempt.model_copy(
                update={"interrupted_evidence_reference": reference(entry)}
            )
        return attempt

    def report(self, tier: Level, run: Callable[[], str]) -> str:
        raw = self.attempt.reports[tier] if tier in self.attempt.reports else run()
        entry = capture_evidence(self.manifest, tier, raw)
        # Archive first: a crash before journaling can repeat a safe operation,
        # but cannot erase previous success or claim an unobserved success.
        write_history(self.archive, History(entries=(entry,)))
        self.attempt = self.attempt.model_copy(
            update={"reports": {**self.attempt.reports, tier: raw}}
        )
        self.state = self.state.model_copy(
            update={"attempts": {**self.state.attempts, self.key: self.attempt}}
        )
        write_state(self.path, self.state)
        return raw

    def finish(
        self,
        *,
        interrupted: bool = False,
        sender: Callable[[dict[str, object]], bool] = send_event,
    ) -> None:
        self.attempt = self.attempt.model_copy(
            update={
                "state": "interrupted" if interrupted else "complete",
                "finished_at": datetime.now(UTC),
            }
        )
        if interrupted:
            self.attempt = self.record_interruption(self.attempt)
        self.state = self.state.model_copy(
            update={"attempts": {**self.state.attempts, self.key: self.attempt}}
        )
        if self.archive.exists():
            current = build_scorecard(
                self.manifest,
                read_history(self.archive),
                now=datetime.now(UTC),
                policy=self.config.policy,
            )
            prior = (
                Scorecard.model_validate_json(self.state.previous_scorecard)
                if self.state.previous_scorecard
                else None
            )
            pending = list(self.state.pending_events)
            if self.config.notifications_enabled:
                old = {t.target_id: t for t in prior.targets} if prior else {}
                new = {t.target_id: t for t in current.targets}
                for event in notification_events(current, prior):
                    target_id = str(cast("dict[str, Any]", event["data"])["target"])
                    before, after = old.get(target_id), new[target_id]
                    # Scheduled attempts with identical conditions stay quiet;
                    # pending events keep their original IDs across restarts.
                    if before and condition(before) == condition(after):
                        continue
                    data = dict(cast("dict[str, Any]", event["data"]))
                    data["environment"] = self.config.environment
                    enriched = {**event, "data": data}
                    if not any(p["event_id"] == enriched["event_id"] for p in pending):
                        pending.append(enriched)
            if len(pending) > 100:
                raise ValueError("notification_outbox_limit")
            self.state = self.state.model_copy(
                update={
                    "previous_scorecard": canonical(current),
                    "pending_events": tuple(pending),
                }
            )
        write_state(self.path, self.state)
        if self.config.notifications_enabled:
            remaining = list(self.state.pending_events)
            for event in self.state.pending_events:
                if sender(event):
                    remaining.remove(event)
                    self.state = self.state.model_copy(
                        update={"pending_events": tuple(remaining)}
                    )
                    write_state(self.path, self.state)
            self.state = self.state.model_copy(
                update={
                    "notification_state": "unavailable" if remaining else "acknowledged"
                }
            )
            write_state(self.path, self.state)


@contextmanager
def execution_session(
    config: OrchestrationConfig,
    key: str,
    manifest: Manifest,
    expected_tier: Level | None = None,
    target_ids: tuple[str, ...] = (),
) -> Iterator[Execution]:
    root = Path(config.state_directory)
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if root.is_symlink():
        raise ValueError("unsafe_execution_state_directory")
    descriptor = os.open(root / "operation.lock", os.O_CREAT | os.O_RDWR, 0o600)
    with os.fdopen(descriptor, "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("recovery_operation_already_running") from None
        execution = Execution(
            config, key, manifest, requested_tier=expected_tier, target_ids=target_ids
        )
        try:
            yield execution
        except BaseException:
            execution.finish(interrupted=True)
            raise
        else:
            execution.finish(
                interrupted=bool(
                    expected_tier and expected_tier not in execution.attempt.reports
                )
            )
