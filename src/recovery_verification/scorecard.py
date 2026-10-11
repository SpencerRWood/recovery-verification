"""Deterministic, revision-bound scorecards over retained native tier reports."""

import json
import os
import re
from datetime import datetime
from hashlib import sha256
from typing import Annotated, Literal

from pydantic import Field

from recovery_verification.contract import (
    Command,
    ContractModel,
    Identifier,
    Level,
    Manifest,
    Target,
    _reject_duplicate_keys,
)
from recovery_verification.models import (
    ConsumerDrillResult,
    ConsumerVerificationResult,
    State,
)
from recovery_verification.monthly import MonthlyReport
from recovery_verification.provenance import BackupProvenance, ExecutionProvenance
from recovery_verification.readiness import (
    Category,
    ReadinessCheck,
    ReadinessReport,
    assess_currency,
    manifest_hash,
    readiness_state,
)
from recovery_verification.weekly import WeeklyReport

Readiness = Literal["READY", "DEGRADED", "NOT_READY", "UNKNOWN"]
TierStatus = Literal[
    "passed", "failed", "unavailable", "missing", "expired", "superseded", "unsupported"
]
LEVELS: tuple[Level, ...] = ("readiness", "verification", "drill")


class ConsumerIdentity(ContractModel):
    owning_repository: Annotated[
        str, Field(pattern=r"^[A-Za-z0-9_-]+/[A-Za-z0-9_.-]+$")
    ]
    bootstrap_entrypoint: str
    recovery_entrypoint: str
    backup_source_ids: tuple[Identifier, ...] = ()


class CapturedEvidence(ContractModel):
    consumers: dict[Identifier, ConsumerIdentity]


class DailyEvidence(CapturedEvidence):
    tier: Literal["readiness"]
    report: ReadinessReport


class WeeklyEvidence(CapturedEvidence):
    tier: Literal["verification"]
    report: WeeklyReport


class MonthlyEvidence(CapturedEvidence):
    tier: Literal["drill"]
    report: MonthlyReport


Entry = Annotated[
    DailyEvidence | WeeklyEvidence | MonthlyEvidence, Field(discriminator="tier")
]


class History(ContractModel):
    """Append-only logical history; no retention pruning or raw execution logs."""

    schema_version: Literal["1"] = "1"
    entries: tuple[Entry, ...] = ()


class ScorecardPolicy(ContractModel):
    """Defaults follow daily/weekly/monthly cadences; target overrides win."""

    readiness_max_age_seconds: Annotated[int, Field(ge=1)] = 86400
    verification_max_age_seconds: Annotated[int, Field(ge=1)] = 604800
    drill_max_age_seconds: Annotated[int, Field(ge=1)] = 2592000
    upstream_targets: dict[Identifier, tuple[Identifier, ...]] = Field(
        default_factory=dict
    )


class CheckDiagnostic(ContractModel):
    check_id: str
    state: State
    reason: str
    failure_category: Category


class Attempt(ContractModel):
    """Safe provenance projected from a native report, never manifest arguments."""

    evidence_reference: Annotated[str, Field(pattern=r"^sha256:[0-9a-f]{64}$")]
    source_tier: Level
    target_id: Identifier
    owning_repository: str
    manifest_version: Literal["1"] = "1"
    manifest_sha256: str
    tested_revision: str
    timestamp: datetime
    evidence_timestamp: datetime
    duration_seconds: float
    readiness: Literal["READY", "NOT_READY", "UNKNOWN"]
    currency: Literal["current", "stale", "superseded"]
    checks: tuple[ReadinessCheck, ...]
    consumer_evidence: dict[str, object]
    diagnostics: tuple[CheckDiagnostic, ...]
    bootstrap_entrypoint: str
    recovery_entrypoint: str
    provenance: ExecutionProvenance
    unavailable_provenance: tuple[str, ...]


class TierScore(ContractModel):
    tier: Level
    supported: bool
    status: Literal[
        "passed",
        "failed",
        "unavailable",
        "missing",
        "expired",
        "superseded",
        "unsupported",
    ]
    latest_attempt: Attempt | None
    last_success: Attempt | None
    evidence_age_seconds: float | None
    reasons: tuple[str, ...]
    next_actions: tuple[str, ...]
    historical_failure_count: int
    consecutive_failures: int
    regression: bool
    recurring_failed_checks: tuple[str, ...]


class TargetScore(ContractModel):
    target_id: Identifier
    owning_repository: str
    tested_revision: str
    readiness: Readiness
    full_recovery_verified: bool
    strongest_successful_level: Level | None
    strongest_current_level: Level | None
    tiers: tuple[TierScore, ...]
    upstream_targets: tuple[str, ...]
    reasons: tuple[str, ...]
    next_actions: tuple[str, ...]


class Scorecard(ContractModel):
    schema_version: Literal["1"] = "1"
    observed_at: datetime
    readiness: Readiness
    full_recovery_verified: bool
    history_reference: str
    history_count: int
    targets: tuple[TargetScore, ...]


def canonical(value: ContractModel) -> str:
    return json.dumps(
        value.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
    )


def reference(value: ContractModel) -> str:
    return "sha256:" + sha256(canonical(value).encode()).hexdigest()


def parse_history(raw: str) -> History:
    try:
        ensure_no_runtime_secrets(raw)
        value = json.loads(raw, object_pairs_hook=_reject_duplicate_keys)
        return History.model_validate_json(json.dumps(value))
    except ValueError, RecursionError:
        raise ValueError("invalid_evidence_history") from None


def ensure_no_runtime_secrets(raw: str) -> None:
    """Reject known injected credentials, including escaped JSON representations."""
    for key, value in os.environ.items():
        if (
            value
            and re.search(r"TOKEN|PASSWORD|SECRET|CREDENTIAL|API_KEY", key)
            and (value in raw or json.dumps(value)[1:-1] in raw)
        ):
            raise ValueError("credential_bearing_evidence")


def merge_history(history: History, additions: tuple[Entry, ...]) -> History:
    """Exact duplicates are idempotent; conflicts are rejected during assessment."""
    entries = {reference(entry): entry for entry in (*history.entries, *additions)}
    return History(entries=tuple(entries[key] for key in sorted(entries)))


def capture_evidence(manifest: Manifest, tier: Level, raw: str) -> Entry:
    """Bind native reports to the manifest's consumer identity at ingestion."""
    try:
        ensure_no_runtime_secrets(raw)
        document = json.loads(raw, object_pairs_hook=_reject_duplicate_keys)
        report_types: dict[Level, type[ReadinessReport]] = {
            "readiness": ReadinessReport,
            "verification": WeeklyReport,
            "drill": MonthlyReport,
        }
        report_type = report_types[tier]
        report = report_type.model_validate_json(json.dumps(document))
        if report.manifest_sha256 != manifest_hash(manifest):
            raise ValueError("manifest_mismatch")
        consumers = {
            t.id: ConsumerIdentity(
                owning_repository=t.owning_repository,
                bootstrap_entrypoint=t.entrypoints.bootstrap.entrypoint,
                recovery_entrypoint=t.entrypoints.recovery.entrypoint,
                backup_source_ids=tuple(sorted(b.id for b in t.backup_sources)),
            )
            for t in manifest.targets
            if t.id in report.target_revisions
        }
        entry = parse_history(
            json.dumps(
                {
                    "schema_version": "1",
                    "entries": [
                        {
                            "tier": tier,
                            "consumers": {
                                key: value.model_dump()
                                for key, value in consumers.items()
                            },
                            "report": document,
                        }
                    ],
                }
            )
        ).entries[0]
        _validate_bound_report(entry, manifest)
        return entry
    except ValueError, RecursionError:
        raise ValueError("invalid_native_evidence") from None


def _expected_daily(target: Target) -> set[str]:
    return {
        "manifest_contract",
        "checkout",
        "entrypoints",
        *(
            f"consumer_{c.id}"
            for c in target.validation_commands
            if "readiness" in c.levels
        ),
        *(f"dependency_{d.id}" for d in target.required_dependencies),
        *(f"backup_{b.id}" for b in target.backup_sources),
    }


def _checks(
    report: ReadinessReport, target: Target, tier: Level
) -> tuple[ReadinessCheck, ...]:
    return tuple(
        sorted(
            (
                c
                for c in report.checks
                if c.target_id == target.id
                and (
                    not c.check_id.startswith(("weekly_", "monthly_"))
                    if tier == "readiness"
                    else c.check_id.startswith(
                        "weekly_" if tier == "verification" else "monthly_"
                    )
                )
            ),
            key=lambda c: c.check_id,
        )
    )


def _consumer_sources(
    report: ReadinessReport, tier: Level
) -> dict[str, ConsumerVerificationResult | ConsumerDrillResult]:
    if isinstance(report, WeeklyReport) and tier == "verification":
        return dict(report.consumer_evidence.items())
    if isinstance(report, MonthlyReport):
        if tier == "verification":
            return dict(report.fallback_evidence.items())
        if tier == "drill":
            return dict(report.drill_evidence.items())
    return {}


def _validate_bound_report(entry: Entry, manifest: Manifest) -> None:  # noqa: PLR0912, PLR0915 -- Explicit wire/capability guards.
    report = entry.report
    if set(entry.consumers) != set(report.target_revisions):
        raise ValueError("inconsistent_consumer_identity")
    for consumer_identity in entry.consumers.values():
        for path in (
            consumer_identity.bootstrap_entrypoint,
            consumer_identity.recovery_entrypoint,
        ):
            try:
                Command(entrypoint=path, args=(), timeout_seconds=1)
            except ValueError:
                raise ValueError("incompatible_consumer_identity") from None
    if {c.target_id for c in report.checks} != set(report.target_revisions):
        raise ValueError("inconsistent_target_evidence")
    if any(c.timestamp > report.timestamp for c in report.checks):
        raise ValueError("inconsistent_evidence_timestamp")
    if any(
        backup.timestamp is not None and backup.timestamp > report.timestamp
        for level in LEVELS
        for evidence in _consumer_sources(report, level).values()
        if isinstance(evidence, ConsumerVerificationResult) and evidence.provenance
        for backup in evidence.provenance.backups
    ):
        raise ValueError("inconsistent_consumer_provenance_timestamp")
    if (
        any(c.check_id.startswith("monthly_") for c in report.checks)
        and entry.tier != "drill"
    ):
        raise ValueError("incompatible_verification_tier")
    if (
        any(c.check_id.startswith("weekly_") for c in report.checks)
        and entry.tier == "readiness"
    ):
        raise ValueError("incompatible_verification_tier")
    if report.manifest_sha256 != manifest_hash(manifest):
        # Historical manifests cannot attest to current readiness.
        return
    targets = {t.id: t for t in manifest.targets}
    for identity, revision in report.target_revisions.items():
        target = targets.get(identity)
        if target is None or revision != target.revision:
            raise ValueError("incompatible_target_evidence")
        identity_record = entry.consumers[identity]
        if identity_record != ConsumerIdentity(
            owning_repository=target.owning_repository,
            bootstrap_entrypoint=target.entrypoints.bootstrap.entrypoint,
            recovery_entrypoint=target.entrypoints.recovery.entrypoint,
            backup_source_ids=tuple(sorted(b.id for b in target.backup_sources)),
        ):
            raise ValueError("incompatible_consumer_identity")
        provenance_values = [
            _execution_provenance(report, target, level) for level in LEVELS
        ]
        provenance = ExecutionProvenance(
            backups=tuple(
                {
                    (b.kind, b.source_id): b
                    for p in provenance_values
                    for b in p.backups
                }.values()
            )
        )
        if {
            b.source_id for b in provenance.backups if b.kind == "recovery_backup"
        } - set(identity_record.backup_source_ids):
            raise ValueError("incompatible_backup_provenance")
        if any(
            b.kind == "representative_fixture"
            and b.source_id in identity_record.backup_source_ids
            for b in provenance.backups
        ):
            raise ValueError("ambiguous_fixture_provenance")
        daily = _checks(report, target, "readiness")
        if not _expected_daily(target) <= {c.check_id for c in daily}:
            raise ValueError("incomplete_daily_contract")
        if entry.tier == "readiness" and len(daily) != len(
            [c for c in report.checks if c.target_id == identity]
        ):
            raise ValueError("incompatible_verification_tier")
        tier_prefixes: tuple[tuple[Level, str], ...] = (
            ("verification", "weekly_"),
            ("drill", "monthly_"),
        )
        for tier, prefix in tier_prefixes:
            observations = _checks(report, target, tier)
            if not observations:
                if entry.tier == tier:
                    raise ValueError("missing_tier_evidence")
                continue
            if tier == "drill" and not target.ephemeral_rebuild.supported:
                if (
                    len(observations) != 1
                    or observations[0].check_id != "monthly_capability"
                    or observations[0].state != "unavailable"
                ):
                    raise ValueError("unsupported_drill_success")
                continue
            if tier not in target.allowed_verification_levels:
                raise ValueError("unsupported_verification_evidence")
            commands = {
                prefix + c.id for c in target.validation_commands if tier in c.levels
            }
            if not commands <= {c.check_id for c in observations}:
                raise ValueError("incomplete_execution_contract")
            sources = _consumer_sources(report, tier)
            for check in observations:
                if (
                    check.check_id in commands
                    and check.state == "passed"
                    and f"{identity}/{check.check_id.removeprefix(prefix)}"
                    not in sources
                ):
                    raise ValueError("missing_execution_provenance")
        if isinstance(report, MonthlyReport):
            for key, drill in report.drill_evidence.items():
                if key.split("/")[0] != identity:
                    continue
                if drill.provider != target.ephemeral_rebuild.provider or {
                    s.source_id for s in drill.snapshots
                } - {b.id for b in target.backup_sources}:
                    raise ValueError("incompatible_drill_provenance")
                if drill.ended_at > report.timestamp:
                    raise ValueError("inconsistent_drill_timestamp")
                restored = any(
                    c.phase == "restore" and c.state == "passed" for c in drill.checks
                )
                if restored and {s.source_id for s in drill.snapshots} != {
                    b.id for b in target.backup_sources
                }:
                    raise ValueError("missing_backup_provenance")
                command_id = key.split("/")[1]
                for index, snapshot in enumerate(drill.snapshots):
                    source = next(
                        b for b in target.backup_sources if b.id == snapshot.source_id
                    )
                    fresh = (
                        drill.started_at - snapshot.timestamp
                    ).total_seconds() <= source.max_age_seconds
                    backup_check = next(
                        (
                            c
                            for c in report.checks
                            if c.target_id == identity
                            and c.check_id == f"monthly_backup_{command_id}_{index}"
                        ),
                        None,
                    )
                    if backup_check is None or backup_check.state != (
                        "passed" if fresh else "failed"
                    ):
                        raise ValueError("inconsistent_backup_freshness")


def _attempt(  # noqa: PLR0913, PLR0917 -- Projection needs source, target and currency policy.
    entry: Entry,
    target: Target,
    tier: Level,
    manifest: Manifest,
    now: datetime,
    age: int,
) -> Attempt | None:
    report = entry.report
    checks = _checks(report, target, tier)
    if not checks:
        return None
    projected = ReadinessReport(
        manifest_sha256=report.manifest_sha256,
        timestamp=max(c.timestamp for c in checks),
        checks=checks,
        target_revisions={target.id: report.target_revisions[target.id]},
        readiness=readiness_state(checks)
        if report.currency == "current"
        else "UNKNOWN",
        currency=report.currency,
    )
    assessed = assess_currency(projected, manifest, now=now, max_age_seconds=age)
    currency = assessed.currency
    if report.currency == "superseded":
        currency = "superseded"
    elif report.currency == "stale" and currency == "current":
        currency = "stale"
    consumer: dict[str, object] = {}
    sources = _consumer_sources(report, tier)
    for key, value in sorted(sources.items()):
        if key.startswith(target.id + "/"):
            consumer[key] = value.model_dump(mode="json")
    diagnostics = [
        CheckDiagnostic(
            check_id=c.check_id,
            state=c.state,
            reason=c.reason,
            failure_category=c.category,
        )
        for c in checks
        if c.state not in {"passed", "not_applicable"}
    ]
    for key, evidence in sorted(sources.items()):
        if key.startswith(target.id + "/"):
            diagnostics.extend(
                CheckDiagnostic(
                    check_id=key + "/" + c.id,
                    state=c.state,
                    reason=c.reason,
                    failure_category="recovery_prerequisite"
                    if c.state == "failed"
                    else "verification_infrastructure",
                )
                for c in evidence.checks
                if c.state not in {"passed", "not_applicable"}
            )
    provenance = _execution_provenance(report, target, tier)
    backups = {b.source_id: b for b in provenance.backups}
    for key, evidence in sources.items():
        if isinstance(evidence, ConsumerDrillResult) and key.startswith(
            target.id + "/"
        ):
            for snapshot in evidence.snapshots:
                observed = BackupProvenance(**snapshot.model_dump())
                if (
                    snapshot.source_id in backups
                    and backups[snapshot.source_id] != observed
                ):
                    raise ValueError("contradictory_backup_provenance")
                backups[snapshot.source_id] = observed
    for source_id in entry.consumers[target.id].backup_source_ids:
        backups.setdefault(source_id, BackupProvenance(source_id=source_id))
    provenance = provenance.model_copy(
        update={"backups": tuple(backups[k] for k in sorted(backups))}
    )
    unavailable = [
        name
        for name in ("playbooks", "inventories")
        if getattr(provenance, name) is None
        and not (name == "inventories" and provenance.inline_inventory)
    ]
    unavailable.extend(
        f"backup:{b.source_id}:{name}"
        for b in provenance.backups
        for name in ("artifact_sha256", "timestamp")
        if getattr(b, name) is None
    )
    return Attempt(
        evidence_reference=reference(entry),
        source_tier=entry.tier,
        target_id=target.id,
        owning_repository=entry.consumers[target.id].owning_repository,
        manifest_sha256=report.manifest_sha256,
        tested_revision=report.target_revisions[target.id],
        timestamp=projected.timestamp,
        evidence_timestamp=report.timestamp,
        duration_seconds=sum(c.duration_seconds for c in checks),
        readiness=readiness_state(checks),
        currency=currency,
        checks=checks,
        consumer_evidence=consumer,
        diagnostics=tuple(sorted(diagnostics, key=lambda d: d.check_id)),
        bootstrap_entrypoint=entry.consumers[target.id].bootstrap_entrypoint,
        recovery_entrypoint=entry.consumers[target.id].recovery_entrypoint,
        provenance=provenance,
        unavailable_provenance=tuple(sorted(unavailable)),
    )


def _execution_provenance(
    report: ReadinessReport, target: Target, tier: Level
) -> ExecutionProvenance:
    values = [report.provenance[target.id]] if target.id in report.provenance else []
    values.extend(c.provenance for c in _checks(report, target, tier) if c.provenance)
    values.extend(
        value.provenance
        for key, value in _consumer_sources(report, tier).items()
        if key.startswith(target.id + "/")
        and isinstance(value, ConsumerVerificationResult)
        and value.provenance
    )
    backups: dict[str, BackupProvenance] = {}
    for value in values:
        for backup in value.backups:
            if backup.source_id in backups and backups[backup.source_id] != backup:
                raise ValueError("contradictory_backup_provenance")
            backups[backup.source_id] = backup
    return ExecutionProvenance(
        playbooks=tuple(sorted({p for v in values for p in (v.playbooks or ())}))
        or None,
        inventories=tuple(sorted({p for v in values for p in (v.inventories or ())}))
        or None,
        inline_inventory="localhost"
        if any(v.inline_inventory for v in values)
        else None,
        backups=tuple(backups[k] for k in sorted(backups)),
    )


def _tier_score(
    target: Target, tier: Level, attempts: list[Attempt], now: datetime
) -> TierScore:
    attempts.sort(
        key=lambda a: (a.timestamp, a.evidence_timestamp, a.evidence_reference)
    )
    latest = attempts[-1] if attempts else None
    successful = [a for a in attempts if a.readiness == "READY"]
    success = successful[-1] if successful else None
    supported = tier in target.allowed_verification_levels
    status: TierStatus
    reasons: tuple[str, ...] = ()
    if not supported:
        status = "unsupported"
        reasons = (
            target.ephemeral_rebuild.reason
            if tier == "drill"
            else "capability_not_declared",
        )
    elif latest is None:
        status = "missing"
        reasons = ("evidence_missing",)
    elif latest.currency != "current":
        status = "expired" if latest.currency == "stale" else "superseded"
        reasons = ("evidence_" + latest.currency,)
    elif latest.readiness == "READY":
        status = "passed"
    else:
        status = "failed" if latest.readiness == "NOT_READY" else "unavailable"
        reasons = tuple(
            sorted({c.check_id + ":" + c.reason for c in latest.diagnostics})
        )
    actions = (
        ()
        if status == "passed"
        else (
            "add_approved_isolated_bootstrap"
            if status == "unsupported" and tier == "drill"
            else "declare_supported_capability"
            if status == "unsupported"
            else "fix_failed_checks_and_rerun_" + tier
            if status == "failed"
            else "run_" + tier + "_on_current_revision",
        )
    )
    return TierScore(
        tier=tier,
        supported=supported,
        status=status,
        latest_attempt=latest,
        last_success=success,
        evidence_age_seconds=(now - latest.timestamp).total_seconds()
        if latest
        else None,
        reasons=reasons,
        next_actions=actions,
        historical_failure_count=sum(a.readiness == "NOT_READY" for a in attempts),
        consecutive_failures=_consecutive_failures(attempts),
        regression=bool(
            latest
            and latest.readiness == "NOT_READY"
            and success
            and success.timestamp < latest.timestamp
        ),
        recurring_failed_checks=tuple(
            sorted(
                check_id
                for check_id in {
                    d.check_id
                    for a in attempts
                    for d in a.diagnostics
                    if d.state == "failed"
                }
                if sum(
                    any(
                        d.check_id == check_id and d.state == "failed"
                        for d in a.diagnostics
                    )
                    for a in attempts
                )
                > 1
            )
        ),
    )


def _consecutive_failures(attempts: list[Attempt]) -> int:
    count = 0
    for attempt in reversed(attempts):
        if attempt.readiness != "NOT_READY":
            break
        count += 1
    return count


def _combine(states: tuple[Readiness, ...]) -> Readiness:
    for state in ("NOT_READY", "UNKNOWN", "DEGRADED"):
        if state in states:
            return state
    return "READY" if states else "UNKNOWN"


def _dependencies(
    manifest: Manifest, policy: ScorecardPolicy
) -> dict[str, tuple[str, ...]]:
    targets = {t.id: t for t in manifest.targets}
    if set(policy.upstream_targets) - targets.keys():
        raise ValueError("unknown_dependency_target")
    result = {}
    for target in manifest.targets:
        owners = {
            d.reference.removeprefix("git://")
            for d in target.required_dependencies
            if d.kind == "repository"
        }
        inferred = {
            t.id
            for t in manifest.targets
            if t.owning_repository in owners
            and t.owning_repository != target.owning_repository
        }
        declared = policy.upstream_targets.get(target.id, ())
        if len(set(declared)) != len(declared):
            raise ValueError("duplicate_upstream_dependency")
        result[target.id] = tuple(sorted(inferred | set(declared)))
    return result


def build_scorecard(
    manifest: Manifest,
    history: History,
    *,
    now: datetime,
    policy: ScorecardPolicy | None = None,
) -> Scorecard:
    if now.tzinfo is None:
        raise ValueError("timezone_required")
    policy = policy or ScorecardPolicy()
    history = parse_history(canonical(merge_history(history, ())))
    for entry in history.entries:
        _validate_bound_report(entry, manifest)
    scores: dict[str, TargetScore] = {}
    seen: dict[tuple[str, Level, datetime], Attempt] = {}
    dependencies = _dependencies(manifest, policy)
    for target in sorted(manifest.targets, key=lambda t: t.id):
        tiers = []
        for tier in LEVELS:
            age = next(
                (
                    c.interval_seconds
                    for c in target.cadence_overrides
                    if c.level == tier
                ),
                getattr(policy, tier + "_max_age_seconds"),
            )
            attempts = []
            for entry in history.entries:
                if target.id not in entry.report.target_revisions:
                    continue
                attempt = _attempt(entry, target, tier, manifest, now, age)
                if attempt is None:
                    continue
                key = (target.id, tier, attempt.timestamp)
                previous = seen.get(key)
                if previous and (
                    previous.checks != attempt.checks
                    or previous.consumer_evidence != attempt.consumer_evidence
                    or previous.manifest_sha256 != attempt.manifest_sha256
                ):
                    raise ValueError("contradictory_evidence")
                seen[key] = attempt
                attempts.append(attempt)
            tiers.append(_tier_score(target, tier, attempts, now))
        successful = [t.tier for t in tiers if t.last_success and t.supported]
        current = [t.tier for t in tiers if t.status == "passed"]
        state: Readiness = (
            "NOT_READY"
            if any(t.status == "failed" for t in tiers)
            else "UNKNOWN"
            if any(t.status in {"unavailable", "expired", "superseded"} for t in tiers)
            else "READY"
            if len(current) == len(LEVELS)
            else "DEGRADED"
            if current
            else "UNKNOWN"
        )
        scores[target.id] = TargetScore(
            target_id=target.id,
            owning_repository=target.owning_repository,
            tested_revision=target.revision,
            readiness=state,
            full_recovery_verified=state == "READY",
            strongest_successful_level=successful[-1] if successful else None,
            strongest_current_level=current[-1] if current else None,
            tiers=tuple(tiers),
            upstream_targets=dependencies[target.id],
            reasons=tuple(sorted({r for t in tiers for r in t.reasons})),
            next_actions=tuple(sorted({a for t in tiers for a in t.next_actions})),
        )
    resolved: dict[str, TargetScore] = {}

    def resolve(identity: str, visiting: frozenset[str]) -> TargetScore | None:
        if identity in visiting:
            raise ValueError("cyclic_recovery_dependencies")
        if identity not in scores:
            return None
        if identity in resolved:
            return resolved[identity]
        score = scores[identity]
        upstream = [resolve(d, visiting | {identity}) for d in score.upstream_targets]
        blockers = tuple(
            sorted(
                d
                for d, s in zip(score.upstream_targets, upstream, strict=True)
                if s is None or s.readiness != "READY"
            )
        )
        if blockers:
            state = _combine(
                (score.readiness, *(s.readiness if s else "UNKNOWN" for s in upstream))
            )
            score = score.model_copy(
                update={
                    "readiness": state,
                    "full_recovery_verified": False,
                    "reasons": tuple(
                        sorted(
                            (
                                *score.reasons,
                                *("upstream_not_ready:" + d for d in blockers),
                            )
                        )
                    ),
                    "next_actions": tuple(
                        sorted(
                            (
                                *score.next_actions,
                                *("verify_upstream:" + d for d in blockers),
                            )
                        )
                    ),
                }
            )
        resolved[identity] = score
        return score

    for identity in scores:
        resolve(identity, frozenset())
    targets = tuple(resolved[key] for key in sorted(resolved))
    scorecard = Scorecard(
        observed_at=now,
        readiness=_combine(tuple(t.readiness for t in targets)),
        full_recovery_verified=all(t.full_recovery_verified for t in targets),
        history_reference=reference(history),
        history_count=len(history.entries),
        targets=targets,
    )
    ensure_no_runtime_secrets(canonical(scorecard))
    return scorecard


def human_summary(scorecard: Scorecard) -> str:
    lines = [
        f"Recovery readiness: {scorecard.readiness}; "
        f"full recovery verified: {scorecard.full_recovery_verified}"
    ]
    for target in scorecard.targets:
        tiers = ", ".join(
            f"{label}={tier.status}"
            for label, tier in zip(
                ("daily", "weekly", "monthly"), target.tiers, strict=True
            )
        )
        lines.append(
            f"{target.owning_repository} / {target.target_id}: "
            f"{target.readiness}; {tiers}"
        )
        lines.append(
            "  Last success: "
            + ", ".join(
                f"{label}="
                + (
                    tier.last_success.timestamp.isoformat()
                    if tier.last_success
                    else "none"
                )
                for label, tier in zip(
                    ("daily", "weekly", "monthly"), target.tiers, strict=True
                )
            )
        )
        if target.reasons:
            lines.append("  Reasons: " + ", ".join(target.reasons))
        if target.next_actions:
            lines.append("  Next: " + ", ".join(target.next_actions))
    return "\n".join(lines) + "\n"
