"""Evidence-derived editorial reports. This module never evaluates readiness."""

from dataclasses import dataclass, field
from datetime import datetime
from zoneinfo import ZoneInfo

from recovery_verification.scorecard import Scorecard, TargetScore, TierScore

LOCAL = ZoneInfo("America/New_York")
TIERS = {
    "readiness": "Daily readiness",
    "verification": "Weekly verification",
    "drill": "Monthly recovery drill",
}
CATEGORIES = (
    "Backup execution and freshness",
    "Backup integrity and storage",
    "Recovery dependencies",
    "Recovery verification",
    "Recovery capabilities",
    "Security and custody",
)
# Catalog checks aggregate child results; their state, never their name, decides
# whether they are actionable. Passed/not-applicable diagnostics are filtered.
DESCRIPTIONS = {
    "independent_key_custody_unverified": (
        5,
        "Independent encryption-key custody remains unverified",
        "Complete OP-543 with acceptable independent custody evidence.",
    ),
    "artifact_publication_required": (
        2,
        "Required local image has no verified recovery artifact",
        "Publish a reviewed immutable image or verify an independently "
        "retained image export.",
    ),
    "registry_auth_unavailable": (
        2,
        "Registry authentication is unavailable",
        "Configure the scoped registry reader and verify the selected digest.",
    ),
    "backup_stale": (
        0,
        "Backup evidence is stale",
        "Produce a current backup and rerun readiness.",
    ),
    "backup_artifact_unproven": (
        1,
        "Required backup artifact has not been verified",
        "Verify the required artifact and rerun readiness.",
    ),
    "storage_missing": (
        1,
        "Required recovery storage is unavailable",
        "Restore access to the required storage.",
    ),
    "storage_unreachable": (
        1,
        "Recovery storage cannot be reached",
        "Check storage connectivity and rerun readiness.",
    ),
    "required_mount_absent": (
        1,
        "Required recovery storage is not mounted",
        "Restore the required mount.",
    ),
    "storage_unreadable": (
        1,
        "Recovery storage cannot be read",
        "Restore read access to the required storage.",
    ),
    "secret_probe_unconfigured": (
        2,
        "Credential verification is not configured",
        "Configure the approved metadata-only credential probe.",
    ),
    "secret_catalog_checked": (
        2,
        "Runtime secret catalog contains unresolved dependencies",
        "Verify the required runtime secret references.",
    ),
    "artifact_catalog_checked": (
        2,
        "Container image catalog contains unresolved dependencies",
        "Validate access to every required container image.",
    ),
    "artifact_catalog_missing": (
        2,
        "Required container image catalog is missing",
        "Restore the declared image catalog.",
    ),
    "secret_reference_missing": (
        5,
        "Required recovery credential is missing",
        "Provision the required credential through the approved secret service.",
    ),
    "isolated_bootstrap_not_supported": (
        4,
        "Isolated recovery bootstrap is not supported",
        "Add an approved isolated bootstrap capability before "
        "demonstrating full recovery.",
    ),
    "evidence_missing": (
        3,
        "Required recovery evidence is missing",
        "Run the required verification on the current revision.",
    ),
    "evidence_stale": (
        3,
        "Recovery evidence is stale",
        "Rerun the required verification.",
    ),
    "evidence_superseded": (
        3,
        "Recovery evidence belongs to an earlier revision",
        "Run verification on the current revision.",
    ),
}
SUBJECTS = {
    "backup_database_cutover": "Database backup",
    "backup_migration_snapshot": "Migration backup",
    "backup_cluster_cutovers": "Cluster recovery backup",
    "backup_dagster_cutover": "Dagster backup",
    "dependency_ci_ghcr": "Container registry credentials",
    "dependency_runtime_secret_names": "Runtime secret catalog",
    "dependency_container_images": "Container image catalog",
    "dependency_application_images": "Container image catalog",
    "dependency_infisical_bootstrap": "Infisical bootstrap storage",
    "dependency_postgres_state": "PostgreSQL recovery state",
}


def environment(target: TargetScore) -> str:
    owner = target.owning_repository.rsplit("/", 1)[-1]
    if owner == "homelab":
        return "Homelab"
    if owner == "infrastructure":
        return "Infrastructure" + (
            " (dev)" if target.target_id.endswith("-dev") else ""
        )
    return target.target_id.replace("_", " ").replace("-", " ").capitalize()


def local_time(value: datetime) -> str:
    local = value.astimezone(LOCAL)
    return (
        local.strftime("%b ") + str(local.day) + local.strftime(", %Y · %-I:%M %p %Z")
    )


@dataclass
class Finding:
    category: str
    severity: int
    problem: str
    action: str
    environments: set[str] = field(default_factory=set)
    diagnostics: list[dict[str, str]] = field(default_factory=list)

    def text(self) -> str:
        return (
            ", ".join(sorted(self.environments))
            + ": "
            + self.problem
            + ". "
            + self.action
        )


def describe(check: str, reason: str, state: str) -> tuple[str, str, str]:
    if check.startswith("consumer_detail_"):
        check = check.split("_", 3)[-1]
    known = DESCRIPTIONS.get(reason)
    category = (
        CATEGORIES[known[0]]
        if known
        else (
            CATEGORIES[1]
            if check.startswith("backup_")
            else CATEGORIES[2]
            if check.startswith("dependency_")
            else CATEGORIES[3]
        )
    )
    subject = SUBJECTS.get(
        check, check.replace("_", " ").replace("/", " / ").capitalize()
    )
    if known:
        problem, action = known[1:]
        if check:
            problem = subject + ": " + problem[0].lower() + problem[1:]
    else:
        problem = (
            subject
            + ": "
            + {
                "failed": "failed",
                "unsupported": "unsupported",
                "expired": "evidence is stale",
                "superseded": "evidence belongs to an earlier revision",
                "missing": "evidence is missing",
                "skipped": "was not verified",
            }.get(state, "evidence is unavailable")
        )
        action = "Inspect the linked diagnostic and rerun the affected verification."
    # An aggregate catalog reason can mean unavailable or failed; never erase it.
    if reason in {"artifact_catalog_checked", "secret_catalog_checked"}:
        problem += (
            " ("
            + (
                "verification failed"
                if state == "failed"
                else "verification unavailable"
            )
            + ")"
        )
    return category, problem, action


def findings(score: Scorecard | None) -> list[Finding]:
    found: dict[tuple[str, str, str], Finding] = {}

    def add(
        target: TargetScore, tier: str, check: str, reason: str, state: str
    ) -> None:
        if state in {"passed", "not_applicable"}:
            return
        category, problem, action = describe(check, reason, state)
        # Identity includes the actual diagnostic, not similar display wording.
        key = (
            ("isolated bootstrap", reason, "unsupported")
            if reason == "isolated_bootstrap_not_supported"
            else (check, reason, state)
        )
        item = found.setdefault(
            key, Finding(category, 0 if state == "failed" else 1, problem, action)
        )
        item.environments.add(environment(target))
        item.diagnostics.append(
            {
                "target": target.target_id,
                "tier": tier,
                "check": check,
                "reason": reason,
                "state": state,
            }
        )

    if score:
        for target in score.targets:
            covered: set[str] = set()
            for tier in target.tiers:
                diagnostics = (
                    tier.latest_attempt.diagnostics if tier.latest_attempt else ()
                )
                for diagnostic in diagnostics:
                    covered.add(diagnostic.check_id + ":" + diagnostic.reason)
                    if diagnostic.state in {"passed", "not_applicable"} or (
                        diagnostic.reason == "consumer_local_checks"
                        and any(
                            d.check_id.startswith("consumer_detail_")
                            for d in diagnostics
                        )
                    ):
                        continue
                    # Consumer IDs include the target prefix; remove only that
                    # prefix so a shared diagnostic can accurately merge targets.
                    check = diagnostic.check_id.removeprefix(target.target_id + "/")
                    add(target, tier.tier, check, diagnostic.reason, diagnostic.state)
                for reason in tier.reasons:
                    if reason in covered:
                        continue
                    check, separator, code = reason.partition(":")
                    add(
                        target,
                        tier.tier,
                        check if separator else TIERS[tier.tier],
                        code if separator else reason,
                        tier.status,
                    )
                    covered.add(reason)
            for reason in target.reasons:
                if reason not in covered:
                    add(
                        target,
                        "scorecard",
                        "Recovery dependency",
                        reason,
                        "unavailable",
                    )
    if score:
        custody_environments = {
            environment(t)
            for t in score.targets
            if t.owning_repository.rsplit("/", 1)[-1] in {"homelab", "infrastructure"}
            and not (
                t.tiers[0].latest_attempt
                and t.tiers[0].latest_attempt.currency == "current"
                and any(
                    c.check_id.endswith("_independent_key_custody")
                    for c in t.tiers[0].latest_attempt.checks
                )
            )
        }
        if custody_environments:
            found[("custody", "OP-543", "unavailable")] = Finding(
                CATEGORIES[5],
                1,
                "Independent encryption-key custody remains unverified",
                "Complete OP-543 with acceptable independent custody evidence.",
                custody_environments,
                [
                    {
                        "target": "shared",
                        "tier": "custody",
                        "check": "OP-543",
                        "reason": "independent_custody_outstanding",
                        "state": "unavailable",
                    }
                ],
            )
    else:
        found[("scorecard", "unavailable", "unavailable")] = Finding(
            CATEGORIES[3],
            1,
            "No trusted recovery scorecard is available",
            "Restore readable verification evidence and rerun verification.",
            {"Recovery Verification"},
        )
    return sorted(
        found.values(),
        key=lambda f: (
            CATEGORIES.index(f.category),
            f.severity,
            f.problem,
            sorted(f.environments),
        ),
    )


def backup_health(target: TargetScore) -> tuple[str, str, str]:
    daily, weekly = target.tiers[:2]
    current = daily.latest_attempt
    checks = (
        [c for c in current.checks if c.check_id.startswith("backup_")]
        if current
        else []
    )
    # Daily probes attest to availability/freshness, not producer execution.
    outcome = "Execution unverified"
    freshness = (
        "Stale"
        if any(c.reason == "backup_stale" for c in checks)
        else (
            "Current"
            if checks
            and all(c.state == "passed" and c.reason == "backup_fresh" for c in checks)
            and current
            and current.currency == "current"
            else "Unavailable"
        )
    )
    if checks and all(c.state == "passed" for c in checks):
        outcome += " · evidence available"
    elif any(c.state == "failed" for c in checks):
        outcome += " · prerequisite failed"
    else:
        outcome += " · evidence unavailable"
    verified = weekly.latest_attempt
    integrity = "Unavailable"
    if verified:
        groups = [v for v in verified.consumer_evidence.values() if isinstance(v, dict)]
        integrity_checks = [
            c
            for v in groups
            for c in v.get("checks", [])
            if isinstance(c, dict) and c.get("phase") == "integrity"
        ]
        if any(c.get("state") == "failed" for c in integrity_checks):
            integrity = "Failed"
        elif integrity_checks and all(
            c.get("state") == "passed" for c in integrity_checks
        ):
            latest = (
                {b.source_id: b.artifact_sha256 for b in current.provenance.backups}
                if current
                else {}
            )
            tested = {
                b.source_id: b.artifact_sha256
                for b in verified.provenance.backups
                if b.kind == "recovery_backup"
            }
            # Historical or mismatched artifacts cannot attest to latest integrity.
            if latest and latest == tested and all(latest.values()):
                integrity = "Verified"
    return outcome, freshness, integrity


def tier_result(tier: TierScore) -> str:
    result = {
        "passed": "Passed",
        "failed": "Failed",
        "missing": "Missing evidence",
        "unavailable": "Unavailable",
        "expired": "Stale evidence",
        "superseded": "Earlier revision",
        "unsupported": "Unsupported",
    }[tier.status]
    attempt = tier.latest_attempt
    if attempt:
        if any(b.kind == "representative_fixture" for b in attempt.provenance.backups):
            result = "Fixture " + result.lower() + " · production recovery unverified"
        result += " · " + attempt.evidence_timestamp.astimezone(LOCAL).strftime(
            "%b %-d, %Y"
        )
    return result


def report_blocks(  # noqa: PLR0912, PLR0913 -- Explicit report sections and inputs.
    score: Scorecard | None,
    status: str,
    job: str,
    at: datetime,
    *,
    historical: bool,
    synthetic: bool = False,
) -> tuple[list[dict[str, str]], list[Finding]]:
    blocks: list[dict[str, str]] = [{"heading": "", "text": local_time(at)}]
    if synthetic:
        blocks.append(
            {
                "heading": "",
                "text": "Synthetic notification — no production backup or recovery "
                "operation was performed.\nAll backup and scorecard evidence below "
                "is test data. Dagster states are simulated.",
            }
        )
    tier = {
        "recovery_readiness_daily": "Daily",
        "recovery_verification_weekly": "Weekly",
        "recovery_drill_monthly": "Monthly",
    }.get(job, "Other")
    blocks.append(
        {
            "heading": "Overall",
            "text": f"Recovery readiness: {score.readiness if score else 'UNKNOWN'}"
            f"\nDagster execution: {status}\nVerification: {tier}",
        }
    )
    if historical or score is None:
        text = "Current scorecard was not generated or could not be established "
        text += "for this run."
        if score:
            times = [
                s.latest_attempt.evidence_timestamp
                for t in score.targets
                for s in t.tiers
                if s.latest_attempt
            ]
            text += "\nHistorical evidence"
            if times:
                text += " · latest " + local_time(max(times))
        else:
            text += "\nNo trusted scorecard is available."
        blocks.append({"heading": "", "text": text})
    if status != "SUCCESS":
        blocks.append(
            {
                "heading": "Execution",
                "text": "Dagster execution "
                + (
                    "failed before completion"
                    if status == "FAILURE"
                    else "was canceled"
                )
                + ". Inspect the run results for the sanitized execution diagnostics.",
            }
        )
    targets = score.targets if score else ()
    backup_lines = [environment(t) + ": " + backup_health(t)[0] for t in targets]
    for index, label in ((2, "Integrity"), (1, "Freshness")):
        values = [(environment(t), backup_health(t)[index]) for t in targets]
        if values and len({v for _, v in values}) == 1:
            backup_lines.append(label + ": " + values[0][1])
        else:
            backup_lines.extend(label + " — " + e + ": " + v for e, v in values)
    blocks.append(
        {
            "heading": "Backups",
            "text": "\n".join(backup_lines)
            or "Backup execution, integrity and freshness: Unavailable",
        }
    )
    recovery_lines = [environment(t) + ": " + t.readiness for t in targets]
    for tier_id, label in TIERS.items():
        values = [
            (environment(t), tier_result(s))
            for t in targets
            for s in t.tiers
            if s.tier == tier_id
        ]
        if values and len({v for _, v in values}) == 1:
            recovery_lines.append(label + ": " + values[0][1])
        else:
            recovery_lines.extend(label + " — " + e + ": " + v for e, v in values)
    fixtures = any(
        b.kind == "representative_fixture"
        for t in targets
        for s in t.tiers
        if s.latest_attempt
        for b in s.latest_attempt.provenance.backups
    )
    full = (
        "Verified"
        if score and score.full_recovery_verified and not fixtures
        else "Unsupported"
        if targets
        and all(
            any(s.tier == "drill" and s.status == "unsupported" for s in t.tiers)
            for t in targets
        )
        else "Unverified"
    )
    recovery_lines.append("Full recovery: " + full)
    blocks.append({"heading": "Recovery scorecard", "text": "\n".join(recovery_lines)})
    items = findings(score)
    blocks.append(
        {
            "heading": "Outstanding findings — " + str(len(items)),
            "text": "" if items else "No outstanding findings.",
        }
    )
    for item in items:
        blocks.append({"heading": item.category, "text": item.text()})
    return blocks, items


def plain_report(blocks: list[dict[str, str]], title: str, link: str) -> str:
    sections = []
    previous = ""
    for block in blocks:
        heading = block["heading"]
        sections.append(
            (heading + "\n" if heading and heading != previous else "") + block["text"]
        )
        previous = heading
    return (
        title
        + "\n\n"
        + "\n\n".join(sections)
        + "\n\nEvidence\nView Dagster results: "
        + link
    )
