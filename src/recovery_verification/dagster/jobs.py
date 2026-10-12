"""Explicit contract validation and infrastructure smoke jobs."""

from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    AssetSelection,
    Config,
    Definitions,
    MetadataValue,
    asset,
    asset_check,
    define_asset_job,
    in_process_executor,
    job,
    mem_io_manager,
    op,
)
from pydantic import Field

from recovery_verification.contract import parse_manifest
from recovery_verification.dagster.runtime import recovery_execution
from recovery_verification.dagster.scorecard import (
    recovery_readiness_scorecard,
    recovery_scorecard,
)
from recovery_verification.execution import RetryingProbe
from recovery_verification.monthly import MonthlyReport, extend_monthly
from recovery_verification.preflight import preflight
from recovery_verification.probes import BoundedProbe
from recovery_verification.readiness import ReadinessReport, run_readiness
from recovery_verification.weekly import WeeklyReport, extend_weekly


class PreflightConfig(Config):
    """Explicit manifest input; no hidden local or legacy configuration."""

    manifest_json: str
    target_ids: list[str] = Field(default_factory=list)


@op
def contract_preflight(config: PreflightConfig) -> str:
    """Validate declarations without invoking consumer recovery interfaces."""
    result = preflight(parse_manifest(config.manifest_json), tuple(config.target_ids))
    return result.model_dump_json()


@job(executor_def=in_process_executor, resource_defs={"io_manager": mem_io_manager})
def contract_preflight_job() -> None:
    """Manual foundation job, independent of later cadence execution."""
    contract_preflight()


@op
def runtime_smoke() -> str:
    """Prove shared execution without application secrets or external APIs."""
    return "ok"


@job(executor_def=in_process_executor, resource_defs={"io_manager": mem_io_manager})
def runtime_smoke_job() -> None:
    """Candidate-image runtime check required by the platform contract."""
    runtime_smoke()


class DailyConfig(PreflightConfig):
    """Only non-secret declarations. Tokens are process-injected runtime inputs."""

    checkouts: dict[str, str] = Field(default_factory=dict)
    check_ids: list[str] = Field(default_factory=list)


@asset(io_manager_key="io_manager", required_resource_keys={"recovery_execution"})
def recovery_readiness(context: AssetExecutionContext, config: DailyConfig) -> str:
    """Same public checks as standalone diagnosis; no alternate daily code path."""
    execution = context.resources.recovery_execution

    def run() -> str:
        return run_readiness(
            parse_manifest(config.manifest_json),
            {target: Path(root) for target, root in config.checkouts.items()},
            RetryingProbe(execution.config.probe_attempts)
            if execution
            else BoundedProbe(),
            target_ids=tuple(config.target_ids),
            check_ids=tuple(config.check_ids),
        ).model_dump_json()

    return execution.report("readiness", run) if execution else run()


@asset_check(asset=recovery_readiness)
def prerequisites_ready(recovery_readiness: str) -> AssetCheckResult:
    report = ReadinessReport.model_validate_json(recovery_readiness)
    return AssetCheckResult(
        passed=report.readiness == "READY",
        metadata={"evidence": MetadataValue.json(report.model_dump(mode="json"))},
    )


recovery_readiness_daily = define_asset_job(
    "recovery_readiness_daily",
    selection=AssetSelection.assets(recovery_readiness),
    executor_def=in_process_executor,
)


@asset(io_manager_key="io_manager", required_resource_keys={"recovery_execution"})
def recovery_verification(
    context: AssetExecutionContext, recovery_readiness: str, config: DailyConfig
) -> str:
    """Consume the selected daily asset, then add executable weekly checks."""
    daily = ReadinessReport.model_validate_json(recovery_readiness)
    manifest = parse_manifest(config.manifest_json)
    execution = context.resources.recovery_execution

    def run() -> str:
        return extend_weekly(
            manifest,
            {target: Path(root) for target, root in config.checkouts.items()},
            daily,
            target_ids=tuple(config.target_ids),
        ).model_dump_json()

    return execution.report("verification", run) if execution else run()


@asset_check(asset=recovery_verification)
def recovery_paths_verified(recovery_verification: str) -> AssetCheckResult:
    report = WeeklyReport.model_validate_json(recovery_verification)
    return AssetCheckResult(
        passed=report.readiness == "READY",
        metadata={"evidence": MetadataValue.json(report.model_dump(mode="json"))},
    )


recovery_verification_weekly = define_asset_job(
    "recovery_verification_weekly",
    selection=AssetSelection.assets(recovery_readiness, recovery_verification),
    executor_def=in_process_executor,
)


@asset(io_manager_key="io_manager", required_resource_keys={"recovery_execution"})
def recovery_drill(
    context: AssetExecutionContext, recovery_readiness: str, config: DailyConfig
) -> str:
    """Reuse the daily asset; weekly fallback runs only for unsupported targets."""
    manifest = parse_manifest(config.manifest_json)
    execution = context.resources.recovery_execution
    if (
        execution
        and not execution.config.isolated_test_mode
        and any(
            t.ephemeral_rebuild.supported
            for t in manifest.targets
            if not config.target_ids or t.id in config.target_ids
        )
    ):
        raise ValueError("isolated_test_mode_required")

    def run() -> str:
        return extend_monthly(
            manifest,
            {target: Path(root) for target, root in config.checkouts.items()},
            ReadinessReport.model_validate_json(recovery_readiness),
            target_ids=tuple(config.target_ids),
        ).model_dump_json()

    return execution.report("drill", run) if execution else run()


@asset_check(asset=recovery_drill)
def isolated_recovery_proven(recovery_drill: str) -> AssetCheckResult:
    report = MonthlyReport.model_validate_json(recovery_drill)
    return AssetCheckResult(
        passed=report.readiness == "READY",
        metadata={"evidence": MetadataValue.json(report.model_dump(mode="json"))},
    )


recovery_drill_monthly = define_asset_job(
    "recovery_drill_monthly",
    selection=AssetSelection.assets(recovery_readiness, recovery_drill),
    executor_def=in_process_executor,
)


daily_definitions = Definitions(
    assets=[
        recovery_readiness,
        recovery_verification,
        recovery_drill,
        recovery_readiness_scorecard,
    ],
    asset_checks=[
        prerequisites_ready,
        recovery_paths_verified,
        isolated_recovery_proven,
    ],
    jobs=[
        recovery_readiness_daily,
        recovery_verification_weekly,
        recovery_drill_monthly,
        recovery_scorecard,
    ],
    resources={"io_manager": mem_io_manager, "recovery_execution": recovery_execution},
)
