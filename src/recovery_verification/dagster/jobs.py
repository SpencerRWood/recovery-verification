"""Explicit contract validation and infrastructure smoke jobs."""

import os
from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetSelection,
    Config,
    Definitions,
    MetadataValue,
    RunRequest,
    SkipReason,
    asset,
    asset_check,
    define_asset_job,
    in_process_executor,
    job,
    mem_io_manager,
    op,
    schedule,
)
from pydantic import Field

from recovery_verification.contract import parse_manifest
from recovery_verification.preflight import preflight
from recovery_verification.probes import BoundedProbe
from recovery_verification.readiness import ReadinessReport, run_readiness


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


@asset(io_manager_key="io_manager")
def recovery_readiness(config: DailyConfig) -> str:
    """Same public checks as standalone diagnosis; no alternate daily code path."""
    report = run_readiness(
        parse_manifest(config.manifest_json),
        {target: Path(root) for target, root in config.checkouts.items()},
        BoundedProbe(),
        target_ids=tuple(config.target_ids),
        check_ids=tuple(config.check_ids),
    )
    return report.model_dump_json()


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


@schedule(
    job=recovery_readiness_daily, cron_schedule="0 6 * * *", execution_timezone="UTC"
)
def recovery_readiness_daily_schedule() -> RunRequest | SkipReason:
    """Stopped by default; explicit non-secret configuration enables cadence."""
    source = os.environ.get("RECOVERY_READINESS_CONFIG")
    if not source:
        return SkipReason("readiness_configuration_not_provided")
    try:
        config = DailyConfig.model_validate_json(Path(source).read_bytes())
        parse_manifest(config.manifest_json)
    except OSError, ValueError:
        return SkipReason("readiness_configuration_invalid")
    return RunRequest(
        run_config={"ops": {"recovery_readiness": {"config": config.model_dump()}}}
    )


daily_definitions = Definitions(
    assets=[recovery_readiness],
    asset_checks=[prerequisites_ready],
    jobs=[recovery_readiness_daily],
    schedules=[recovery_readiness_daily_schedule],
    resources={"io_manager": mem_io_manager},
)
