"""Explicit contract validation and infrastructure smoke jobs."""

from dagster import Config, in_process_executor, job, mem_io_manager, op
from pydantic import Field

from recovery_verification.contract import parse_manifest
from recovery_verification.preflight import preflight


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
