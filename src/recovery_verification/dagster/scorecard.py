"""Manual scorecard asset over explicit retained evidence; no production schedule."""

from datetime import datetime
from pathlib import Path

from dagster import AssetSelection, Config, asset, define_asset_job, in_process_executor

from recovery_verification.contract import parse_manifest
from recovery_verification.history import read_history
from recovery_verification.scorecard import ScorecardPolicy, build_scorecard, canonical


class ScorecardConfig(Config):
    manifest_json: str
    history_path: str
    observed_at: str
    policy_json: str = "{}"


@asset(io_manager_key="io_manager")
def recovery_readiness_scorecard(config: ScorecardConfig) -> str:
    return canonical(
        build_scorecard(
            parse_manifest(config.manifest_json),
            read_history(Path(config.history_path)),
            now=datetime.fromisoformat(config.observed_at),
            policy=ScorecardPolicy.model_validate_json(config.policy_json),
        )
    )


recovery_scorecard = define_asset_job(
    "recovery_scorecard",
    selection=AssetSelection.assets(recovery_readiness_scorecard),
    executor_def=in_process_executor,
)
