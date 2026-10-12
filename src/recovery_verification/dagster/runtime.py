"""Optional operational resource; explicit manual diagnostics retain their scope."""

import os
from collections.abc import Iterator
from typing import Any, cast

from dagster import InitResourceContext, resource

from recovery_verification.contract import parse_manifest
from recovery_verification.execution import Execution, execution_session
from recovery_verification.orchestration import JOBS, load_config


@resource
def recovery_execution(context: InitResourceContext) -> Iterator[Execution | None]:
    if not os.environ.get("RECOVERY_ORCHESTRATION_CONFIG"):
        yield None
        return
    config = load_config()
    run = context.run
    if run is None:
        raise ValueError("operational_run_context_required")
    inputs = cast("dict[str, Any]", run.run_config)["ops"]["recovery_readiness"][
        "config"
    ]
    if inputs["checkouts"] != config.checkouts:
        raise ValueError("operational_checkout_configuration_mismatch")
    manifest = parse_manifest(inputs["manifest_json"])
    key = run.tags.get("recovery/execution_key", run.run_id)
    tier = next((tier for tier, name in JOBS.items() if name == run.job_name), None)
    with execution_session(
        config,
        key,
        manifest,
        expected_tier=tier,
        target_ids=tuple(inputs.get("target_ids", ())),
    ) as execution:
        yield execution
