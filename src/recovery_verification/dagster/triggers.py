"""Configured New York cadence and restart-safe Git change sensor."""

import json
import subprocess
from pathlib import Path

from dagster import (
    DefaultScheduleStatus,
    DefaultSensorStatus,
    RunRequest,
    ScheduleDefinition,
    ScheduleEvaluationContext,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
)

from recovery_verification.contract import Level
from recovery_verification.dagster import jobs
from recovery_verification.orchestration import (
    JOBS,
    changed_paths,
    current_manifest,
    deployed_cadence,
    execution_request,
    load_config,
    proportional_tier,
    revision_cursor,
)


def schedules() -> list[ScheduleDefinition]:
    cadence = deployed_cadence()
    result = []
    for tier, cron in zip(
        JOBS, (cadence.daily, cadence.weekly, cadence.monthly), strict=True
    ):

        def tick(
            context: ScheduleEvaluationContext, selected_tier: Level = tier
        ) -> RunRequest | SkipReason:
            try:
                config = load_config()
                manifest = current_manifest(config)
                timestamp = context.scheduled_execution_time
                if timestamp is None:
                    return SkipReason("scheduled_execution_time_required")
                return execution_request(
                    config,
                    manifest,
                    selected_tier,
                    f"schedule:{config.environment}:{selected_tier}:{timestamp.isoformat()}",
                )
            except OSError, ValueError, subprocess.SubprocessError:
                return SkipReason("orchestration_configuration_or_export_unavailable")

        result.append(
            ScheduleDefinition(
                name=JOBS[tier] + "_schedule",
                job=getattr(jobs, JOBS[tier]),
                cron_schedule=cron,
                execution_timezone="America/New_York",
                execution_fn=tick,
                default_status=DefaultScheduleStatus.STOPPED,
            )
        )
    return result


@sensor(
    name="recovery_relevant_changes",
    jobs=[getattr(jobs, name) for name in JOBS.values()],
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.STOPPED,
)
def recovery_relevant_changes(
    context: SensorEvaluationContext,
) -> SensorResult | SkipReason:
    try:
        config = load_config()
        manifest = current_manifest(config)
        previous = revision_cursor(context.cursor)
        current = {t.id: t.revision for t in manifest.targets}
        requests = []
        for target in manifest.targets:
            old = previous.get(target.id)
            if not old or old == target.revision:
                continue
            paths = changed_paths(
                Path(config.checkouts[target.id]), old, target.revision
            )
            tier = proportional_tier(
                paths, isolated_test_mode=config.isolated_test_mode
            )
            if tier:
                requests.append(
                    execution_request(
                        config,
                        manifest,
                        tier,
                        f"change:{config.environment}:{target.id}:{old}:{target.revision}:{tier}",
                        (target.id,),
                    )
                )
        return SensorResult(
            run_requests=requests, cursor=json.dumps(current, sort_keys=True)
        )
    except OSError, ValueError, subprocess.SubprocessError:
        # Retain cursor on failure so the same change is considered next tick.
        return SkipReason("recovery_change_detection_unavailable")
