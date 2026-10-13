"""Observe terminal events independently of job teardown and run cancellation."""

from dagster import DefaultSensorStatus, SensorEvaluationContext, SkipReason, sensor

from recovery_verification.orchestration import load_config
from recovery_verification.terminal_notifications import process_terminal_runs


@sensor(minimum_interval_seconds=60, default_status=DefaultSensorStatus.STOPPED)
def recovery_run_notifications(context: SensorEvaluationContext) -> SkipReason:
    config = load_config()
    if not config.notifications_enabled:
        return SkipReason("terminal_notifications_disabled")
    state = process_terminal_runs(config, context.instance)
    return SkipReason(
        "terminal_notifications_pending"
        if state.pending
        else "terminal_notifications_acknowledged"
    )
