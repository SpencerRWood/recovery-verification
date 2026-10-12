"""Host-independent code location without consumer-specific definitions."""

from dagster import Definitions

from recovery_verification.dagster import jobs
from recovery_verification.dagster.triggers import recovery_relevant_changes, schedules

defs = Definitions.merge(
    Definitions(jobs=[jobs.contract_preflight_job, jobs.runtime_smoke_job]),
    jobs.daily_definitions,
    Definitions(schedules=schedules(), sensors=[recovery_relevant_changes]),
)
