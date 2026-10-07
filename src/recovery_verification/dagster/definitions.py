"""Host-independent code location without consumer-specific definitions."""

from dagster import Definitions

from recovery_verification.dagster import jobs

defs = Definitions.merge(
    Definitions(jobs=[jobs.contract_preflight_job, jobs.runtime_smoke_job]),
    jobs.daily_definitions,
)
