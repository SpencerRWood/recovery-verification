"""Host-independent code location without consumer-specific definitions."""

from dagster import Definitions

from recovery_verification.dagster.jobs import contract_preflight_job, runtime_smoke_job

defs = Definitions(jobs=[contract_preflight_job, runtime_smoke_job])
