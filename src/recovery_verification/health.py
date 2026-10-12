"""Read-only code-location registration contract; never runs recovery jobs."""

import json
import sys

from dagster import Definitions, ScheduleDefinition

from recovery_verification.dagster.definitions import defs
from recovery_verification.orchestration import JOBS


def main() -> int:
    Definitions.validate_loadable(defs)
    jobs = {job.name for job in defs.resolve_all_job_defs()}
    schedules = {
        s.name: s.execution_timezone
        for s in defs.schedules or ()
        if isinstance(s, ScheduleDefinition)
    }
    sensors = {s.name for s in defs.sensors or ()}
    passed = (
        set(JOBS.values()).issubset(jobs)
        and all(
            schedules.get(name + "_schedule") == "America/New_York"
            for name in JOBS.values()
        )
        and "recovery_relevant_changes" in sensors
    )
    sys.stdout.write(
        json.dumps(
            {
                "state": "passed" if passed else "failed",
                "jobs": sorted(jobs),
                "schedules": schedules,
                "sensors": sorted(sensors),
                "scope": "registration_only",
            },
            sort_keys=True,
        )
        + "\n"
    )
    return 0 if passed else 1
