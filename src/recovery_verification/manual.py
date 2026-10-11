"""Targeted operational runs use the same lock, journal, assets and broker outbox."""

import argparse
import json
import sys
from uuid import uuid4

from recovery_verification.dagster.definitions import defs
from recovery_verification.orchestration import (
    JOBS,
    current_manifest,
    execution_request,
    load_config,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tier", choices=JOBS)
    parser.add_argument("--target", action="append", default=[])
    parser.add_argument("--execution-key", default=None)
    args = parser.parse_args(argv)
    try:
        config = load_config()
        manifest = current_manifest(config)
        request = execution_request(
            config,
            manifest,
            args.tier,
            args.execution_key or str(uuid4()),
            tuple(args.target),
        )
        run = defs.resolve_job_def(JOBS[args.tier]).execute_in_process(
            run_config=request.run_config, tags=request.tags, raise_on_error=False
        )
    except OSError, ValueError:
        sys.stdout.write(
            '{"state":"unavailable","reason":"operational_configuration_unavailable"}\n'
        )
        return 2
    sys.stdout.write(
        json.dumps(
            {
                "state": "executed" if run.success else "failed",
                "run_id": run.run_id,
                "assurance": "inspect_retained_evidence",
            },
            sort_keys=True,
        )
        + "\n"
    )
    return 0 if run.success else 1
