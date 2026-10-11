"""Require the proposed reusable and consumer workflow safety contracts."""

import json
import os
import subprocess
import sys
from pathlib import Path


def main() -> int:
    source = os.environ.get("RECOVERY_WORKFLOWS_CHECKOUT")
    consumers = os.environ.get("RECOVERY_CONSUMER_CHECKOUTS")
    if not source or not consumers:
        sys.stdout.write(
            '{"status":"unavailable","reason":"workflow_checkouts_required"}\n'
        )
        return 4
    paths = json.loads(consumers)
    environment = dict(
        os.environ,
        RECOVERY_GOVERNANCE_CHECKOUTS=os.pathsep.join([str(Path.cwd()), *paths]),
    )
    return subprocess.run(  # noqa: S603 — explicit operator-provided contract checkout.
        [
            sys.executable,
            "-m",
            "unittest",
            "discover",
            "-s",
            str(Path(source) / "tests"),
            "-p",
            "test_operation_authorization.py",
            "-v",
        ],
        env=environment,
        check=False,
        timeout=30,
    ).returncode


if __name__ == "__main__":
    raise SystemExit(main())
