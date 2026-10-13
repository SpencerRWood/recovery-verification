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
        RECOVERY_GOVERNANCE_CHECKOUTS=str(Path.cwd()),
    )
    result = subprocess.run(  # noqa: S603 — explicit operator-provided contract checkout.
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
    if result:
        return result
    # Consumers own their approved publication/deployment semantics, including
    # homelab's guarded automatic main delivery. Require their current contracts
    # rather than imposing the provider's manual-only trigger matrix on them.
    for path in paths:
        checkout = Path(path)
        if not (checkout / "tests/test_workflow_governance.py").is_file():
            return 4
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "unittest",
                "discover",
                "-s",
                "tests",
                "-p",
                "test_workflow_governance.py",
            ],
            cwd=checkout,
            env=environment,
            check=False,
            timeout=30,
        ).returncode
        if result:
            return result
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
