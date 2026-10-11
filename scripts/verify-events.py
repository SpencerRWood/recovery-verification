"""Require an explicit local events-service checkout for schema verification."""

import os
import subprocess
import sys
from pathlib import Path


def main() -> int:
    source = os.environ.get("RECOVERY_EVENTS_CHECKOUT")
    if not source or not (Path(source) / "src/events_service/contracts.py").is_file():
        sys.stdout.write(
            '{"status":"unavailable","reason":"events_checkout_required"}\n'
        )
        return 4
    environment = dict(os.environ, RECOVERY_REQUIRE_EVENTS_CONTRACT="1")
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "tests/integration/test_events_contract.py",
            "--no-cov",
        ],
        env=environment,
        check=False,
        timeout=25,
    ).returncode


if __name__ == "__main__":
    raise SystemExit(main())
