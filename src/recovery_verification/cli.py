"""Bounded local preflight interface; no consumer commands are executed."""

import argparse
import json
import sys
from pathlib import Path

from recovery_verification.contract import parse_manifest
from recovery_verification.invocation import invoke_readiness
from recovery_verification.preflight import preflight


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--target", action="append", default=[], dest="targets")
    parser.add_argument("--checkout", type=Path)
    parser.add_argument(
        "--invoke-check", help="Explicitly trust and invoke one readiness check"
    )
    args = parser.parse_args(argv)
    try:
        manifest = parse_manifest(args.manifest.read_text(encoding="utf-8"))
        result = preflight(manifest, tuple(args.targets))
        if args.checkout is not None or args.invoke_check is not None:
            if (
                args.checkout is None
                or args.invoke_check is None
                or len(args.targets) != 1
            ):
                raise ValueError("explicit checkout/check/target required")
            target = next(
                target for target in manifest.targets if target.id == args.targets[0]
            )
            consumer = invoke_readiness(
                target, args.checkout, args.invoke_check, trusted=True
            )
            sys.stdout.write(
                json.dumps(
                    {
                        "target_id": target.id,
                        "owning_repository": target.owning_repository,
                        "tested_revision": target.revision,
                        "check_id": args.invoke_check,
                        "result": consumer.model_dump(mode="json"),
                    }
                )
                + "\n"
            )
            return {
                "passed": 0,
                "failed": 1,
                "unavailable": 4,
                "skipped": 3,
                "not_applicable": 3,
            }[consumer.readiness_state]
    except OSError, ValueError:
        sys.stdout.write(
            json.dumps({"contract_state": "failed", "code": "invalid_input"})
        )
        sys.stdout.write("\n")
        return 2
    sys.stdout.write(result.model_dump_json() + "\n")
    return 0
