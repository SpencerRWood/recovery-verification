"""Bounded local preflight interface; no consumer commands are executed."""

import argparse
import json
import sys
from pathlib import Path

from recovery_verification.contract import parse_manifest
from recovery_verification.preflight import preflight


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--target", action="append", default=[], dest="targets")
    args = parser.parse_args(argv)
    try:
        result = preflight(
            parse_manifest(args.manifest.read_text(encoding="utf-8")),
            tuple(args.targets),
        )
    except OSError, ValueError:
        sys.stdout.write(
            json.dumps({"contract_state": "failed", "code": "invalid_input"})
        )
        sys.stdout.write("\n")
        return 2
    sys.stdout.write(result.model_dump_json() + "\n")
    return 0
