"""Opt-in invocation of trusted, revision-bound consumer readiness interfaces."""

import json
import os
import selectors
import signal
import subprocess
from contextlib import suppress
from pathlib import Path
from time import monotonic

from recovery_verification.contract import Target, _reject_duplicate_keys
from recovery_verification.models import ConsumerResult

OUTPUT_LIMIT = 65536


class InvocationError(ValueError):
    """Sanitized diagnostic: never expose consumer output or command exceptions."""


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(  # noqa: S603
        ["git", *args],  # noqa: S607 -- Git comes from the operator's tool PATH.
        cwd=root,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    if result.returncode:
        raise InvocationError("repository_unavailable")
    return result.stdout.strip()


def _stop(process: subprocess.Popen[bytes]) -> None:
    with suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGKILL)
    process.wait(timeout=10)


def _output(process: subprocess.Popen[bytes], timeout: int) -> bytes:
    assert process.stdout is not None  # noqa: S101
    deadline = monotonic() + timeout
    output = bytearray()
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        while True:
            remaining = deadline - monotonic()
            if remaining <= 0 or not selector.select(remaining):
                raise InvocationError("command_timeout")
            chunk = os.read(process.stdout.fileno(), 4096)
            if not chunk:
                break
            output.extend(chunk)
            if len(output) > OUTPUT_LIMIT:
                raise InvocationError("output_limit_exceeded")
    process.wait(timeout=max(0, deadline - monotonic()))
    return bytes(output)


def _command_path(target: Target, root: Path, check_id: str) -> Path:
    checks = [check for check in target.validation_commands if check.id == check_id]
    if len(checks) != 1 or "readiness" not in checks[0].levels:
        raise InvocationError("unsupported_readiness_check")
    command = checks[0].command
    executable = root / command.entrypoint
    if not executable.resolve().is_relative_to(root):
        raise InvocationError("entrypoint_outside_checkout")
    if any(
        part.is_symlink() for part in (executable, *executable.parents) if part != root
    ):
        raise InvocationError("symlink_entrypoint")
    if not executable.is_file() or not os.access(executable, os.X_OK):
        raise InvocationError("entrypoint_unavailable")
    return executable


def invoke_readiness(
    target: Target, checkout: Path, check_id: str, *, trusted: bool = False
) -> ConsumerResult:
    """Run one explicitly trusted readiness check; never bootstrap/recover/drill.

    Trust is an explicit caller grant for this repository/revision. A manifest
    cannot establish it. Consumer authors own the non-destructive semantics.
    """
    if not trusted:
        raise InvocationError("explicit_trust_required")
    root = checkout.resolve()
    executable = _command_path(target, root, check_id)
    command = next(
        check.command for check in target.validation_commands if check.id == check_id
    )
    try:
        if _git(root, "rev-parse", "--show-toplevel") != str(root):
            raise InvocationError("checkout_root_mismatch")
        if _git(root, "rev-parse", "HEAD") != target.revision:
            raise InvocationError("revision_mismatch")
        if _git(root, "status", "--porcelain", "--untracked-files=all"):
            raise InvocationError("dirty_checkout")
        remote = _git(root, "remote", "get-url", "origin")
        if remote not in {
            f"https://github.com/{target.owning_repository}.git",
            f"https://github.com/{target.owning_repository}",
            f"git@github.com:{target.owning_repository}.git",
        }:
            raise InvocationError("repository_mismatch")
        env = {
            "PATH": os.environ.get("PATH", os.defpath),
            "LANG": "C.UTF-8",
            "PYTHONDONTWRITEBYTECODE": "1",
        }
        with subprocess.Popen(  # noqa: S603
            [str(executable), *command.args],
            cwd=root,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        ) as process:
            try:
                output = _output(process, command.timeout_seconds)
            finally:
                _stop(process)
        if _git(root, "rev-parse", "HEAD") != target.revision:
            raise InvocationError("revision_changed")
        if _git(root, "status", "--porcelain", "--untracked-files=all"):
            raise InvocationError("checkout_changed")
        document = json.loads(output, object_pairs_hook=_reject_duplicate_keys)
        result = ConsumerResult.model_validate_json(json.dumps(document))
        expected_exit = {
            "passed": 0,
            "failed": 1,
            "unavailable": 4,
            "skipped": 3,
            "not_applicable": 3,
        }[result.readiness_state]
        if process.returncode != expected_exit:
            raise InvocationError("exit_state_mismatch")
        return result
    except InvocationError:
        raise
    except OSError, ValueError, subprocess.SubprocessError:
        raise InvocationError("consumer_invocation_failed") from None
