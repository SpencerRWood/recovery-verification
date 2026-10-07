"""Exercise actual subprocess containment, trust, bounded output and false success."""

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from recovery_verification.cli import main
from recovery_verification.contract import Target, parse_manifest
from recovery_verification.invocation import InvocationError, invoke_readiness
from recovery_verification.models import ConsumerResult

RESULT: dict[str, Any] = {
    "schema_version": "1",
    "mode": "preflight",
    "readiness_state": "unavailable",
    "checks": [{"id": "external", "state": "unavailable", "reason": "not_probed"}],
}


def repository(tmp_path: Path, document: dict[str, Any], body: str = "") -> Target:
    path = tmp_path / "scripts/check"
    path.parent.mkdir()
    path.write_text(
        "#!/usr/bin/env python3\n"
        + (body or f"import json\nprint(json.dumps({RESULT!r}))\nraise SystemExit(4)\n")
    )
    path.chmod(0o755)
    for args in (
        ["init", "-q"],
        ["config", "user.name", "Fixture"],
        ["config", "user.email", "fixture@example.invalid"],
        ["remote", "add", "origin", "https://github.com/example/linux-consumer.git"],
        ["add", "."],
        ["commit", "-qm", "test: disposable trusted consumer"],
    ):
        subprocess.run(["git", *args], cwd=tmp_path, check=True, capture_output=True)  # noqa: S603, S607
    target = document["targets"][0]
    target["validation_commands"][0]["id"] = "validate"
    target["revision"] = subprocess.check_output(
        ["git", "rev-parse", "HEAD"],  # noqa: S607
        cwd=tmp_path,
        text=True,
    ).strip()
    target["validation_commands"][0]["command"] = {
        "entrypoint": "scripts/check",
        "args": [],
        "timeout_seconds": 1,
    }
    return parse_manifest(json.dumps(document)).targets[0]


def test_trusted_check_preserves_unavailable_without_inheriting_credentials(
    tmp_path: Path, document: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SENSITIVE_TOKEN", "do-not-inherit")
    body = "import os, json\nassert 'SENSITIVE_TOKEN' not in os.environ\n"
    body += f"print(json.dumps({RESULT!r}))\nraise SystemExit(4)\n"
    target = repository(tmp_path, document, body)
    result = invoke_readiness(target, tmp_path, "validate", trusted=True)
    assert result.readiness_state == "unavailable"


def test_explicit_trust_and_readiness_scope_required(
    tmp_path: Path, document: dict[str, Any]
) -> None:
    target = repository(tmp_path, document)
    with pytest.raises(InvocationError, match="explicit_trust_required"):
        invoke_readiness(target, tmp_path, "validate")
    with pytest.raises(InvocationError, match="unsupported_readiness_check"):
        invoke_readiness(target, tmp_path, "absent", trusted=True)


@pytest.mark.parametrize("problem", ["revision", "dirty", "owner", "root", "git"])
def test_source_identity_failures(
    tmp_path: Path, document: dict[str, Any], problem: str
) -> None:
    target = repository(tmp_path, document)
    if problem == "revision":
        target = target.model_copy(update={"revision": "2" * 40})
    elif problem == "dirty":
        (tmp_path / "untracked").write_text("do-not-echo")
    elif problem == "owner":
        target = target.model_copy(update={"owning_repository": "other/repository"})
    elif problem == "root":
        root = tmp_path / "nested"
        (root / "scripts").mkdir(parents=True)
        (root / "scripts/check").write_text("#!/bin/sh\nexit 0\n")
        (root / "scripts/check").chmod(0o755)
        tmp_path = root
    else:
        target = target.model_copy(update={"revision": "2" * 40})
        (tmp_path / ".git/HEAD").unlink()
    with pytest.raises(InvocationError):
        invoke_readiness(target, tmp_path, "validate", trusted=True)


@pytest.mark.parametrize("problem", ["missing", "permission", "outside", "symlink"])
def test_entrypoint_containment(
    tmp_path: Path, document: dict[str, Any], problem: str
) -> None:
    target = repository(tmp_path, document)
    executable = tmp_path / "scripts/check"
    if problem == "permission":
        executable.chmod(0o644)
    else:
        executable.unlink()
        if problem == "outside":
            executable.symlink_to("/etc/passwd")
        elif problem == "symlink":
            alternate = tmp_path / "scripts/other"
            alternate.write_text("#!/bin/sh\nexit 0\n")
            executable.symlink_to(alternate)
    with pytest.raises(InvocationError):
        invoke_readiness(target, tmp_path, "validate", trusted=True)


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        ("import time\ntime.sleep(30)\n", "command_timeout"),
        ("print('x' * 70000)\n", "output_limit_exceeded"),
        ("print('do-not-echo-secret')\n", "consumer_invocation_failed"),
        (f"import json\nprint(json.dumps({RESULT!r}))\n", "exit_state_mismatch"),
        (
            "from pathlib import Path\nPath('mutation').write_text('bad')\n",
            "checkout_changed",
        ),
        (
            "import subprocess\n"
            "subprocess.run(['git', 'checkout', '--orphan', 'other'], "
            "capture_output=True)\n",
            "repository_unavailable",
        ),
        (
            'print(\'{\\"schema_version\\":\\"1\\",\\"schema_version\\":\\"2\\"}\')\n',
            "consumer_invocation_failed",
        ),
    ],
)
def test_timeout_output_and_result_fail_closed(
    tmp_path: Path, document: dict[str, Any], body: str, reason: str
) -> None:
    target = repository(tmp_path, document, body)
    with pytest.raises(InvocationError, match=reason) as error:
        invoke_readiness(target, tmp_path, "validate", trusted=True)
    assert "do-not-echo" not in str(error.value)


def test_public_cli_invokes_only_explicit_trusted_target(
    tmp_path: Path, document: dict[str, Any], capsys: pytest.CaptureFixture[str]
) -> None:
    target = repository(tmp_path, document)
    manifest_file = tmp_path / ".git/manifest.json"
    manifest_file.write_text(
        json.dumps({"schema_version": "1", "targets": [target.model_dump(mode="json")]})
    )
    assert (
        main(
            [
                str(manifest_file),
                "--checkout",
                str(tmp_path),
                "--invoke-check",
                "validate",
                "--target",
                target.id,
            ]
        )
        == 4
    )
    result = json.loads(capsys.readouterr().out)
    assert result["tested_revision"] == target.revision
    assert result["result"]["readiness_state"] == "unavailable"
    for args in (
        ["--checkout", str(tmp_path)],
        ["--invoke-check", "validate"],
        ["--checkout", str(tmp_path), "--invoke-check", "validate"],
    ):
        assert main([str(manifest_file), *args]) == 2
        assert json.loads(capsys.readouterr().out)["code"] == "invalid_input"


def test_consumer_states_cannot_disguise_missing_checks() -> None:
    cases: tuple[dict[str, Any], ...] = (
        {"readiness_state": "passed"},
        {"checks": []},
        {"checks": RESULT["checks"] * 2},
        {"password": "do-not-echo"},
    )
    for changes in cases:
        with pytest.raises(ValidationError):
            ConsumerResult.model_validate_json(json.dumps({**RESULT, **changes}))
